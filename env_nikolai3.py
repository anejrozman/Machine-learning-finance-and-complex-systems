import glob
from pathlib import Path

import gymnasium as gym
from gymnasium import spaces
import numpy as np
import pandas as pd

from config.env_config import Config

UNISWAP_POOL_EVENTS_PATH = "uniswap_lp_data"
ETH_USDC_PRICE_PATH = "data/binance/price_data/coinUSDC-price-data/ETHUSDC_20250316.csv"
UNISWAP_SAMPLE_PATH = "uniswap_lp_data/uniswap_lp_data_1.csv"
FEE_TABLE_PATH = "data/uniswap/fee_table.parquet"
POOL_FEE_TIER = 0.0005


def ticks_to_sqrtp(tick: int) -> float:
    return 1.0001 ** (tick / 2)


def _group_concat(frames):
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _build_fee_table(swap_paths: list[str]) -> pd.DataFrame:
    dfs = []
    for p in swap_paths:
        df = pd.read_csv(
            p,
            usecols=["timestamp", "amount0", "amount1", "liquidity", "tick"],
            low_memory=False,
        )
        for col in ("amount0", "amount1", "liquidity"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(subset=["amount0", "amount1", "liquidity"], inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        df["fee0"] = df["amount0"].abs() * POOL_FEE_TIER
        df["fee1"] = df["amount1"].abs() * POOL_FEE_TIER
        dfs.append(df[["timestamp", "fee0", "fee1", "liquidity", "tick"]])

    raw = pd.concat(dfs, ignore_index=True)
    fee_grid = (
        raw.set_index("timestamp")
        .resample("1min")
        .agg({"fee0": "sum", "fee1": "sum", "liquidity": "mean", "tick": "last"})
    )
    fee_grid.rename(
        columns={"liquidity": "liquidity_pool", "tick": "tick_close"}, inplace=True
    )
    fee_grid.fillna({"fee0": 0.0, "fee1": 0.0, "liquidity_pool": np.nan}, inplace=True)
    return fee_grid


class UniswapV3LPGymEnv(gym.Env):
    metadata = {"render.modes": ["human"]}

    def __init__(self, config: Config | None = None, feat_num: int | None = None):
        super().__init__()
        self.config = config or Config()
        self.initial_wealth = self.config.WEALTH
        self.FEAT_NUM = feat_num or 6  # features 0–5 defined below
        self.EPISODE_LEN = 1000

        self.cumulative_pnl = 0.0
        self.active = False
        self.L = 0.0
        self.tick_l = 0
        self.tick_u = 0
        self.x_prev = 0.0
        self.y_prev = 0.0

        self._load_data()
        self._build_decision_grid()

        self.action_space = spaces.Box(
            low=np.array([0, -887272, -887272], dtype=np.float32),
            high=np.array([1, 887272, 887272], dtype=np.float32),
            dtype=np.float32,
        )
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(self.FEAT_NUM,), dtype=np.float32
        )

        self.idx = 0
        self.steps_left = 0

    # ------------------------ data loaders ------------------------
    def _load_data(self):
        cols = ["open_time", "open"]
        px = pd.read_csv(ETH_USDC_PRICE_PATH, usecols=cols)
        px["open_time"] = pd.to_datetime(px["open_time"])
        self.eth_px = px.set_index("open_time")

        lp = pd.read_csv(UNISWAP_SAMPLE_PATH, usecols=["timestamp", "tick"])
        lp["timestamp"] = pd.to_datetime(lp["timestamp"], unit="s")
        self.lp_span = lp.set_index("timestamp")

        # store full LP data for feature extraction
        self.uniswap_lp_data_1 = pd.read_csv(UNISWAP_SAMPLE_PATH)
        self.uniswap_lp_data_1["timestamp"] = pd.to_datetime(
            self.uniswap_lp_data_1["timestamp"], unit="s"
        )

        fee_frames: dict[str, list[pd.DataFrame]] = {}
        for path in glob.glob(f"{UNISWAP_POOL_EVENTS_PATH}/uniswap_lp_data_*.csv"):
            df = pd.read_csv(path, usecols=["timestamp", "event_type", "gas_eth"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            for evt, grp in df.groupby("event_type"):
                fee_frames.setdefault(evt, []).append(grp)
        self.gas_fee = {
            evt: _group_concat(lst).sort_values("timestamp").set_index("timestamp")
            for evt, lst in fee_frames.items()
        }

        fee_tbl_path = Path(FEE_TABLE_PATH)
        if fee_tbl_path.exists():
            self.fee_grid = pd.read_parquet(fee_tbl_path)
        else:
            swap_paths = glob.glob(f"{UNISWAP_POOL_EVENTS_PATH}/uniswap_lp_data_*.csv")
            self.fee_grid = _build_fee_table(swap_paths)
            fee_tbl_path.parent.mkdir(parents=True, exist_ok=True)
            self.fee_grid.to_parquet(fee_tbl_path, compression="zstd")

    def _build_decision_grid(self):
        start = self.lp_span.index.min()
        end = self.lp_span.index.max()
        self.decision_grid = pd.date_range(start=start, end=end, freq="1min")

    # ------------------------ price & gas helpers ------------------------
    def _eth_price(self, ts: pd.Timestamp) -> float:
        if ts in self.eth_px.index:
            return float(self.eth_px.loc[ts, "open"])
        pos = self.eth_px.index.searchsorted(ts, side="right") - 1
        if pos < 0:
            raise ValueError(f"ETH price not available before {ts}.")
        return float(self.eth_px.iloc[pos]["open"])

    def _gas_cost(self, evt: str, ts) -> float:
        df = self.gas_fee.get(evt)
        if df is None or ts not in self.eth_px.index:
            return 0.0
        last20 = df.loc[:ts].tail(20)
        if last20.empty:
            return 0.0
        return float(last20["gas_eth"].mean() * self._eth_price(ts))

    # ------------------------ pool fee helpers ------------------------
    def _pool_fees(self, ts: pd.Timestamp):
        if ts not in self.fee_grid.index:
            return 0.0, 0.0, np.nan
        row = self.fee_grid.loc[ts]
        return float(row.fee0), float(row.fee1), float(row.liquidity_pool)

    def _accrue_fees(self, ts: pd.Timestamp):
        if not self.active:
            return 0.0, 0.0
        fee0_pool, fee1_pool, L_pool = self._pool_fees(ts)
        if np.isnan(L_pool) or L_pool == 0.0:
            return 0.0, 0.0
        share = self.L / L_pool
        return share * fee0_pool, share * fee1_pool

    # ------------------------ feature engineering ------------------------
    def form_observable_features(
        self, timestamp: pd.Timestamp, lookback_period: pd.Timedelta = pd.Timedelta(hours=1)
    ) -> np.ndarray:
        beginning = timestamp - lookback_period
        features = np.zeros(self.FEAT_NUM, dtype=np.float32)

        period_df = (
            self.uniswap_lp_data_1
                .loc[(self.uniswap_lp_data_1["timestamp"] >= beginning)
                    & (self.uniswap_lp_data_1["timestamp"] <= timestamp)]
                .copy()                # 👈 guarantees an independent frame
        )


        period_df["sqrtPriceX96"] = pd.to_numeric(
            period_df["sqrtPriceX96"], errors="coerce"
        )
        swap_df = period_df[period_df["event_type"] == "Swap"].copy()
        swap_df.dropna(subset=["sqrtPriceX96"], inplace=True)
        swap_df["price"] = (swap_df["sqrtPriceX96"] / (2 ** 96)) ** 2

        features[0] = swap_df["price"].mean() if not swap_df.empty else 0.0
        features[1] = swap_df["price"].std(ddof=0) if not swap_df.empty else 0.0
        if len(swap_df) > 1:
            features[2] = swap_df["price"].iloc[-1] - swap_df["price"].iloc[0]
        else:
            features[2] = 0.0

        swap_df["amount0"] = pd.to_numeric(swap_df["amount0"], errors="coerce")
        swap_df["amount1"] = pd.to_numeric(swap_df["amount1"], errors="coerce")
        features[3] = swap_df["amount0"].abs().sum() + swap_df["amount1"].abs().sum()

        fee_tier = 0.005

        def _lp_fee(row):
            dx, dy = row["amount0"], row["amount1"]
            if dy > 0:
                return (fee_tier / (1 - fee_tier)) * dy
            if dx > 0:
                return (fee_tier / (1 - fee_tier)) * dx
            return 0.0

        swap_df["lp_fee"] = swap_df.apply(_lp_fee, axis=1)
        features[4] = swap_df["lp_fee"].sum()

        period_df["gas_eth"] = pd.to_numeric(period_df["gas_eth"], errors="coerce")
        features[5] = period_df["gas_eth"].mean() if not period_df.empty else 0.0

        return features

    # wrapper to keep original naming used elsewhere
    def _features(self, ts: pd.Timestamp) -> np.ndarray:
        return self.form_observable_features(ts)

    # ------------------------ Gym API ------------------------
    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)

        max_start = len(self.decision_grid) - self.EPISODE_LEN - 1
        self.idx = self.np_random.integers(0, max_start + 1)
        self.steps_left = self.EPISODE_LEN

        self.active = False
        self.L = 0.0
        self.cumulative_pnl = 0.0

        ts = self.decision_grid[self.idx]
        return self._features(ts), {}

    def step(self, action):
        ts = self.decision_grid[self.idx]
        act, t_l, t_u = map(int, action)

        p = self._eth_price(ts)
        dx_fee, dy_fee = self._accrue_fees(ts)
        self.x_prev += dx_fee
        self.y_prev += dy_fee
        reward = p * dx_fee + dy_fee

        if not self.active and act == 1:
            self.tick_l, self.tick_u = t_l, t_u
            sqrt_pl, sqrt_pu = ticks_to_sqrtp(t_l), ticks_to_sqrtp(t_u)
            sqrt_pc = np.sqrt(p)
            denom = sqrt_pc * (sqrt_pu - sqrt_pc) + sqrt_pl * (sqrt_pc - sqrt_pl)
            self.L = self.initial_wealth / denom
            x0 = self.L * (sqrt_pu - sqrt_pc) / (sqrt_pc * sqrt_pu)
            y0 = self.L * (sqrt_pc - sqrt_pl)
            self.x_prev, self.y_prev = x0, y0
            self.active = True
            reward -= self._gas_cost("Mint", ts)

        elif self.active and act == 0:
            reward -= self._gas_cost("Burn", ts) + self._gas_cost("Collect", ts)
            self.active = False
            self.L = 0.0

        elif self.active:
            sqrt_pl, sqrt_pu = ticks_to_sqrtp(self.tick_l), ticks_to_sqrtp(self.tick_u)
            sqrt_pc = np.sqrt(p)
            if sqrt_pc <= sqrt_pl:
                xt, yt = self.L * (sqrt_pu - sqrt_pl) / (sqrt_pl * sqrt_pu), 0.0
            elif sqrt_pc < sqrt_pu:
                xt = self.L * (sqrt_pu - sqrt_pc) / (sqrt_pc * sqrt_pu)
                yt = self.L * (sqrt_pc - sqrt_pl)
            else:
                xt, yt = 0.0, self.L * (sqrt_pu - sqrt_pl)
            reward += p * (xt - self.x_prev) + (yt - self.y_prev)
            self.x_prev, self.y_prev = xt, yt

        self.cumulative_pnl += reward
        self.idx += 1
        self.steps_left -= 1

        done = self.steps_left == 0 or self.idx >= len(self.decision_grid)
        obs = self._features(self.decision_grid[self.idx]) if not done else None

        return obs, reward, done, False, {}

    def render(self, mode="human"):
        ts = self.decision_grid[self.idx]
        print(f"{ts} | cumPnL = {self.cumulative_pnl:,.2f}")

    def close(self):
        pass
