import os
import gymnasium as gym
from gymnasium import spaces
import numpy as np
import pandas as pd
from config.env_config import Config

class UniswapV3LPGymEnv(gym.Env):
    metadata = {'render.modes': ['human']}

    def __init__(self, config: Config = None):
        super(UniswapV3LPGymEnv, self).__init__()
        self.config = config if config is not None else Config()
        self.wealth = self.config.WEALTH
        self.tau = self.config.TAU
        base_dir = self.config.PATH_FAKE_DATA
        self.dune_data = pd.read_csv(os.path.join(base_dir, "dune_data.csv"))
        self.fees_data = pd.read_csv(os.path.join(base_dir, "fees_data.csv"))
        self.uniswap_v3_params = pd.read_csv(os.path.join(base_dir, "uniswap_v3_params.csv"))
        self.align_datasets()
        self.initialize_time_grid()
        self.action_space = spaces.Box(low=np.array([0, 0, 0], dtype=np.float32),
                                       high=np.array([1, np.finfo(np.float32).max, np.finfo(np.float32).max], dtype=np.float32),
                                       shape=(3,),
                                       dtype=np.float32)
        self.observation_space = spaces.Dict({
            "time_index": spaces.Discrete(len(self.time_grid)),
            "state_info": spaces.Box(low=-np.inf, high=np.inf, shape=(19,), dtype=np.float32)
        })
        self.current_time_index = 0
        self.prev_state = None
        self.cumulative_pnl = 0.0

    def align_datasets(self):
        self.dune_data['timestamp'] = pd.to_datetime(self.dune_data['timestamp'])
        self.fees_data['timestamp'] = pd.to_datetime(self.fees_data['timestamp'])
        self.uniswap_v3_params['timestamp'] = pd.to_datetime(self.uniswap_v3_params['timestamp'])
        dune_sorted = self.dune_data.sort_values("timestamp")
        fees_sorted = self.fees_data.sort_values("timestamp")
        uni_sorted = self.uniswap_v3_params.sort_values("timestamp")
        merged = pd.merge_asof(dune_sorted, fees_sorted, on="timestamp", direction="nearest")
        merged = pd.merge_asof(merged, uni_sorted, on="timestamp", direction="nearest")
        merged.sort_values("timestamp", inplace=True)
        merged.reset_index(drop=True, inplace=True)
        self.merged_data = merged

    def initialize_time_grid(self):
        self.time_grid = list(self.merged_data.index)

    def form_observable_data(self):
        t = self.time_grid[self.current_time_index]
        row = self.merged_data.iloc[t]
        obs = {
            "eth_reserve": row.get("eth_reserve", np.nan),
            "usdc_reserve": row.get("usdc_reserve", np.nan),
            "sqrt_price": row.get("sqrt_price", np.nan),
            "tick": row.get("tick", np.nan),
            "total_liquidity": row.get("total_liquidity", np.nan),
            "trading_volume": row.get("trading_volume", np.nan),
            "eth_fees": row.get("eth_fees", np.nan),
            "gas_fee": row.get("gas_fee", np.nan),
            "current_sqrt_price": row.get("current_sqrt_price", np.nan),
            "lower_sqrt_price": row.get("lower_sqrt_price", np.nan),
            "upper_sqrt_price": row.get("upper_sqrt_price", np.nan),
            "current_tick": row.get("current_tick", np.nan),
            "lower_tick": row.get("lower_tick", np.nan),
            "upper_tick": row.get("upper_tick", np.nan),
            "delta_x": row.get("delta_x", np.nan),
            "delta_y": row.get("delta_y", np.nan),
            "liquidity_provided": row.get("liquidity_provided", np.nan),
            "computed_delta_x": row.get("computed_delta_x", np.nan),
            "computed_delta_y": row.get("computed_delta_y", np.nan)
        }
        state_vector = np.array(list(obs.values()), dtype=np.float32)
        return {"time_index": self.current_time_index, "state_info": state_vector}

    def compute_pnl(self, prev_row, curr_row, action):
        P_prev = (prev_row.get("current_sqrt_price", np.nan))**2 if not np.isnan(prev_row.get("current_sqrt_price", np.nan)) else 0
        P_curr = (curr_row.get("current_sqrt_price", np.nan))**2 if not np.isnan(curr_row.get("current_sqrt_price", np.nan)) else 0
        if action[0] >= 0.5:
            lower_bound = action[1]
            upper_bound = action[2]
            fee_income = 0.0
            if lower_bound <= curr_row.get("current_sqrt_price", np.nan) <= upper_bound:
                total_liquidity = curr_row.get("total_liquidity", 1)
                lp_liquidity = curr_row.get("liquidity_provided", 0)
                trading_volume = curr_row.get("trading_volume", 0)
                fee_income = (lp_liquidity / total_liquidity) * (trading_volume * self.tau)
            c_r = -self.tau * curr_row.get("computed_delta_x", 0) * P_curr
            c_m = -curr_row.get("gas_fee", 0) * self.wealth * P_curr
            holding_profit = curr_row.get("computed_delta_x", 0) * (P_curr - P_prev) + curr_row.get("computed_delta_y", 0) * (P_curr - P_prev)
            pnl = fee_income + c_r + c_m + holding_profit
        else:
            pnl = self.wealth * (P_curr - P_prev)
        return pnl

    def reset(self, seed=None, options=None):
        self.current_time_index = 0
        self.prev_state = self.merged_data.iloc[self.time_grid[self.current_time_index]].to_dict()
        self.cumulative_pnl = 0.0
        return self.form_observable_data(), {}

    def step(self, action):
        current_row = self.merged_data.iloc[self.time_grid[self.current_time_index]].to_dict()
        if self.prev_state is None:
            self.prev_state = current_row
        pnl = self.compute_pnl(self.prev_state, current_row, action)
        self.cumulative_pnl += pnl
        self.prev_state = current_row
        self.current_time_index += 1
        done = self.current_time_index >= len(self.time_grid)
        next_state = self.form_observable_data() if not done else None
        return next_state, pnl, done, False, {}

    def render(self, mode="human"):
        pass

    def close(self):
        pass
