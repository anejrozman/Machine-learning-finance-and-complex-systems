import os
import gymnasium as gym
from gymnasium import spaces
import numpy as np
import pandas as pd
from config.env_config import Config

class UniswapV3LPGymEnv(gym.Env):
    metadata = {'render.modes': ['human']}

    def __init__(self, config: Config = None, feat_num: int = 19):
        super().__init__()
        self.config = config if config is not None else Config()
        self.wealth = self.config.WEALTH
        self.tau = self.config.TAU
        self.FEAT_NUM = feat_num

        self.load_data()
        self.initialize_decision_grid()

        self.action_space = spaces.Box(
            low=np.array([0, 0, 0], dtype=np.float32),
            high=np.array([1, np.finfo(np.float32).max, np.finfo(np.float32).max],
                          dtype=np.float32),
            shape=(3,),
            dtype=np.float32
        )

        self.observation_space = spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=(self.FEAT_NUM,),
            dtype=np.float32
        )

        self.current_time_index = 0
        self.prev_state = None
        self.cumulative_pnl = 0.0

    def load_data(self):

        self.uniswap_lp_data_1 = pd.read_csv("data/uniswap/uniswap_lp_data_1.csv")
        self.uniswap_lp_data_1['timestamp'] = pd.to_datetime(self.uniswap_lp_data_1['timestamp'], unit='s')

        # Load Binance futures and spot data. For these, we'll assume open_time is already in a string format.
        self.binance_futures_data = pd.read_csv(
            "data/binance/hedging_data/data/ETHUSDC_futures_minute_data.csv",
            parse_dates=["open_time"]
        )
        self.binance_spot_data = pd.read_csv(
            "data/binance/hedging_data/data/ETHUSDC_spot_minute_data.csv",
            parse_dates=["open_time"]
        )

    def initialize_decision_grid(self):

        start_dt = self.uniswap_lp_data_1['timestamp'].min()
        end_dt = self.uniswap_lp_data_1['timestamp'].max()
        self.decision_grid = pd.date_range(start=start_dt, end=end_dt, freq='1min')

    def get_state_at_time(self, query_timestamp):

        available_rows = self.uniswap_lp_data_1[self.uniswap_lp_data_1['timestamp'] <= query_timestamp]
        if available_rows.empty:
            raise ValueError(f"No Uniswap data available up to timestamp {query_timestamp}")
        latest_row = available_rows.iloc[-1]
        return latest_row.to_dict()

    def form_observable_features(self, timestamp):

        state = self.get_state_at_time(timestamp)
        # we fill in this list
        features = np.zeros(self.FEAT_NUM, dtype=np.float32)
        
        # placeholder
        sqrtPriceX96 = state.get("sqrtPriceX96", None)
        if sqrtPriceX96 is not None:

            price = (float(sqrtPriceX96) / (2 ** 96)) ** 2
            features[0] = price
        
        return features

    def compute_pnl(self, prev_state, current_state, action):

        sqrtPrice_current = current_state.get("sqrtPriceX96", None)
        if sqrtPrice_current is not None:
            price = (float(sqrtPrice_current) / (2 ** 96)) ** 2
        else:
            price = 0

        token0_balance_diff = float(current_state.get("token0_balance", 0)) - float(prev_state.get("token0_balance", 0))
        token1_balance_diff = float(current_state.get("token1_balance", 0)) - float(prev_state.get("token1_balance", 0))

        pnl = token0_balance_diff * price + token1_balance_diff

        return pnl

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        total_decisions = len(self.decision_grid)
        self.episode_length = max(1, int(0.1 * total_decisions))
        max_start = total_decisions - self.episode_length

        self.current_time_index = np.random.randint(0, max_start + 1)
        self.start_index = self.current_time_index

        current_timestamp = self.decision_grid[self.current_time_index]
        self.prev_state = self.get_state_at_time(current_timestamp)
        self.cumulative_pnl = 0.0

        observable_features = self.form_observable_features(current_timestamp)
        return observable_features, {}

    def step(self, action):
        
        current_timestamp = self.decision_grid[self.current_time_index]
        current_state = self.get_state_at_time(current_timestamp)
        if self.prev_state is None:
            self.prev_state = current_state

        pnl = self.compute_pnl(self.prev_state, current_state, action)
        self.cumulative_pnl += pnl
        self.prev_state = current_state

        self.current_time_index += 1
        done = self.current_time_index >= len(self.decision_grid)
        next_observation = None
        if not done:
            next_timestamp = self.decision_grid[self.current_time_index]
            next_observation = self.form_observable_features(next_timestamp)

        return next_observation, pnl, done, False, {}

    def render(self, mode="human"):

        print(f"Time Index: {self.current_time_index}, Cumulative PnL: {self.cumulative_pnl}")

    def close(self):
        pass
