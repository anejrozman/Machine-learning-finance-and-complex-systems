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
        self.last_timestamp = None
        self.cumulative_pnl = 0.0

    def load_data(self):
        self.uniswap_lp_data = pd.read_csv("data/uniswap/uniswap_lp_data_1.csv")
        self.uniswap_lp_data['timestamp'] = pd.to_datetime(self.uniswap_lp_data['timestamp'], unit='s')

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
        start_dt = self.uniswap_lp_data['timestamp'].min()
        end_dt = self.uniswap_lp_data['timestamp'].max()
        self.decision_grid = pd.date_range(start=start_dt, end=end_dt, freq='1min')

    def form_observable_data(self, timestamp):
        binance_futures = self.binance_futures_data[self.binance_futures_data['open_time'] <= timestamp]
        if binance_futures.empty:
            raise ValueError(f"No binance_futures data available up to timestamp {timestamp}")
        binance_spot = self.binance_spot_data[self.binance_spot_data['open_time'] <= timestamp]
        if binance_spot.empty:
            raise ValueError(f"No binance_spot data available up to timestamp {timestamp}")
        uniswap_lp = self.uniswap_lp_data[self.uniswap_lp_data['timestamp'] <= timestamp]
        if uniswap_lp.empty:
            raise ValueError(f"No uniswap_lp_data available up to timestamp {timestamp}")

        return (binance_futures, binance_spot, uniswap_lp)

    def form_observable_features(self, timestamp):
        features = np.zeros(self.FEAT_NUM, dtype=np.float32)

        (binance_futures, binance_spot, uniswap_lp) = self.form_observable_data(timestamp)
        
        ## now code which takes existing data and fills in features data
        ## e.g.
        features[0] = np.log(binance_futures.close.iloc[-1] / binance_futures.close.iloc[-1])
        features[1] = 3
        return features

    def compute_pnl(self, current_timestamp, action):
        """
        Placeholder for PnL computation.
        """

        # previous_data = self.form_observable_data(self.last_timestamp)
        # current_data = self.form_observable_data(current_timestamp)
        # pnl = ... compute pnl based on the difference between current_data and previous_data ...
        return np.random.normal(0, 1)

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        total_decisions = len(self.decision_grid)
        self.episode_length = max(1, int(0.1 * total_decisions))
        max_start = total_decisions - self.episode_length

        self.current_time_index = np.random.randint(0, max_start + 1)

        current_timestamp = self.decision_grid[self.current_time_index]
        self.last_timestamp = current_timestamp
        self.cumulative_pnl = 0.0

        observable_features = self.form_observable_features(current_timestamp)
        return observable_features, {}

    def step(self, action):

        prev_timestamp = self.last_timestamp
        current_timestamp = self.decision_grid[self.current_time_index]
        
        pnl = self.compute_pnl(current_timestamp, action)
        self.cumulative_pnl += pnl
        
        self.last_timestamp = current_timestamp

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
