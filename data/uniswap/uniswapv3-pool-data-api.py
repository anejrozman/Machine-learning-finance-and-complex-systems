import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import json
from tqdm import tqdm

class UniswapV3PoolAPI:
    def __init__(self):
        # The Graph API endpoint for Uniswap V3
        self.graph_url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
        
        # Pool addresses (converted to lowercase)
        self.pools = {
            "UNI_ETH": "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
            "UNI_AAVE": "0x59c38b6775ded821f010dbd30ecabdcf84e04756",
            "USDC_ETH_005": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",  # 0.05% fee
            "USDC_ETH_03": "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",   # 0.3% fee
            "USDC_ETH_001": "0xe0554a476a092703abdb3ef35c80e0d76d32939f"   # 0.01% fee
        }
        
    def execute_query(self, query, variables=None):
        """Execute a GraphQL query against the Uniswap V3 subgraph"""
        request = {"query": query}
        if variables:
            request["variables"] = variables
            
        try:
            response = requests.post(self.graph_url, json=request)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Query failed: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response: {e.response.text}")
            return None
    
    def get_pool_info(self, pool_address):
        """Get basic information about a specific pool"""
        query = """
        query getPoolInfo($poolAddress: ID!) {
          pool(id: $poolAddress) {
            id
            token0 {
              id
              symbol
              name
              decimals
            }
            token1 {
              id
              symbol
              name
              decimals
            }
            feeTier
            volumeUSD
            totalValueLockedUSD
            createdAtTimestamp
          }
        }
        """
        
        variables = {"poolAddress": pool_address}
        result = self.execute_query(query, variables)
        
        if result and "data" in result and "pool" in result["data"]:
            return result["data"]["pool"]
        return None
    
    def get_pool_hourly_data(self, pool_address, start_timestamp, end_timestamp, skip=0):
        """Get hourly data for a specific pool within a time range"""
        query = """
        query getPoolHourData($poolAddress: String!, $startTime: Int!, $endTime: Int!, $skip: Int!) {
          poolHourDatas(
            where: {pool: $poolAddress, periodStartUnix_gte: $startTime, periodStartUnix_lte: $endTime}
            orderBy: periodStartUnix
            skip: $skip
            first: 1000
          ) {
            periodStartUnix
            pool {
              id
            }
            token0Price
            token1Price
            volumeToken0
            volumeToken1
            volumeUSD
            tvlUSD
            feesUSD
            txCount
            open
            high
            low
            close
          }
        }
        """
        
        variables = {
            "poolAddress": pool_address,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "skip": skip
        }
        
        result = self.execute_query(query, variables)
        
        if result and "data" in result and "poolHourDatas" in result["data"]:
            return result["data"]["poolHourDatas"]
        return []
    
    def get_pool_daily_data(self, pool_address, start_timestamp, end_timestamp, skip=0):
        """Get daily data for a specific pool within a time range"""
        query = """
        query getPoolDayData($poolAddress: String!, $startTime: Int!, $endTime: Int!, $skip: Int!) {
          poolDayDatas(
            where: {pool: $poolAddress, date_gte: $startTime, date_lte: $endTime}
            orderBy: date
            skip: $skip
            first: 1000
          ) {
            date
            pool {
              id
            }
            volumeToken0
            volumeToken1
            volumeUSD
            feesUSD
            tvlUSD
            token0Price
            token1Price
            txCount
            open
            high
            low
            close
          }
        }
        """
        
        variables = {
            "poolAddress": pool_address,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "skip": skip
        }
        
        result = self.execute_query(query, variables)
        
        if result and "data" in result and "poolDayDatas" in result["data"]:
            return result["data"]["poolDayDatas"]
        return []
    
    def get_swap_events(self, pool_address, start_timestamp, end_timestamp, skip=0):
        """Get swap events for a specific pool within a time range"""
        query = """
        query getSwaps($poolAddress: String!, $startTime: Int!, $endTime: Int!, $skip: Int!) {
          swaps(
            where: {pool: $poolAddress, timestamp_gte: $startTime, timestamp_lte: $endTime}
            orderBy: timestamp
            skip: $skip
            first: 1000
          ) {
            timestamp
            pool {
              id
            }
            sender
            recipient
            origin
            amount0
            amount1
            amountUSD
            sqrtPriceX96
            tick
            transaction {
              id
              blockNumber
              gasUsed
              gasPrice
            }
          }
        }
        """
        
        variables = {
            "poolAddress": pool_address,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "skip": skip
        }
        
        result = self.execute_query(query, variables)
        
        if result and "data" in result and "swaps" in result["data"]:
            return result["data"]["swaps"]
        return []
    
    def collect_all_data(self, pool_address, start_timestamp, end_timestamp):
        """Collect all data for a specific pool within a time range"""
        # First, get pool info
        pool_info = self.get_pool_info(pool_address)
        
        # Prepare to collect data
        all_hourly_data = []
        all_daily_data = []
        all_swaps = []
        
        # Collect hourly data with pagination
        skip = 0
        while True:
            hourly_data = self.get_pool_hourly_data(pool_address, start_timestamp, end_timestamp, skip)
            if not hourly_data:
                break
                
            all_hourly_data.extend(hourly_data)
            
            if len(hourly_data) < 1000:
                break
                
            skip += 1000
            time.sleep(0.5)  # To avoid rate limiting
        
        # Collect daily data with pagination
        skip = 0
        while True:
            daily_data = self.get_pool_daily_data(pool_address, start_timestamp, end_timestamp, skip)
            if not daily_data:
                break
                
            all_daily_data.extend(daily_data)
            
            if len(daily_data) < 1000:
                break
                
            skip += 1000
            time.sleep(0.5)
        
        # For swap events, we need to paginate and also handle the date range in smaller chunks
        # This is necessary because there can be many swaps in a short time period
        # Dividing the time range into months
        current_start = start_timestamp
        month_seconds = 30 * 24 * 60 * 60  # Approximately 1 month in seconds
        
        with tqdm(total=(end_timestamp - start_timestamp) // month_seconds, desc=f"Collecting swaps for {pool_address}") as pbar:
            while current_start < end_timestamp:
                current_end = min(current_start + month_seconds, end_timestamp)
                
                skip = 0
                while True:
                    swaps = self.get_swap_events(pool_address, current_start, current_end, skip)
                    if not swaps:
                        break
                        
                    all_swaps.extend(swaps)
                    
                    if len(swaps) < 1000:
                        break
                        
                    skip += 1000
                    time.sleep(0.5)
                
                current_start = current_end
                pbar.update(1)
        
        # Generate minute-level OHLCV data from swap events
        minute_data = self.generate_minute_ohlcv(all_swaps, pool_info)
        
        return {
            "pool_info": pool_info,
            "hourly_data": pd.DataFrame(all_hourly_data) if all_hourly_data else None,
            "daily_data": pd.DataFrame(all_daily_data) if all_daily_data else None,
            "minute_data": minute_data,
            "swaps": pd.DataFrame(all_swaps) if all_swaps else None
        }
    
    def generate_minute_ohlcv(self, swaps, pool_info):
        """Generate minute-level OHLCV data from swap events"""
        if not swaps:
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(swaps)
        
        # Check if DataFrame is empty or required columns are missing
        if df.empty or 'timestamp' not in df.columns or 'amountUSD' not in df.columns:
            return None
        
        # Process timestamps
        df['timestamp'] = df['timestamp'].astype(int)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        
        # Process price information
        # Extract token price from swaps
        if 'sqrtPriceX96' in df.columns and not df['sqrtPriceX96'].isnull().all():
            # Convert sqrtPriceX96 to actual price
            # Price = (sqrtPriceX96 / 2^96)^2
            df['sqrtPriceX96'] = pd.to_numeric(df['sqrtPriceX96'], errors='coerce')
            df['price'] = (df['sqrtPriceX96'] / (2**96))**2
            
            # Adjust for token decimals
            if pool_info and 'token0' in pool_info and 'token1' in pool_info:
                token0_decimals = int(pool_info['token0']['decimals'])
                token1_decimals = int(pool_info['token1']['decimals'])
                decimal_adjustment = 10**(token1_decimals - token0_decimals)
                df['price'] = df['price'] * decimal_adjustment
        else:
            # If sqrtPriceX96 is not available, try to use amounts
            if 'amount0' in df.columns and 'amount1' in df.columns:
                df['amount0'] = pd.to_numeric(df['amount0'], errors='coerce')
                df['amount1'] = pd.to_numeric(df['amount1'], errors='coerce')
                # Avoid division by zero
                df.loc[df['amount0'] != 0, 'price'] = abs(df.loc[df['amount0'] != 0, 'amount1']) / abs(df.loc[df['amount0'] != 0, 'amount0'])
            else:
                # If no price information is available
                return None
        
        # Clean data
        df['amountUSD'] = pd.to_numeric(df['amountUSD'], errors='coerce')
        
        # Resample to minute level
        df = df.set_index('datetime')
        
        # Create OHLCV
        ohlcv = pd.DataFrame()
        ohlcv['open'] = df['price'].resample('1min').first()
        ohlcv['high'] = df['price'].resample('1min').max()
        ohlcv['low'] = df['price'].resample('1min').min()
        ohlcv['close'] = df['price'].resample('1min').last()
        ohlcv['volume'] = df['amountUSD'].resample('1min').sum()
        ohlcv['count'] = df['amountUSD'].resample('1min').count()
        
        # Forward fill missing values
        ohlcv = ohlcv.fillna(method='ffill')
        
        # Reset index to have datetime as a column
        ohlcv = ohlcv.reset_index()
        
        return ohlcv
    
    def fetch_all_pool_data(self, look_back_days=365):
        """Fetch data for all pools for the past year"""
        # Calculate start and end times
        end_time = int(time.time())
        start_time = end_time - (look_back_days * 24 * 60 * 60)
        
        results = {}
        
        # Loop through all pools
        for pool_name, pool_address in self.pools.items():
            print(f"\nFetching data for {pool_name} pool: {pool_address}")
            
            pool_data = self.collect_all_data(pool_address, start_time, end_time)
            results[pool_name] = pool_data
            
            # Sleep to be courteous to the API
            time.sleep(2)
            
        return results
    
    def save_to_csv(self, data_dict, folder="pool-data"):
        """Save each dataframe to a CSV file"""
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Created directory: {folder}")
            
        timestamp = datetime.now().strftime("%Y%m%d")
        
        for pool_name, pool_data in data_dict.items():
            # Save pool info as JSON
            if pool_data["pool_info"]:
                info_filename = f"{folder}/{pool_name}_info_{timestamp}.json"
                with open(info_filename, 'w') as f:
                    json.dump(pool_data["pool_info"], f, indent=2)
                print(f"Saved {pool_name} info to {info_filename}")
            
            # Save hourly data
            if pool_data["hourly_data"] is not None and not pool_data["hourly_data"].empty:
                hourly_filename = f"{folder}/{pool_name}_hourly_{timestamp}.csv"
                pool_data["hourly_data"].to_csv(hourly_filename, index=False)
                print(f"Saved {pool_name} hourly data to {hourly_filename} ({len(pool_data['hourly_data'])} records)")
            
            # Save daily data
            if pool_data["daily_data"] is not None and not pool_data["daily_data"].empty:
                daily_filename = f"{folder}/{pool_name}_daily_{timestamp}.csv"
                pool_data["daily_data"].to_csv(daily_filename, index=False)
                print(f"Saved {pool_name} daily data to {daily_filename} ({len(pool_data['daily_data'])} records)")
            
            # Save minute data (our primary target)
            if pool_data["minute_data"] is not None and not pool_data["minute_data"].empty:
                minute_filename = f"{folder}/{pool_name}_minute_{timestamp}.csv"
                pool_data["minute_data"].to_csv(minute_filename, index=False)
                print(f"Saved {pool_name} minute data to {minute_filename} ({len(pool_data['minute_data'])} records)")
            
            # Save raw swaps (optional, can be very large)
            if pool_data["swaps"] is not None and not pool_data["swaps"].empty:
                swaps_filename = f"{folder}/{pool_name}_swaps_{timestamp}.csv"
                # Flatten the nested JSON structure
                swaps_df = pool_data["swaps"].copy()
                if 'pool' in swaps_df.columns:
                    swaps_df['pool_id'] = swaps_df['pool'].apply(lambda x: x['id'] if isinstance(x, dict) and 'id' in x else None)
                    swaps_df = swaps_df.drop('pool', axis=1)
                if 'transaction' in swaps_df.columns:
                    for col in ['id', 'blockNumber', 'gasUsed', 'gasPrice']:
                        swaps_df[f'tx_{col}'] = swaps_df['transaction'].apply(
                            lambda x: x[col] if isinstance(x, dict) and col in x else None
                        )
                    swaps_df = swaps_df.drop('transaction', axis=1)
                swaps_df.to_csv(swaps_filename, index=False)
                print(f"Saved {pool_name} swaps to {swaps_filename} ({len(swaps_df)} records)")

# Example usage
if __name__ == "__main__":
    # Initialize the API
    api = UniswapV3PoolAPI()
    
    # Fetch data for all pools for the past year
    print("Fetching data for the past year for all specified pools...")
    pool_data = api.fetch_all_pool_data(look_back_days=365)
    
    # Save the data to CSV files
    print("\nSaving data to CSV files...")
    api.save_to_csv(pool_data, folder="pool-data")
    
    print("\nProcess completed!")