import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Etherscan API key (Send me a message or make your own on etherscan, Anej Rozman)
API_KEY = "private"

end_date = datetime.today()
start_date = end_date - timedelta(days=365)

start_timestamp = int(start_date.timestamp())
end_timestamp = int(end_date.timestamp())

# API URL
url = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={API_KEY}"

folder = "gas-fees"
os.makedirs(folder, exist_ok=True)

data = []

date = start_date
while date <= end_date:
    timestamp = int(date.timestamp())
    response = requests.get(url)
    
    if response.status_code == 200:
        result = response.json().get("result", {})
        safe_gas = result.get("SafeGasPrice", None)
        propose_gas = result.get("ProposeGasPrice", None)
        fast_gas = result.get("FastGasPrice", None)
        
        data.append([date.strftime("%Y-%m-%d"), safe_gas, propose_gas, fast_gas])
    else:
        print(f"Failed to fetch data for {date.strftime('%Y-%m-%d')}")
    
    date += timedelta(days=1)

df = pd.DataFrame(data, columns=["Date", "SafeGasPrice", "ProposeGasPrice", "FastGasPrice"])

filename = f"eth-gas-prices-{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.csv"
filepath = os.path.join(folder, filename)


df.to_csv(filepath, index=False)
print(f"Data saved to {filepath}")
