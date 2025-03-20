import pandas as pd
from dune_client.client import DuneClient

duneClient = DuneClient("LO6QPy1XqN2RS6O0C0u0wfvKKQ9d1AMs") # Api key
query_result = duneClient.get_latest_result(4869396) # query number from: https://dune.com/queries/4869396
rows = query_result.result.rows

df = pd.DataFrame(rows)

df.to_csv("dune_data.csv", index=False)