from locustdb import Client
import numpy as np
import argparse

# parse argument (single positinal arge for column name)
parser = argparse.ArgumentParser(description='Query locustDB')
parser.add_argument('column_name', type=str, help='column name to query')
args = parser.parse_args()
colname = args.column_name

client = Client("http://localhost:8080")
results = client.query(f'SELECT "{colname}" FROM "avid-wildflower-3446"')

# replace brakets with underscores
scolname = colname.replace('[', '_').replace(']', '_')

with open(scolname + ".txt", "w") as f:
    np.savetxt(f, [(r if r is not None else np.nan) for r in results[colname]])
