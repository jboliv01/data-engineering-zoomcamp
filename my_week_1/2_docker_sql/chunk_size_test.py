import pandas as pd

df = pd.read_csv(
    'taxi+_zone_lookup.csv', chunksize=100
)

chunk_size = 100
chunk_count = 0

for df in pd.read_csv('taxi+_zone_lookup.csv', chunksize=chunk_size):
    chunk_count += df.shape[0]

print(chunk_count)