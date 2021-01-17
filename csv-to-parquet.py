import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df_source = 'extended-compressTest.csv'
df_fileprefix = 'extended-compressTest.parquet'
df = pd.read_csv(df_source)



# df to parquet
table = pa.Table.from_pandas(df, preserve_index=True)
pq.write_table(table, df_fileprefix)
print("Parquet written")


# parquet to df (parquet can be queried by column as well, so reading it all into pandas doesn't quite do it justice)
df = pd.read_parquet