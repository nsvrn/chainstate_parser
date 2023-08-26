SQLITE_FNAME = 'chainstate.sqlite'
PARQUET_FNAME = 'chainstate.parquet'

OUTPUT_FORMAT = 'parquet'       # parquet/sqlite/both
MAX_ROWS = None                 # None or int value like int(1e6)
BATCH_SIZE = int(5e6)           # appends to db file in batches of these many rows