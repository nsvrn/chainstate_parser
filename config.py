SQLITE_FOLDER = 'sqlite'
PARQUET_FOLDER = 'parquet'

OUTPUT_FORMAT = 'parquet'       # parquet/sqlite/both
MAX_ROWS = None                 # None or int value like int(1e6)
BATCH_SIZE = int(5e6)           # appends to db file in batches of these many rows
PARTITION = True                 # if True then output db files are split by script_type