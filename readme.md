#### Chainstate Parser
Parses Bitcoin's LevelDB UTXO set (chainstate folder) and dumps to a flat sqlite table



![alt screenshot](chainstate_screenshot.png)


#### Setup
- Required: >= python3.7
- `pip install -r requirements.txt`
- Shutdown bitcoind and copy chainstate folder inside this package folder
- Setup config.py settings (refer config section below)
- Run `python chainstate.py`
- The parsed data will be saved down in chainstate.sqlite
- You can load it back in python/dataframes or use [SQLite browser](https://sqlitebrowser.org/) to query

#### Config.py
> **Warning**: processing time: ~110M leveldb rows, parses at ~10k rows/second
- MAX_ROWS (int): set max rows to parse(useful for debugging to run quickly), set None to disable limit
- OUTPUT_FORMAT (str): parquet/sqlite/both, file format of output db file. 
    [for this db parquet is taking less than 50% storage compared to sqlite and is a lot faster to load entirely back into a dataframe]
- BATCH_SIZE (int): num of rows to append in batches to the output db, adjust accordingly if memory is not enough



---
#### References:
- https://github.com/in3rsha/bitcoin-chainstate-parser
- https://github.com/proger/utxo-dump
- https://github.com/jimmysong/programmingbitcoin