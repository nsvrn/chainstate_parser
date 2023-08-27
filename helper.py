from pandas import DataFrame
import sqlite3
import config as cfg
from fastparquet import write
from pathlib import Path


def write_to_db(obj_list, is_txids=False):
    is_partition = cfg.PARTITION
    db_name = 'chainstate'
    if is_txids: 
        is_partition = False
        db_name = 'txid'
        df = DataFrame(obj_list.items(), columns=['tx_id', 'txid_key'])
    else:
        df = DataFrame([o.__dict__ for o in obj_list])
    
    if is_partition: 
        partition_by = 'height_batch'
        df[partition_by] = (df['height']//25000 + 1).astype(str)
    else:
        df['__x'] = 1
        partition_by = '__x'
    
    for partition, gdf in df.groupby(partition_by):
        fname = f'height_p{partition.zfill(2)}' if is_partition else db_name
        if cfg.OUTPUT_FORMAT.lower() in ['sqlite', 'both']:
            f = Path(__file__).parents[0].joinpath(cfg.SQLITE_FOLDER).joinpath(f'{fname}.sqlite')
            conn = sqlite3.connect(f)
            if partition_by in gdf.columns: del gdf[partition_by]
            gdf.to_sql(f'{fname}', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
        if cfg.OUTPUT_FORMAT.lower() in ['parquet', 'both']:
            f = Path(__file__).parents[0].joinpath(cfg.PARQUET_FOLDER).joinpath(f'{fname}.parquet')
            if partition_by in gdf.columns: del gdf[partition_by]
            write(f, gdf, compression='snappy', append=f.is_file())
    

def read_varint(buf, offset=0):
    '''
        Decode MSB base-128 VarInt
        Ref: https://github.com/bitcoin/bitcoin/blob/v0.13.2/src/serialize.h#L306-L328
        
        MSB b128 Varints have set the bit 128 for every byte but the last one,
            indicating that there is an additional byte following the one being read.
    '''
    n = 0
    for i, byte in enumerate(buf[offset:]):
        n = n << 7 | byte & 0x7F
        if byte & 0x80:
            n += 1
        else:
            return n, offset + i + 1
        

def txout_decompressamount(x):
    '''
        Decompress the Satoshi amount
        Ref: https://github.com/bitcoin/bitcoin/blob/v0.13.2/src/compressor.cpp#L161#L185
    '''
    if x == 0:
        return 0
    x -= 1
    e = x % 10
    x //= 10
    if e < 9:
        d = (x % 9) + 1
        x //= 9
        n = x * 10 + d
    else:
        n = x + 1
    while e > 0:
        n *= 10
        e -= 1
    return n