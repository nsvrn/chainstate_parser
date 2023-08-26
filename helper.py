from pandas import DataFrame
import sqlite3
import config as cfg
from fastparquet import write
from pathlib import Path


def write_to_db(obj_list, output):
    '''
        output: parquet/sqlite/both
    '''
    nop = 'no_partition'
    df = DataFrame([o.__dict__ for o in obj_list])
    if cfg.PARTITION: 
        partition_by = 'script_type'
    else:
        df[nop] = '0'
        partition_by = nop
    for partition, gdf in df.groupby(partition_by):
        if output.lower() in ['sqlite', 'both']:
            f = Path(__file__).parents[0].joinpath(cfg.SQLITE_FOLDER).joinpath(f'{partition}.sqlite')
            conn = sqlite3.connect(f)
            if not cfg.PARTITION: del gdf[nop]
            gdf.to_sql(f'{partition}', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
        if output.lower() in ['parquet', 'both']:
            f = Path(__file__).parents[0].joinpath(cfg.PARQUET_FOLDER).joinpath(f'{partition}.parquet')
            if not cfg.PARTITION: del gdf[nop]
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