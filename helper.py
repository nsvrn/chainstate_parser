from pandas import DataFrame
import sqlite3


def write_to_db(obj_list):
    df = DataFrame([o.__dict__ for o in obj_list])
    conn = sqlite3.connect('chainstate.sqlite')
    df.to_sql('chainstate', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()

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