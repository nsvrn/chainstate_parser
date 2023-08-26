import sys, plyvel
from tqdm import tqdm
from loguru import logger
from pathlib import Path
import helper as hp
from dataclasses import dataclass
import script 


@dataclass
class UTXO:
    tx_id: str
    vout: str
    height: int
    is_coinbase: int
    amount: int # sats
    script_type: str
    script_pubkey: str


def set_logger(level='INFO', sink=sys.stderr):
    logger.remove()
    logger.add(sink, level='DEBUG')

def get_db():
    fpath = str(Path(__file__).parents[0].joinpath('chainstate'))
    db = plyvel.DB(fpath, create_if_missing=False)
    return db

def get_obfuscation_key(db):
    obf_key = db.get(bytes.fromhex('0e006f62667573636174655f6b6579'))
    obf_key = obf_key.hex()[2:]
    logger.debug(f'Obfuscation key: {obf_key}')
    return obf_key


def deobfuscate(key, value):
    dv = ''
    for idx, v in enumerate(value):
        c = (int(v, base=16) ^ int(key[idx % len(key)], base=16))
        dv += f'{c:x}'
    return bytes.fromhex(dv)


def parse_chainstate():
    db = get_db()
    obf_key = get_obfuscation_key(db)
    idx = 0
    utxo_set = []
    logger.info('Parsing in progress...')
    for key, value in tqdm(db.iterator()):
        if chr(key[0]) == 'C':
            # parse key:
            tx_id = (key[1:33][::-1]).hex()
            vout = hp.read_varint(key[33:])[0]
            
            # parse value:
            d_value = deobfuscate(obf_key, value.hex())
            code, offset = hp.read_varint(d_value)
            height = code >> 1
            is_coinbase = code & 0x01
            amount, offset = hp.read_varint(d_value, offset)
            amount = hp.txout_decompressamount(amount)
            nsize, offset = hp.read_varint(d_value, offset)
            script_type, script_pubkey = script.decompress(nsize, d_value[offset:])   
            utxo_set.append(UTXO(tx_id, vout, height, is_coinbase, amount, 
                                    script_type, script_pubkey.__repr__()))

        idx += 1
        if idx >= int(10e6):
            break
    if len(utxo_set) > 0:
        logger.info(f'Saving {len(utxo_set)} rows to db...')
        hp.write_to_db(utxo_set)
    db.close()



if __name__ == '__main__':
    set_logger('DEBUG') 
    parse_chainstate()
    pass

