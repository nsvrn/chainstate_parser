import sys, plyvel
from tqdm import tqdm
from loguru import logger
from pathlib import Path
import helper as hp
from dataclasses import dataclass
import script 
import config as cfg


@dataclass
class UTXO:
    if cfg.NORMALIZATION: txid_key: int
    else: tx_id: str
    vout: str
    height: int
    height_bin: str
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


def ht_bin_tag(ht, prior, bin):
    p = ((ht - prior)//bin) + 1
    if (ht - prior) % bin == 0:
        p -= 1
    hb = f'upto_{int(((p*bin)+prior)/1000)}k'
    return hb

def height_bin(ht):
    hb = ''
    if ht <= 250000:
        hb = 'upto_250k'
    elif ht <= 325000:
        hb = 'upto_325k'
    elif ht <= 575000:
        hb = ht_bin_tag(ht, 325000, 25000) 
    elif ht <= 775000:
        hb = ht_bin_tag(ht, 575000, 10000)
    elif ht <= 800000:
        hb = ht_bin_tag(ht, 775000, 3000)
    else:
        hb = ht_bin_tag(ht, 800000, 5000)
    return hb



def dump_chainstate():
    db = get_db()
    obf_key = get_obfuscation_key(db)
    idx = 0
    utxo_set = []
    txid_dict = {}
    txid_set = set({})
    assigned_id = 0
    logger.info('Parsing in progress...')
    for key, value in tqdm(db.iterator()):
        if chr(key[0]) == 'C':
            tx_key = None
            # parse key:
            tx_id = (key[1:33][::-1]).hex()
            if cfg.NORMALIZATION and tx_id not in txid_set:
                assigned_id += 1
                txid_dict[tx_id] = assigned_id
                txid_set.add(tx_id)
                tx_key = assigned_id
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
            if tx_key is None: 
                tx_key = txid_dict[tx_id] if cfg.NORMALIZATION else tx_id
            script_pk = script_pubkey.__repr__().replace('CScript', '')[1:][:-1]

            utxo_set.append(UTXO(tx_key, vout, height, height_bin(height), 
                                 is_coinbase, amount, script_type, 
                                 script_pk))
            
            # batch db append:
            if len(utxo_set) % cfg.BATCH_SIZE == 0:
                logger.info(f'Saving a batch of {len(utxo_set)} rows to db...')
                hp.write_to_db(utxo_set)
                utxo_set = []

        idx += 1
        if cfg.MAX_ROWS and idx >= cfg.MAX_ROWS:
            break
    if len(utxo_set) > 0:
        logger.info(f'Saving {len(utxo_set)} rows to db...')
        hp.write_to_db(utxo_set)
        if cfg.NORMALIZATION:
            hp.write_to_db(txid_dict, is_txids=True)
    db.close()


def purge_old_output_files():
    logger.info('Purging old files...')
    for fname in [cfg.PARQUET_FOLDER, cfg.SQLITE_FOLDER]:
        f = Path(fname)
        if f.is_file(): 
            f.unlink(missing_ok=True)
        elif f.is_dir():
            for dir_f in f.iterdir():
                dir_f.unlink()
            f.rmdir()
    Path(cfg.PARQUET_FOLDER).mkdir()
    Path(cfg.SQLITE_FOLDER).mkdir()


if __name__ == '__main__':
    set_logger('DEBUG') 

    purge_old_output_files()
    dump_chainstate()
    

