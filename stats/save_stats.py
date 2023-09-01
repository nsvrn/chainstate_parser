from tqdm import tqdm
from loguru import logger
import pandas as pd
from pathlib import Path

dirpath = Path(__file__).parents[1] / 'parquet_s'
hfile = dirpath / 'stats_by_ht.parquet'
sfile = dirpath / 'stats_by_ht_stype.parquet'

def write_to_db(hdf: pd.DataFrame, sdf: pd.DataFrame):
    hdf.to_parquet(hfile, compression='snappy')
    sdf.to_parquet(sfile, compression='snappy')
    logger.info('Saved to db files.')


def save_stats():
    h_df = pd.DataFrame()
    hs_df = pd.DataFrame()

    for f in tqdm(dirpath.iterdir()):
        if f.name.startswith('upto'): 
            logger.info(f'Calculating stats for {f.name}')
            df = pd.read_parquet(f)

            # stats by height
            hdf = df[['height', 'amount', 'tx_id']] \
                        .groupby(['height']) \
                        .agg({'amount':'sum', 'height':'count', 'tx_id':'nunique'}) \
                        .rename(columns={'height':'num_outputs', 'tx_id':'num_txs'}) \
                        .reset_index()
            h_df = pd.concat([h_df, hdf], ignore_index=True)

            # stats by height and script type:
            sdf = df[['height', 'script_type', 'amount']] \
                        .groupby(['height', 'script_type']) \
                        .agg({'amount':'sum', 'height':'count'}) \
                        .rename(columns={'height':'num_outputs'}) \
                        .reset_index()
            hs_df = pd.concat([hs_df, sdf], ignore_index=True)
    write_to_db(h_df, hs_df)



if __name__ == '__main__':
    save_stats()

