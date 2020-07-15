import argparse
import logging
from urllib.parse import urlparse
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(filename):
  logging.info('Starting cleaning process')
  df = _read_data(filename)
  newspaper_uid = _extract_newspaper_uid(filename)
  df = _add_newspaper_uid_column(df, newspaper_uid)
  df = _extract_and_add_host(df)
  return df

def _read_data(filename):
  logger.info('Reading file: {}'.format(filename))
  return pd.read_csv(filename)

def _extract_newspaper_uid(filename):
  logger.info('Extracting newspaper uid')
  newspaper_uid = filename.split('_')[0]
  logger.info('Newspaper uid detected: {}'.format(newspaper_uid))
  return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
  logger.info('Filling newspaper_uid column with: {}'.format(newspaper_uid))
  df['newspapaer_uid'] = newspaper_uid
  return df

def _extract_and_add_host(df):
  logger.info('Extracting host from url and adding to dataframe')
  df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
  return df

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('filename',
                      help='The path to the dirty data',
                      type=str)
  args = parser.parse_args()
  df = main(args.filename)
  df.to_csv(r'modified_{}'.format(args.filename))
  print('File saved')