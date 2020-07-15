import argparse
import logging
from urllib.parse import urlparse
import pandas as pd
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(filename):
  logging.info('Starting cleaning process')
  df = _read_data(filename)
  newspaper_uid = _extract_newspaper_uid(filename)
  df = _add_newspaper_uid_column(df, newspaper_uid)
  df = _extract_and_add_host(df)
  df = _fill_missing_titles(df)
  df = _generate_uids_for_rows(df)
  df = _remove_non_desired_characters(df)
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

def _fill_missing_titles(df):
  logger.info('Filling missing titles')
  missing_titles_mask = df['title'].isna()
  missing_titles = (df[missing_titles_mask]['url']
                     .str.extract(r'(?P<missing_titles>[^/]+)$')
                     .applymap(lambda title: title.replace('-',' ')))
  df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
  return df

def _generate_uids_for_rows(df):
  logger.info('Generating uids for each row')
  uids = (df
           .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
           .apply(lambda hash_object: hash_object.hexdigest()))
  df['uid'] = uids
  return df.set_index('uid')

def _remove_non_desired_characters(df):
  logger.info('Removing non desired characters on body')
  stripped_body = (df
                    .apply(lambda row: row['body'], axis=1)
                    .apply(lambda body: body.replace('\n',''))
                    .apply(lambda body: body.replace('\r',''))
                  )
  df['body'] = stripped_body
  return df

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('filename',
                      help='The path to the dirty data',
                      type=str)
  args = parser.parse_args()
  df = main(args.filename)
  print(df)
  #df.to_csv(r'modified_{}'.format(args.filename))
  #print('File saved')