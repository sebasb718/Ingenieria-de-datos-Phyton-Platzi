import logging
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

news_sites_uids = ['eltiempo','eluniversal','elpais']

def main():
  _extract()
  _transform()
  _load()

def _extract():
  logger.info('Starting extract process')
  for news_site_uid in news_sites_uids:
    subprocess.run(['python', 'main.py', news_site_uid], cwd='./extractingData')
    subprocess.run(['find', '.', '-name', '{}*'.format(news_site_uid),
                    '-exec', 'mv', '{}','../transformingData/{}_.csv'.format(news_site_uid),';'],
                    cwd='./extractingData')

def _transform():
  logger.info('Starting transform process')
  for news_site_uid in news_sites_uids:
    dirty_data_filename = '{}_.csv'.format(news_site_uid)
    clean_data_filename = 'clean_{}'.format(dirty_data_filename)
    subprocess.run(['python', 'main.py', dirty_data_filename], cwd='./transformingData')
    subprocess.run(['rm', dirty_data_filename], cwd='./transformingData')
    subprocess.run(['mv', clean_data_filename, '../importingToSqLite/{}.csv'.format(news_site_uid)], 
                   cwd='./transformingData')

def _load():
  logger.info('Starting load to SQLite process')
  for news_site_uid in news_sites_uids:
    clean_data_filename = '{}.csv'.format(news_site_uid)
    subprocess.run(['python', 'main.py', clean_data_filename], cwd='./importingToSqLite')
    subprocess.run(['rm', clean_data_filename], cwd='./importingToSqLite')

if __name__ == "__main__":
  main()