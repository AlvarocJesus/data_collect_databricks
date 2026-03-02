# %%
import requests
import pandas as pd
import datetime
import json
from time import sleep

# API - https://www.tabnews.com.br/GabrielSozinho/documentacao-da-api-do-tabnews#gabrielsozinho-content-b
# %%
def get_response(**kwargs):
  url = 'https://www.tabnews.com.br/api/v1/contents' #?page=1&per_page=100&strategy=new

  resp = requests.get(url, params=kwargs)
  return resp

def save_data(data, option='json'):
  now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

  if option == 'json':
    with open(f'data/contents/json/{now}.json', 'w') as open_file:
      json.dump(data, open_file, indent=2)
  elif option == 'dataframe':
    df = pd.DataFrame(data)
    df.to_parquet(f'data/contents/parquet/{now}.parquet', index=False)
# %%
page = 1
date_stop = pd.to_datetime('2026-02-01').date()

while True:
  print(f'Getting page {page}...')

  resp = get_response(page=page, per_page=100, strategy='new')

  if resp.status_code == 200:
    print('Request successful!')
    data = resp.json()
    save_data(data)

    date = pd.to_datetime(data[-1]['updated_at']).date()
    
    if len(data) < 100 or date < date_stop:
      print('No more data to fetch. Stopping...')
      break
    
    page += 1
    sleep(2)
  else:
    print(resp.status_code)
    print(resp.json())
    sleep(60 * 5)
    print('Request failed. Retrying...')

# %%
