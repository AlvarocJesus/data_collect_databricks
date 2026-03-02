# %%
import requests
import pandas as pd
import datetime
import json
from time import sleep

# API - https://www.tabnews.com.br/GabrielSozinho/documentacao-da-api-do-tabnews#gabrielsozinho-content-b
# %%

class Collector:
  def __init__(self, url, instance_name):
    self.url = url
    self.instance_name = instance_name

  def get_content(self, **kwargs):
    # url = 'https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/'

    resp = requests.get(self.url, params=kwargs)
    return resp
  
  def save_parquet(self, data):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    df = pd.DataFrame(data)
    df.to_parquet(f'data/{self.instance_name}/parquet/{now}.parquet', index=False)
  
  def save_json(self, data):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    with open(f'data/{self.instance_name}/json/{now}.json', 'w') as open_file:
      json.dump(data, open_file, indent=2)

  def save_data(self, data, option='json'):
    if option == 'json':
      self.save_json(data)
    elif option == 'dataframe':
      self.save_parquet(data)

  def get_and_save(self, save_format='json', **kwargs):
    resp = self.get_content(**kwargs)

    if resp.status_code == 200:
      data = resp.json()
      self.save_data(data, option=save_format)
    else:
      data = None
      print(f'Request sem sucesso: {resp.status_code}', resp.json())
    
    return data
  
  def auto_exect(self, date_stop='2000-01-01', save_format='json'):
    page = 1

    while True:
      print(f'Coletando pagina {page}')

      data = self.get_and_save(save_format=save_format, page=page, per_page=1000)

      if data == None:
        print('Erro ao coletar dados... aguardando...')
        sleep()
      else:
        date_last = pd.to_datetime(data[-1]['published_at']).date()

        if date_last < pd.to_datetime(date_stop).date():
          print('Data de parada atingida... finalizando coleta')
          break
        elif len(data) < 1000:
          print('Sem mais dados para coletar... finalizando coleta')
          break
        sleep(5)
      
      page += 1
# %%
collect = Collector('https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/', 'episodios')
collect.auto_exect()
# %%
