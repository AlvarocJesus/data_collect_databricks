# %%
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd

headers = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
  'accept-language': 'en-US,en;q=0.9',
  'cache-control': 'max-age=0',
  'priority': 'u=0, i',
  'referer': 'https://www.residentevildatabase.com/personagens/',
  'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Brave";v="144"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-user': '?1',
  'sec-gpc': '1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
}

def get_content(url):
  resp = requests.get(url, headers=headers)

  return resp
# %%
def get_basic_infos(soup):
  div_page = soup.find('div', class_='td-page-content')
  div_page
  dict = {}

  for i in div_page.find_all('p')[1].find_all('em'):
    key, value, *_ = i.text.strip().split(':')
    dict[key] = value.strip(' ')

  return dict
# %%
def get_aparicoes(soup):
  lis = (soup.find('div', class_='td-page-content').find('h4').find_next().find_all('li'))
  aparicoes = [i.text.strip() for i in lis]

  return aparicoes
# %%
def get_personagem_info(url):
  resp = get_content(url)

  if resp.status_code != 200:
    print('Request not successful!')
    return None

  soup = BeautifulSoup(resp.text, 'html.parser')
  infos = get_basic_infos(soup)
  infos['aparicoes'] = get_aparicoes(soup)
  return infos
# %%
def get_links():
  url = 'https://www.residentevildatabase.com/personagens/'

  resp = requests.get(url)
  soup_personagens = BeautifulSoup(resp.text, 'html.parser')
  links_ancoras = (soup_personagens.find('div', class_='td-page-content').find_all('a'))

  links = [i['href'] for i in links_ancoras]
  return links
# %%
links = get_links()

data = []
for i in tqdm(links):
  d = get_personagem_info(i)
  if d is None:
    d = {}
  d['link'] = i
  d['nome'] = i.split('/')[-2].replace('-', ' ').title()
  data.append(d)
# %%
df = pd.DataFrame(data)
df
# %%
# df.to_csv('residente_evil_personagens.csv', index=False, sep=';')
df.to_parquet('residente_evil_personagens.parquet', index=False)
df.to_pickle('residente_evil_personagens.pkl')