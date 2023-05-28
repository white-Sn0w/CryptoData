import requests
from datetime import datetime, date

import datetime
import pandas as pd
import numpy as np

from typing import List
from typing import Dict
from datetime import datetime
from tqdm import tqdm

import time


class MarketData(): 

  def __init__(self, mapping, market_data):
    self.mapping = mapping
    self.market_data = market_data

    self.FIAT = "USD"

  def update_market_data(self):
    print("****************** Updating Existing Market Data ******************")

    active_instruments = self.get_instruments_from_mapping(update=True)
    
    active_instruments_enriched = self.get_internal_id_data(active_instruments)
    
    updated_market_data = self.request_market_data_from_cmc(active_instruments_enriched)

    if not updated_market_data.empty: 
      self.market_data = pd.concat([self.market_data, updated_market_data])
      self.market_data.to_parquet("usd_market_data.parquet", index=False)  
      shutil.copy('usd_market_data.parquet', 'drive/My Drive/[6] CryptoData/Data')

  def fetch_market_data_for_new_instruments(self):
    print("****************** Fetching New Data ******************")
    new_instruments = self.get_instruments_from_mapping(update=True) 
    
    new_instruments_enriched = self.get_internal_id_data(new_instruments)

    updated_market_data = self.request_market_data_from_cmc(new_instruments_enriched)

    if not updated_market_data.empty: 
      self.market_data = pd.concat([self.market_data, updated_market_data])

      self.market_data.to_parquet("usd_market_data.parquet", index=False)  
      shutil.copy('usd_market_data.parquet', 'drive/My Drive/[6] CryptoData/Data')

    self.mapping.to_parquet("mapping.parquet",index=False)
    shutil.copy('mapping.parquet', 'drive/My Drive/[6] CryptoData/Data')

  def request_market_data_from_cmc(self, internal_ids_info: Dict[str, Dict[str, str]]) -> pd.DataFrame:
      rows = []
      
      for internal_id, info in tqdm(internal_ids_info.items(), desc='Requesting pricing data with CMC'):
          slug = info['slug']
          
          if info['most_recent_date'] is None:
              start_date = info['first_record_date']
              end_date = info['last_record_date']
          
              if start_date is None: 
                start_date = "2013-04-28"
                
              if end_date is None: 
                end_date = datetime.now().strftime('%Y-%m-%d')

          else:
              start_date = pd.to_datetime(info['most_recent_date'])
              end_date = datetime.now().strftime('%Y-%m-%d')

              # Check if end_date is at least two days after start_date
              if (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days < 2:
                continue

          time.sleep(0.5)

          URL = f"https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical?convert={self.FIAT}&slug={slug}&time_end={end_date}&time_start={start_date}"
          response = requests.get(URL).json()
        
          if response["status"]["error_code"] == 400 and response["status"]["error_message"] == f'Invalid value for "slug": "{slug}"':
              self.mapping.loc[self.mapping['internal_id'] == internal_id, 'cmc_market_data_available'] = False
              continue

          if response["status"]["error_code"] == 0:
              if "data" in response: 
                  data = response["data"]
                  quotes = data["quotes"]

                  if not quotes:  # Check if quotes list is empty
                      self.mapping.loc[self.mapping['internal_id'] == internal_id, 'cmc_market_data_available'] = False
                      continue

                  for quote in quotes:
                      row = {
                          "internal_id": internal_id,
                          "date": pd.to_datetime(quote["time_close"]).date().strftime("%Y-%m-%d"),
                          "open": quote["quote"][self.FIAT]["open"],
                          "high": quote["quote"][self.FIAT]["high"],
                          "low": quote["quote"][self.FIAT]["low"],
                          "close": quote["quote"][self.FIAT]["close"],
                          "volume": quote["quote"][self.FIAT]["volume"],
                          "market_cap": quote["quote"][self.FIAT]["market_cap"]
                      }
                      rows.append(row)

      df = pd.DataFrame(rows)
      return df

  def get_instruments_from_mapping(self, update=None, new=None): 
    
    if update: 
      active_instruments = self.mapping[self.mapping['is_active'] == True]
      active_internal_id_list = active_instruments['internal_id'].tolist()

      # returns list of internal_ids
      return active_internal_id_list

    if new: 
      internal_ids_in_market_data = self.market_data['internal_id'].unique()
      mapping_with_data_available = self.mapping[self.mapping['cmc_market_data_available'] == True]
      new_internal_id_list = mapping_with_data_available[~mapping_with_data_available['internal_id'].isin(internal_ids_in_market_data)]['internal_id'].tolist()

      return new_internal_id_list 

  def get_internal_id_data(self, internal_ids: List[int]) -> dict:
      # Preprocess market_data and mapping to create dictionaries
      market_data_dict = self.market_data.groupby('internal_id')['date'].max().to_dict()
      mapping_dict = self.mapping.set_index('internal_id').to_dict('index')

      information = {}
      for internal_id in tqdm(internal_ids, desc='Fetching internal_id data'):
          # Get the most recent record date from market_data_dict
          recent_date = market_data_dict.get(internal_id)
          recent_date = pd.to_datetime(recent_date).strftime('%Y-%m-%d') if recent_date else None

          # Get the first and last record date and the slug from mapping_dict
          mapping_info = mapping_dict.get(internal_id, {})
          first_date = mapping_info.get('first_historical_data')
          first_date = pd.to_datetime(first_date).strftime('%Y-%m-%d') if pd.notnull(first_date) else None
          last_date = mapping_info.get('last_historical_data')
          last_date = pd.to_datetime(last_date).strftime('%Y-%m-%d') if pd.notnull(last_date) else None
          slug = mapping_info.get('cmc_slug')

          # Store the information in the dictionary
          information[internal_id] = {
              'most_recent_date': recent_date,
              'first_record_date': first_date,
              'last_record_date': last_date,
              'slug': slug
          }

      return information

  def run(self): 
    self.update_market_data()
    print()
    self.fetch_market_data_for_new_instruments()



