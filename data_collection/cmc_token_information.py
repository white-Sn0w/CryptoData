import requests
from datetime import datetime

import datetime
import pandas as pd
import shutil
import uuid

"""#Class"""
class TokenInformation(): 

  def __init__(self, mapping): 
    self.mapping = mapping

  def request_cmc_assets(self):
    response = requests.get('https://s3.coinmarketcap.com/generated/core/crypto/cryptos.json')
    data = response.json()
    response_df = pd.DataFrame(data['values'], columns=data['fields'])
    response_df['is_active'] = response_df['is_active'].map({1: True, 0: False})

    return response_df

  def update_existing_assets(self): 
    response_df = self.request_cmc_assets()

    # Drop the old columns
    self.mapping.drop(['is_active', 'first_historical_data', 'last_historical_data','updated_at',"address"], axis=1, inplace=True)

    # Merge the dataframes
    self.mapping = pd.merge(self.mapping, response_df[['id', 'is_active', 'first_historical_data', 'last_historical_data',"address"]], left_on='cmc_id', right_on='id', how='left')

    # Drop the 'id' column from the merged dataframe
    self.mapping.drop('id', axis=1, inplace=True)

    # Add the 'updated_at' column with the current date
    self.mapping['updated_at'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    self.adjust_dtypes()

    self.mapping.to_parquet("mapping.parquet")
    shutil.copy('mapping.parquet', 'drive/My Drive/[6] CryptoData/Data')
                
  def add_new_assets(self): 
    response_df = self.request_cmc_assets()

    # Find the ids that are not in the mapping dataframe
    new_assets = response_df[~response_df['id'].isin(self.mapping['cmc_id'])].copy()

    # Drop the 'status' and 'rank' columns
    new_assets.drop(['status', 'rank'], axis=1, inplace=True)

    # Rename the columns to match the mapping dataframe
    new_assets.rename(columns={
        'id': 'cmc_id',
        'name': 'cmc_name',
        'symbol': 'cmc_symbol',
        'slug': 'cmc_slug',
        'is_active': 'is_active',
        'first_historical_data': 'first_historical_data',
        'last_historical_data': 'last_historical_data'
    }, inplace=True)

    # Add the 'updated_at' column with the current date
    new_assets['updated_at'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    # Generate a UUID for each new asset
    new_assets['internal_id'] = [uuid.uuid4() for _ in range(len(new_assets))]

    new_assets["cmc_market_data_available"] = True

    # Append the new assets to the mapping dataframe
    self.mapping = pd.concat([self.mapping, new_assets], ignore_index=True)

    self.adjust_dtypes()

    self.mapping.to_parquet("mapping.parquet")
    shutil.copy('mapping.parquet', 'drive/My Drive/[6] CryptoData/Data')

  def adjust_dtypes(self): 
    self.mapping["is_active"] = self.mapping["is_active"].astype(bool)
    self.mapping["first_historical_data"] = pd.to_datetime(self.mapping["first_historical_data"])
    self.mapping["last_historical_data"] = pd.to_datetime(self.mapping["last_historical_data"])
    self.mapping["updated_at"] = pd.to_datetime(self.mapping["updated_at"])
    

