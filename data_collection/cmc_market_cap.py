import requests
from datetime import datetime, timedelta
import datetime
import pandas as pd

class MarketCap():
    def __init__(self, market_cap):
        self.market_cap = market_cap

    def _request_market_cap_from_cmc(self, dates: list):
        number_of_assets = 1000000
        data_list = []  # Accumulate the data in a list

        for day in dates:
            URL = f"https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listings/historical?date={day}&start=1&limit={number_of_assets}&convertId=2781"
            response = requests.get(URL)

            print(day, response.status_code, URL)
            if response.status_code == 200:
                json_response = response.json()

                if 'status' in json_response and json_response['status'].get('error_message') == 'SUCCESS':
                    data = json_response['data']
                    df = pd.json_normalize(data)
                    data_list.append(df)  # Append the DataFrame to the list
                else:
                    print(f"No data in response for date {day}")

        if data_list:
            updated_market_cap = pd.concat(data_list)  # Concatenate the accumulated DataFrames
            return updated_market_cap
        
        else:
            return None

    def update_market_cap(self):
        most_recent_date = self._get_most_recent_date()
        date_list = self.generate_date_list(most_recent_date)
        updated_market_cap = self._request_market_cap_from_cmc(date_list)

        if updated_market_cap is not None:
            # Update the market_cap DataFrame
            # Get the column names and dtypes from the source dataframe
            updated_market_cap = updated_market_cap.astype(self.market_cap.dtypes)

            updated_market_cap["tags"] = updated_market_cap["tags"].astype(str)
            updated_market_cap["quotes"] = updated_market_cap["quotes"].astype(str)
    
            self.market_cap = pd.concat([self.market_cap, updated_market_cap])

        return self.market_cap
            
    def _get_most_recent_date(self):
        # Convert 'lastUpdated' column to datetime
        self.market_cap['lastUpdated'] = pd.to_datetime(self.market_cap['lastUpdated'])

        # Get the most recent date
        most_recent_date = self.market_cap['lastUpdated'].max().date()

        return most_recent_date.strftime('%Y-%m-%d')

    @staticmethod
    def generate_date_list(start_date):
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)
        end = datetime.datetime.now()
        date_list = [(start + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end - start).days)]
        return date_list
