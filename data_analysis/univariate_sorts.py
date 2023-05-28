from datetime import datetime, timedelta
import pandas as pd
import numpy as np

import warnings

import pyarrow
import fastparquet


from scipy.stats.mstats import winsorize
from scipy.stats import mstats
import ast

"""#Univariate Portfolio"""

class UnivariatePortfolio(): 

  def __init__(self, market_data, sorting_variable, n_portfolios, rebalance_increments,formation_type, threshold):
    
    if sorting_variable in market_data.columns: 
      self.sorting_variable = sorting_variable
    else: 
      raise Exception(f"{sorting_variable} not in columns of market_data.")

    self.market_data = self.clean_sorting_variable(market_data ,self.sorting_variable)

    self.n_portfolios = n_portfolios

    self.rebalance_increments = rebalance_increments

    self.threshold = threshold
    
    valid_types = ["equal","mcap"]
    if formation_type not in valid_types:
      raise Exception("Formation Type is invalid.")
    else: 
      self.formation_type = formation_type 

#main
  def backtest_single_date(self, start_date: str,) -> pd.DataFrame: 
    
    self.start_date = start_date

    #Handling Incorrect User Input
    if pd.to_datetime(self.start_date) < usd_market_data["date"].min():
      raise Exception("Error: start_date is earlier than earliest date in usd_market_data")

    if pd.to_datetime(self.start_date) > usd_market_data["date"].max() - pd.DateOffset(days=7):
      raise Exception("Error: start_date is later than latest date in usd_market_data")


    rebalance_dates = self.generate_rebalance_dates(start_date = self.start_date, 
                                                    rebalance_increments = self.rebalance_increments,
                                                    market_data = self.market_data)
    
    #Extract only relevant dates for the backtesting from market_data
    _market_data = self.filter_by_date(dates = rebalance_dates,
                        market_data = self.market_data)
    
    
    #Calculate the returns for each asset.
    _market_data = self.calculate_returns(market_data = _market_data)

    return _market_data

    #Apply various filters
    _market_data = self.various_filters(threshold = self.threshold,
                         market_data = _market_data)
    

    #Sort each value into a portfolio
    _market_data = self.sort_values_into_portfolios(n_portfolios = self.n_portfolios,
                                                    market_data = _market_data)

    #Adjust the weights each asset for a given portfolio
    _market_data = self.adjust_weights(formation_type = self.formation_type,
                                       market_data = _market_data)

    #Calculate the weighted returns for a given assets
    _market_data["weighted_return"] = _market_data["return"] * _market_data["weight"]

    #Calculate the returns of the portfolio
    performance_summary = self.calculate_portfolio_returns(market_data = _market_data)

    return performance_summary

  def backtest_multiple_dates(self, start_date: str) -> pd.DataFrame: 
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    dates = [start_date + timedelta(days=i) for i in range(7)]
    
    multi_implementationn_performance = []
    
    for date in dates:
      print(date.strftime("%Y-%m-%d"))

      performance = self.backtest_single_date(date)
      
      multi_implementationn_performance.append(performance)
    
    return pd.concat(multi_implementationn_performance)

# Date Functions
  def generate_rebalance_dates(self, start_date, rebalance_increments, market_data: pd.DataFrame) -> list:
    rebalance_dates = []
    current_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(market_data['date'].max()) - pd.DateOffset(days=7)
    
    
    while current_date <= end_date:
        rebalance_dates.append(current_date)
        current_date += pd.DateOffset(days=rebalance_increments)

    return rebalance_dates

#Filters
  def filter_by_date(self, dates: list, market_data: pd.DataFrame) -> pd.DataFrame:
      market_data_copy = market_data.copy()

      market_data_copy = market_data_copy[market_data_copy['date'].isin(dates)]

      return market_data_copy

  def various_filters(self, threshold: int, market_data: pd.DataFrame) -> pd.DataFrame: 
    #Filter_out_missing_sorting_variables
    market_data = market_data.dropna(subset=[self.sorting_variable])
    
    #Filter out NAN in the returns column
    market_data = market_data.dropna(subset=["return"])

    #Filter out assets with small market capitalization
    market_data = market_data[(market_data['market_cap'] >= threshold) & market_data['market_cap'].notna()]

    return market_data

#Sorting Functions
  def sort_values_into_portfolios(self, n_portfolios: int, market_data: pd.DataFrame) -> pd.DataFrame:
      sorted_data = pd.DataFrame()

      # Group the market_data by date
      grouped_data = market_data.groupby('date')

      # Iterate over each date group
      for date, group in grouped_data:
          # Sort the group based on the sorting variable
          sorted_group = group.sort_values(self.sorting_variable)

          # Assign portfolios using quantiles
          sorted_group['portfolio'] = pd.qcut(sorted_group[self.sorting_variable], n_portfolios, labels=False)

          # Concatenate the sorted group to the sorted_data DataFrame
          sorted_data = pd.concat([sorted_data, sorted_group])

      return sorted_data

  def adjust_weights(self, formation_type: str, market_data: pd.DataFrame) -> pd.DataFrame:
      
      if formation_type == "mcap":
    
          # Group the DataFrame by date and portfolio
          grouped_data = market_data.groupby(['date', 'portfolio'])

          # Calculate the sum of market_cap within each group
          sum_market_cap = grouped_data['market_cap']

          # Calculate the weight by dividing each market_cap value by the sum
          market_data['weight'] =  sum_market_cap.transform(lambda x: x / x.sum())


      elif formation_type == "equal":
          # Group the DataFrame by date and portfolio
          grouped_data = market_data.groupby(['date', 'portfolio'])

          # Count the number of rows within each group
          count_rows = grouped_data['portfolio'].transform('count')

          # Calculate the weight as 1 divided by the count
          market_data['weight'] = 1 / count_rows

      return market_data
  
# Calculate Returns
  def calculate_returns(self, market_data: pd.DataFrame) -> pd.DataFrame: 
    # Pivot the market_data DataFrame to have internal_id as columns and date as the index
    pivot_market_data = market_data.pivot(index='date', columns='internal_id', values='close')

    # Calculate the percentage change for each column (internal_id) using pct_change()
    pivot_market_data = pivot_market_data.pct_change()

    pivot_market_data = pivot_market_data.shift(-1)

    # Reshape the DataFrame to have a stacked format
    pivot_market_data = pivot_market_data.stack().reset_index()

    pivot_market_data.columns = ["date", "internal_id", "return"]

    # Merge the sorted data (sorting_variable) and market_cap based on internal_id and date
    merged_data = pd.merge(market_data, pivot_market_data, on=['internal_id', 'date'], how="left")

    # Return the merged DataFrame with calculated percentage changes
    return merged_data

# Do whatever
  def clean_sorting_variable(self, market_data, sorting_variable): 
    market_data = market_data.replace([np.inf, -np.inf], np.nan).dropna(subset=[sorting_variable])

    return market_data
    
#Portfolio Performance Evaluation
  def calculate_portfolio_returns(self, market_data: pd.DataFrame) -> pd.DataFrame:
      # Group the DataFrame by date and portfolio
      grouped_data = market_data.groupby(['date', 'portfolio'])

      # Sum the weighted_returns within each group
      portfolio_returns = grouped_data['weighted_return'].sum()

      # Reset the index to make 'date' and 'portfolio' regular columns
      portfolio_returns = portfolio_returns.reset_index()

      # Pivot the DataFrame to have portfolios as columns and returns as rows
      pivoted_returns = portfolio_returns.pivot(index='date', columns='portfolio', values='weighted_return')

      return pivoted_returns
