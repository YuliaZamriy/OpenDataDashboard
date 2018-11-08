import pandas as pd

from datetime import datetime, timedelta, time, date
from ga_functions import return_ga_data


def load_usp_data():
  """
  Collects users, sessions, pageviews (usp) from google analytics api

  Returns:
    pandas data frame
  """

  usp_data = return_ga_data(
      start_date='2017-09-21',
      end_date='today',
      # last part of the url: report/visitors-overview/a48417207w79902137p82639170
      view_id='82639170',
      metrics=[
          {'expression': 'ga:users'},
          {'expression': 'ga:newUsers'},
          {'expression': 'ga:sessions'},
          {'expression': 'ga:pageviews'},
          {'expression': 'ga:bounces'}
      ],
      dimensions=[
          {'name': 'ga:date'},
      ],
  )

  # convert to correct data types
  usp_data['ga:users'] = pd.to_numeric(usp_data['ga:users'])
  usp_data['ga:newUsers'] = pd.to_numeric(usp_data['ga:newUsers'])
  usp_data['ga:sessions'] = pd.to_numeric(usp_data['ga:sessions'])
  usp_data['ga:pageviews'] = pd.to_numeric(usp_data['ga:pageviews'])
  usp_data['ga:bounces'] = pd.to_numeric(usp_data['ga:bounces'])
  usp_data['ga:date'] = pd.to_datetime(usp_data['ga:date'], format='%Y%m%d')

  return usp_data


def make_usp(usp_data, freq='d'):
  """
  Groups input data frame to requested frequency

  Args:
    usp: pandas data frame

  Kwargs:
    freq: char with desired frequency (default = 'd' daily)

  Returns:
    pandas data frame
  """

  usp = usp_data.groupby(pd.Grouper(key='ga:date', freq=freq)).sum()
  usp = usp.reset_index()
  usp['bounce_rate'] = usp['ga:bounces'] / usp['ga:sessions']

  return usp

# user sources from google analytics api


def load_sources_data():
  """
  Collects users by channel data from google analytics api

  Returns:
    pandas data frame
  """

  sources_data = return_ga_data(
      start_date='2017-09-21',
      end_date='today',
      # last part of the url: report/visitors-overview/a48417207w79902137p82639170
      view_id='82639170',
      metrics=[
          {'expression': 'ga:users'},
      ],
      dimensions=[
          {'name': 'ga:channelGrouping'},
          {'name': 'ga:date'},
      ],
  )

  # convert to correct data types
  sources_data['ga:users'] = pd.to_numeric(sources_data['ga:users'])
  sources_data['ga:date'] = pd.to_datetime(sources_data['ga:date'], format='%Y%m%d')

  return sources_data


def make_sources(sources_data, freq='M'):
  """
   Groups input data frame on requested level

  Args:
    sources_data: pandas data frame

  Kwargs:
    freq: char with desired frequency (default = 'M' monthly)

  Returns:
    pandas data frame
  """
  sources = sources_data.groupby(['ga:channelGrouping', pd.Grouper(key='ga:date', freq=freq)]).sum().reset_index()
  sources['ga:date'] = sources['ga:date'].astype('datetime64[M]')

  return sources
