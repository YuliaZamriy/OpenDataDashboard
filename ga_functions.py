import pandas as pd
from ga_config import service
from datetime import datetime
from dateutil.rrule import rrule, DAILY
from time import sleep


def get_report(start_date, end_date, view_id, metrics, dimensions, analytics=service, dimensionFilterClauses=[], segments=[]):
  """
  Generates a report with GA data based on input arguments

  Args:
    start_date: str, 'yyyy-mm-dd'
    end_date: str, 'yyyy-mm-dd' or 'today'
    view_id: str, digits for GA report
    metrics: list of dictionaries with GA-defined metrics
    dimensions: list of dictionaries with GA-defined dimentions

  Kwargs:
    analytics: googleapiclient.discovery.Resource
    dimensionFilterClauses: list  (TODO: what is this for???)
    segments: list  (TODO: what is this for???)

  Returns:
    dictionary with key 'reports'
  """

    response = analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view_id,
                    'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                    'metrics': metrics,
                    'dimensions': dimensions,
                    'pageSize': 10000,
                    'dimensionFilterClauses': dimensionFilterClauses,
                    'segments': segments,
                }]
        }
    ).execute()

    return response


def convert_reponse_to_df(response):
  """
  Converts the dictionary with GA report into a pandas daframe

  Arg:
    response: dictionary with key 'report'

  Returns:
    pandas data frame
  """

  # initialize list of data rows (dictionaries)
    ga_list = []

    # parse report data
    for report in response.get('reports', []):

      # dictionary with column headers
      columnHeader = report.get('columnHeader', {})
      # list of selected dimensions
      dimensionHeaders = columnHeader.get('dimensions', [])
      # list of metrics (same as dimensions) and data types
      metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

      # list of dictionaries with data
      rows = report.get('data', {}).get('rows', [])

    # iterate through all rows of data
    for row in rows:
      ga_dict = {}
      dimensions = row.get('dimensions', [])
      # extract actual values for selected metrics
      dateRangeValues = row.get('metrics', [])
      # create a dictionary with keys => column names (dimensions)
      # values => dimension values
      for header, dimension in zip(dimensionHeaders, dimensions):
        ga_dict[header] = dimension

      # for each combination of dimensions
      for i, values in enumerate(dateRangeValues):
        # input the values
        for metric, value in zip(metricHeaders, values.get('values')):
          if ',' in value:
            ga_dict[metric.get('name')] = float(value)
          else:
            ga_dict[metric.get('name')] = int(value)

      ga_list.append(ga_dict)

    return pd.DataFrame(ga_list)


def return_ga_data(start_date, end_date, view_id, metrics, dimensions, split_dates=False, group_by=[], dimensionFilterClauses=[], segments=[]):
  """
  Returns GA report converted to pandas data frame, split by date if requestes
  TODO:  not clear about the split_dates intention

  Args:
    start_date: str, 'yyyy-mm-dd'
    end_date: str, 'yyyy-mm-dd' or 'today'
    view_id: str, digits for GA report
    metrics: list of dictionaries with GA-defined metrics
    dimensions: list of dictionaries with GA-defined dimentions

  Kwargs:
    split_dates: bool (TODO: what is this for???)
    group_by: list (TODO: what is this for???)
    dimensionFilterClauses: list (TODO: what is this for???)
    segments: list (TODO: what is this for???)

  Returns:
    pandas data frame
  """

    if split_dates == False:
      return convert_reponse_to_df(get_report(service, start_date, end_date, view_id, metrics, dimensions, dimensionFilterClauses, segments))
    else:
      start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
      if end_date == 'today':
        end_date = datetime.today().date()
      else:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    df_total = pd.DataFrame()

    # for each day between start and end date
    # generate a separate table
    for dt in rrule(freq=DAILY, dtstart=start_date, until=end_date):
      dt = str(dt.date())
      df_total = df_total.append(convert_reponse_to_df(get_report(service, dt, dt, view_id, metrics, dimensions, dimensionFilterClauses, segments)))
      sleep(1)

    if len(group_by) != 0:
      df_total = df_total.groupby(group_by).sum()

    return df_total
