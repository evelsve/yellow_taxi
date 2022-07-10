import argparse
import pyarrow.parquet as pq
import pandas as pd
from tqdm import tqdm
import argparse
from os import listdir
from csv import writer

from timeit import time
from sqlalchemy import create_engine

from timeit import default_timer as timer

import warnings
warnings.filterwarnings('ignore')



def do_time(start, end):
    time = end - start
    return round(time/60, 2)


def zeros(zone):
    zone = str(zone)
    while len(zone) < 3:
        zone = '0' + zone
    return zone


def do_unique_ids(df):
    # creates unique id's based on vendor, pick-up, drop-off locations and dates
    trip_ids = list()
    for i, row in df.iterrows():
        vid, puid, doid, pudate, dodate = row['VendorID'], row['PULocationID'], row['DOLocationID'], row['tpep_pickup_datetime'], row['tpep_dropoff_datetime']
        puid, doid =  zeros(puid), zeros(doid)
        unique_id = f'{vid}{puid}{doid}{pudate}{dodate}'.replace('-', '').replace(':', '').replace(' ', '')
        trip_ids.append(unique_id)
    df.insert(loc=0, column='trip_id', value=trip_ids)
    return df


def do_report(data):
    # adapted from: https://www.pschwan.de/how-to/setting-up-data-quality-reports-with-pandas-in-no-time
    data_types = pd.DataFrame(
        data.dtypes,
        columns=['Data Type'])

    missing_data = pd.DataFrame(
        data.isnull().sum(),
        columns=['Missing Values']
    )

    unique_values = pd.DataFrame(
        columns=['Unique Values']
    )
    for row in list(data.columns.values):
        unique_values.loc[row] = [data[row].nunique()]

    maximum_values = pd.DataFrame(
        columns=['Maximum Value']
    )
    for row in list(data.columns.values):
        try:
            maximum_values.loc[row] = [data[row].max()]
        except:
            maximum_values.loc[row] = None

    maximum_values = pd.DataFrame(
        columns=['Maximum Value']
    )
    for row in list(data.columns.values):
        try:
            maximum_values.loc[row] = [data[row].max()]
        except:
            maximum_values.loc[row] = None

    minimum_values = pd.DataFrame(
        columns=['Minimum Value']
    )
    for row in list(data.columns.values):
        try:
            minimum_values.loc[row] = [data[row].min()]
        except:
            minimum_values.loc[row] = None

    dq_report = data_types.join(missing_data).join(unique_values).join(maximum_values).join(minimum_values)

    return dq_report


def nan_rows_count(raw_df):
    # counts number of rows with nan values
    rows_with_nan = list()
    for index, row in tqdm(raw_df.iterrows()):
        is_nan_series = row.isnull()
        if is_nan_series.any():
            rows_with_nan.append(index)
    return rows_with_nan


def do_metrics(file_name, clean_df, raw_df, raw_report, duplicated_count, start):

    date = file_name.replace('.parquet', '').split('_')[-1]

    error_to_normal = round(len(clean_df)/len(raw_df)*100, 4)
    empty_values = round(raw_df.isnull().sum().sum()/raw_df.notnull().count().sum()*100, 4)
    missing_values = raw_report['Missing Values'].sum()
    nan_rows = len(nan_rows_count(raw_df.drop('airport_fee', axis=1)))
    data_usage = round(clean_df.memory_usage(deep=True).sum()*0.000001, 2)
    data_reduction = round((raw_df.memory_usage(deep=True).sum()* 0.000001 - data_usage), 2)
    duplicates =round(duplicated_count/len(raw_df)*100, 4)

    end = timer()
    processing_time = do_time(start, end)

    return {'date':date,
            'non_error_ratio': error_to_normal,
            'empty_values_ratio': empty_values,
            'missing_values': missing_values,
            'nan_rows_without_airport_fee': nan_rows,
            'data_usage': data_usage,
            'data_reduction_after_cleanup': data_reduction,
            'duplicates_ratio': duplicates,
            'processing_time':processing_time}









def main():

    parser = argparse.ArgumentParser()   

    parser.add_argument('--user', type=str, required=True)
    parser.add_argument('--db', type=str, required=True)
    parser.add_argument('--psw', type=str, required=True)
    parser.add_argument('--y', type=str, required=False)
    parser.add_argument('--m', type=str, required=False)
    
    args = parser.parse_args() 

    paths = listdir(f'data/{args.y}/{args.m}')
    paths.sort()

    for i, file_name in enumerate(paths):
        print(f'Loading file... {file_name}')
        start = timer()
        table = pq.read_table(f'data/{args.y}/{args.m}/{file_name}')
        df = table.to_pandas()
        raw_df = df
        df = df.drop('airport_fee', axis=1)

        df = do_unique_ids(df)

        raw_report = do_report(df)

        duplicates = df[df.duplicated('trip_id')].index
        df.drop(duplicates, axis=0, inplace=True)
        duplicates_count = len(raw_df) - len(df)
        df = df.dropna()
        df = df.drop('str_date', axis=1)

        # clean_report = do_report(df)

        metrics_dict = do_metrics(file_name, df, raw_df, raw_report, duplicates_count, start)
        with open(f'metrics/metrics_{args.y}.csv', 'a', newline='') as f:
            writer(f).writerow(metrics_dict.values())

        df.columns = ['trip_id',
                'vendor_id',
                'pickup_date',
                'drop_off_date', 
                'passenger_count',
                'trip_distance',
                'pu_location_id',
                'do_location_id',
                'rate_code_id',
                'flag',
                'payment_type_id',
                'fare_amount',
                'extra',
                'mta_tax',
                'improvement_surcharge',
                'tip_amount',
                'tolls_amount',
                'total_amount',
                'congestion_surcharge']

        df = df[['trip_id',
                'vendor_id',
                'pu_location_id',
                'do_location_id',
                'rate_code_id',
                'payment_type_id',
                'pickup_date',
                'drop_off_date', 
                'passenger_count',
                'trip_distance',
                'flag',
                'fare_amount',
                'extra',
                'mta_tax',
                'improvement_surcharge',
                'tip_amount',
                'tolls_amount',
                'total_amount',
                'congestion_surcharge']]

        engine = create_engine(f"mysql+pymysql://{args.user}:{args.psw}@localhost/{args.db}")
        df.to_sql('fact_trips', con=engine, if_exists='append', chunksize=100)
        engine.dispose()
        print('File done.')


if __name__ == '__main__':
    main()