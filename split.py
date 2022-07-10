import pyarrow.parquet as pq
import pandas as pd
import os
import argparse



def to_new_files(chosen_year, chosen_month):
    table = pq.read_table(f"data/original/yellow_tripdata_{chosen_year}-{chosen_month}.parquet")
    df = table.to_pandas()
    df['str_date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date.astype(str)
    dates = df['str_date'].unique()
    print(f'Amount of unique dates in chosen file: {len(dates)}')

    #creating space
    if chosen_year not in os.listdir(f'data'):
         os.mkdir(f'data/{chosen_year}')
    if chosen_month not in os.listdir(f'data/{chosen_year}'):
        os.mkdir(f'data/{chosen_year}/{chosen_month}')

    # retrieving specific days
    for date in dates:
        year, month, day = date.split('-')
        if year == chosen_year and month == chosen_month:
            only_specifc_day_df = df.loc[df['str_date'] == date]
            print(f'Day: {date}, rows: {len(only_specifc_day_df)}')

            # writing to specific year-month-day files
            only_specifc_day_df.to_parquet(f"data/{chosen_year}/{chosen_month}/yellow_tripdata_{chosen_year}-{chosen_month}-{day}.parquet")   
        








def main():
    for file_name in os.listdir('data/original'):
        if '.parquet' in file_name:
            year_month = file_name.split('_')[-1].replace('.parquet', '').split('-')
            year, month = year_month[0], year_month[1]
            to_new_files(year, month)

if __name__ == '__main__':
    main()
        
