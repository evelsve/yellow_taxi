import pymysql

import json
import csv
import argparse
from os import listdir


def to_database(query, c):
    cursor=c.cursor()
    cursor.execute(query)
    c.commit()


def connect_to_db(host, user, psw):
    connection = pymysql.connect(
            host=host,
            user=user,
            password=psw)
    return connection


def get_dimensions(path):
    # retrieves dimensions form file OR retrieves and updates dimensions_dict with a new dimension
    # supports json and csv additional dimension files
    paths = listdir(path)
    paths.sort()
    for file_name in paths:
        with open(f'{path}/{file_name}', mode='r') as f:
            if file_name.endswith('.json'):
                dim_dict = json.load(f)
            elif file_name.endswith('.csv'):
                csv_dim_dict = list(csv.DictReader(f, delimiter=","))
                new_dim_dict = dict()
                for item in csv_dim_dict:
                    zone = item['Zone']
                    if "'" in zone:
                        zone = zone.replace("'", '`')
                    id = item['LocationID']
                    while len(id) < 3:
                        id = '0' + str(id)
                    print(id, zone)
                    new_dim_dict[id] = zone
                dim_dict['location'] = new_dim_dict
            else:
                print(f'Random file: {file_name}')
    return dim_dict


def fill_dimensions(dim_dict, c):
    # fills database with the dimensions retrieved with gext_dimensions
    for dim in dim_dict.keys():
        create_table = f"""create table generic.dim_{dim} (
            {dim}_id int(4),
            {dim}_name varchar(100)
        )"""
        to_database(create_table, c)
        for key, val in dim_dict[dim].items():
            try:
                insertion_query = f"""insert into generic.dim_{dim} ({dim}_id, {dim}_name) 
                values ({key}, '{val}');"""
                to_database(insertion_query, c)
            except pymysql.err.ProgrammingError:
                try:
                    insertion_query = f"""insert into generic.dim_{dim} ({dim}_id, {dim}_name) 
                    values ({key}, {val});"""
                    to_database(insertion_query, c)
                except:
                    print(insertion_query)
                    pass



def main():

    parser = argparse.ArgumentParser()   

    parser.add_argument('--host', type=str, required=True)
    parser.add_argument('--user', type=str, required=True)
    parser.add_argument('--db', type=str, required=True)
    parser.add_argument('--psw', type=str, required=True)
    
    
    args = parser.parse_args() 

    c = connect_to_db(args.host, args.user, args.psw)
    
    query = f'CREATE DATABASE {args.db}'
    to_database(query, c)

    dimensions_dictionary = get_dimensions('data/other')
    fill_dimensions(dimensions_dictionary, c)

    print('Setup done.')
    c.close() 

    

if __name__ == '__main__':
    main()



