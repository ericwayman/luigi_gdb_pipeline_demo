#generate table with users and time stamps
import time
import datetime
from random import sample,randint,seed
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

#gpdb connection
connection = pg.connect(
        host=os.getenv("GPDB_HOST"),
        database=os.getenv("GPDB_DATABASE"),
        user=os.getenv("GPDB_USER"),
        password=os.getenv("GPDB_PASSWORD"),
        port = os.getenv("GPDB_PORT"))
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#parameters
USERS = list('abcdefg')
MAX_DATE = "15/01/2017"
MIN_DATE = "01/01/2017"
SEED = 1

#helper functions
def unix_time_from_string(date_str):
    "converts string of form dd/mm/yyyy to unix time stamp"
    ut = int(time.mktime(datetime.datetime.strptime(date_str, "%d/%m/%Y").timetuple()))
    return ut

def generate_unix_time_stamps(num_dates,min_date,max_date):
    """generate num_dates by hour in min_date,max_date range"""
    min_seconds = unix_time_from_string(min_date)
    max_seconds = unix_time_from_string(max_date)
    unix_times = sample(range(min_seconds,max_seconds),num_dates)
    return unix_times



def main():
    seed(SEED)
    with connection.cursor() as curs:
        curs.execute(
            """DROP TABLE IF EXISTS sample_data;
            CREATE TABLE sample_data
            (
            user_name     varchar,
            start_time   bigint
            );"""
        )
    for user in USERS:
        NUM_FLOWS = randint(800,2400)
        print(NUM_FLOWS)
        date_list = generate_unix_time_stamps(NUM_FLOWS,min_date=MIN_DATE,max_date=MAX_DATE)
        rows = [(user,str(date)) for date in date_list]
        records_list_template = ','.join(['%s'] * len(rows))
        insert_query = 'INSERT INTO sample_data (user_name, start_time) VALUES {}'.format(records_list_template)
        with connection.cursor() as curs:
            curs.execute(insert_query, rows)


if __name__ == "__main__":
    main()