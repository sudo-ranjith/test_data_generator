import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from datetime import datetime
import sys
import traceback
import uuid
import names
import random
import  time


DB_SERVER = "localhost"       
DB_NAME = "test_db_name"
DB_USER = "postgres"
DB_PWD = "*********"
CONN_STRING = "dbname=%s user=%s password=%s host=%s" %(DB_NAME, DB_USER, DB_PWD, DB_SERVER)
DATA_COUNT = 10000

TABLE_NAME = "users"


def random_phone_num_generator():
    first = str(random.randint(100, 999))
    second = str(random.randint(1, 888)).zfill(3)
    last = (str(random.randint(1, 9998)).zfill(4))
    while last in ['1111', '2222', '3333', '4444', '5555', '6666', '7777', '8888']:
        last = (str(random.randint(1, 9998)).zfill(4))
    return '{}-{}-{}'.format(first, second, last)


try:
    start_time = time.time()
    with psycopg2.connect(CONN_STRING, cursor_factory=RealDictCursor) as conn_pg:
        with conn_pg.cursor() as cur_pg:
            for data in range(DATA_COUNT):
                user_id = uuid.uuid4()     
                created_by = "bot"
                created_dt = datetime.now()
                updated_by = "admin"
                updated_dt = datetime.now()
                first_name = names.get_first_name()
                last_name = names.get_last_name()
                email = f"{first_name + last_name}.{data}@gmail.com"
                password = "password"
                phone_no = random_phone_num_generator()
                status = "pass"
                token = uuid.uuid4()
                user_name = first_name + last_name
                address = uuid.uuid4() 
                organization = uuid.uuid4() 
                role_id = uuid.uuid4() 


                QRY = f"insert into {TABLE_NAME} (userid, created_by, created_dt, updated_by, updated_dt, email, first_name, last_name, password, phone_no, status, token, user_name, address, organisation, role_id) VALUES ('{user_id}', '{created_by}', '{created_dt}', '{updated_by}', '{updated_dt}', '{email}', '{first_name}', '{last_name}', '{password}', '{phone_no}', '{status}', '{token}', '{user_name}', '{address}', '{organization}', '{role_id}')"
                cur_pg.execute(QRY)
                print(f"inserted succedssfully : \n {QRY}")

except psycopg2.DatabaseError as e:
    print(traceback.format_exc())
    if conn_pg:
        conn_pg.rollback()

finally:
    if cur_pg:
        cur_pg.close()

    if conn_pg:
        conn_pg.close()
    end_time = time.time()
    print('PROCESS COMPLETED...')
    print(f'PROCESS TIME {end_time - start_time}...')
