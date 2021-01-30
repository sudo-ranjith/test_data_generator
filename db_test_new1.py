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
import pycountry
from faker import Faker

# config section here...
DB_SERVER = "localhost"       
DB_NAME = "test"
DB_USER = "postgres"
DB_PWD = "test@123"
CONN_STRING = "dbname=%s user=%s password=%s host=%s" %(DB_NAME, DB_USER, DB_PWD, DB_SERVER)
DATA_COUNT = 1000

TABLE_NAME = "patient"
faker = Faker()


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

                # new table entries
                patient_id = int(data+1)
                age = random.randint(21, 99)
                agree_status = random.randint(0, 1)
                alt_phone_number = random_phone_num_generator()
                rand_int = random.randint(5, 240)
                country_data = list(pycountry.countries)[rand_int]
                country_data_mother = list(pycountry.countries)[rand_int+1]
                country_data_father = list(pycountry.countries)[rand_int-1]
                country_code = country_data.numeric
                country_name = country_data.name

                if "'" in country_name:
                    country_name = country_name.replace("'", " ").strip()

                created_dt = datetime.now()
                dob = faker.date_of_birth()
                email_id = faker.email()
                father_island = country_data_father.name
                if "'" in father_island:
                    father_island = father_island.replace("'", " ").strip()

                first_name = names.get_first_name()
                gender = "bot"
                hospital_id = int(f"{data+1}{random.randint(1, 9998)}")
                island_name = "island_name"
                last_name = names.get_last_name()
                middle_name = faker.name()
                if "'" in middle_name:
                    middle_name = middle_name.replace("'", " ").strip()
                mother_island = country_data_mother.name

                if "'" in mother_island:
                    mother_island = mother_island.replace("'", " ").strip()

                phone_number = random_phone_num_generator()
                race_ethnicity = "test race ethincity"
                race_id = rand_int
                sub_race = f"{rand_int}"
                updated_dt = datetime.now()

                QRY = f"insert into {TABLE_NAME} VALUES ('{patient_id}', '{age}', '{agree_status}', '{alt_phone_number}', '{country_code}', '{country_name}', '{created_dt}', '{dob}', '{email_id}', '{father_island}', '{first_name}', '{gender}', '{hospital_id}', '{island_name}', '{last_name}', '{middle_name}', '{mother_island}', '{phone_number}', '{race_ethnicity}', '{race_id}', '{sub_race}', '{updated_dt}')"
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
