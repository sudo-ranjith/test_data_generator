import os
import pandas as pd
from datetime import datetime
import sys
import traceback
import uuid
import names
import random
import  time


DATA_COUNT = 10000


def random_phone_num_generator():
    first = str(random.randint(100, 999))
    second = str(random.randint(1, 888)).zfill(3)
    last = (str(random.randint(1, 9998)).zfill(4))
    while last in ['1111', '2222', '3333', '4444', '5555', '6666', '7777', '8888']:
        last = (str(random.randint(1, 9998)).zfill(4))
    return '{}-{}-{}'.format(first, second, last)


try:
    start_time = time.time()
    # create a dataframe and add data to it
    df = pd.DataFrame(columns=['userid', 'created_by', 'created_dt', 'updated_by', 'updated_dt', 'email', 'first_name', 'last_name', 'password', 'phone_no', 'status', 'token', 'user_name', 'address', 'organisation', 'role_id'])

    for data in range(DATA_COUNT):
        print(f"data generation started {data}")
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

        df.loc[data] = [user_id, created_by, created_dt, updated_by, updated_dt, email, first_name, last_name, password, phone_no, status, token, user_name, address, organization, role_id]
        print(f"it is adding row to the data frame...")
    # write to csv file
    df.to_csv('users.csv', index=False)
    print(f"Dataframe created and saved to csv file successfully")

except Exception:
    print(traceback.format_exc())

finally:
    end_time = time.time()
    print('PROCESS COMPLETED...')
    print(f'PROCESS TIME {end_time - start_time}...')
