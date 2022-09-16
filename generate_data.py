from faker import Faker
import psycopg2
from time import sleep
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

if __name__ == '__main__':
    conn = create_engine("postgresql://postgres:AVENIR123o@localhost:5432/postgres") 
    faker = Faker() 
    fields = ['job','company','residence','username']
    i = 0

    while True: 
        data = faker.profile(fields)
        data['timestamp'] = datetime.now()
        df = pd.DataFrame(data, index = [i])
        print(f"Inserting data {data}")
        df.to_sql('streams',conn, if_exists='append')
        i +=1 
        sleep(1)