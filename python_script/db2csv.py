import sqlalchemy
import pandas as pd
from sqlalchemy import text
from utility import logger

DB_USER = 'ds_user'
DB_PASS = "admin"
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'project1-task'


def export_to_csv():
    logger('Начало загрузки данных из dm.dm_f101_round_f в csv', engine)
    try:
        query = "SELECT * FROM dm.dm_f101_round_f"
        df = pd.read_sql(query, engine)
        df.to_csv('output.csv', index=False)
        logger('Загрузка данных из dm.dm_f101_round_f в csv закончена', engine)
    except:
        logger('Ошибка при загрузке данных из dm.dm_f101_round_f в csv', engine)


engine = sqlalchemy.create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
export_to_csv()
