import sqlalchemy
import pandas as pd
from sqlalchemy.types import Date, CHAR, Numeric
from utility import logger


DB_USER = 'ds_user'
DB_PASS = "admin"
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'project1-task'

dtype = {
    'from_date': Date(),
    'to_date': Date(),
    'chapter': CHAR(1),
    'ledger_account': CHAR(5),
    'characteristic': CHAR(1),
    'balance_in_rub': Numeric(23, 8),
    'r_balance_in_rub': Numeric(23, 8),
    'balance_in_val': Numeric(23, 8),
    'r_balance_in_val': Numeric(23, 8),
    'balance_in_total': Numeric(23, 8),
    'r_balance_in_total': Numeric(23, 8),
    'turn_deb_rub': Numeric(23, 8),
    'r_turn_deb_rub': Numeric(23, 8),
    'turn_deb_val': Numeric(23, 8),
    'r_turn_deb_val': Numeric(23, 8),
    'turn_deb_total': Numeric(23, 8),
    'r_turn_deb_total': Numeric(23, 8),
    'turn_cre_rub': Numeric(23, 8),
    'r_turn_cre_rub': Numeric(23, 8),
    'turn_cre_val': Numeric(23, 8),
    'r_turn_cre_val': Numeric(23, 8),
    'turn_cre_total': Numeric(23, 8),
    'r_turn_cre_total': Numeric(23, 8),
    'balance_out_rub': Numeric(23, 8),
    'r_balance_out_rub': Numeric(23, 8),
    'balance_out_val': Numeric(23, 8),
    'r_balance_out_val': Numeric(23, 8),
    'balance_out_total': Numeric(23, 8),
    'r_balance_out_total': Numeric(23, 8)
}


engine = sqlalchemy.create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def csv_to_db():
    logger('Начало загрузки данных из csv в dm.dm_f101_round_f_v2', engine)
    try:

        df = pd.read_csv('output.csv')
        df.to_sql('dm_f101_round_f_v2', engine, if_exists='replace',
                  index=False, schema='dm', dtype=dtype)
        logger('Загрузка данных из csv в dm.dm_f101_round_f_v2 прошла успешно', engine)
    except:
        logger('Ошибка при загрузке данных из csv в dm.dm_f101_round_f_v2', engine)


csv_to_db()
