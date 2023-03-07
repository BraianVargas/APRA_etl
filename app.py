import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import click
from flask import current_app, g
from flask.cli import with_appcontext

def getDB():
    db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "",
        database = "apra_etl"
    )
    c = db.cursor(dictionary=True)
    return db, c

df = pd.read_excel('./data/monthly_banking_statistics_june_2019_back_series .xlsx', sheet_name='Table 1')

columns = df.columns
db, c = getDB()
engine = create_engine('mysql+mysqlconnector://root@127.0.0.1/apra_etl')
try:
    df.to_sql('datos_historicos', con=engine, if_exists='replace')
except Exception as e:
    print(e)