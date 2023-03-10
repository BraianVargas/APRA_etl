# %%
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import click
from flask import current_app, g
from flask.cli import with_appcontext
import os

# %%

def getDB():
     db = mysql.connector.connect(
          host = "localhost",
          user = "root",
          password = "",
          database = "apra_etl",
          connect_timeout = 60
     )
     c = db.cursor(dictionary=True)
     return db, c


# %%
def abrev(text):
     words = text.split()
     initials = [word[0] for word in words]
     abbreviation = ''.join(initials).upper()

     return abbreviation

# %%
# process the historical File only Once
folder = "./data/historical/"
files = os.listdir(folder)
files = [f for f in files]

filename = files[0]
historical_df = pd.read_excel(os.path.join(folder,filename), sheet_name='Table 1')


# %%
columns = historical_df.columns

for col in columns:
     if len(col.split()) > 1:
          # Generate an abbreviation for the column name
          abbr = abrev(col)
          if abbr in historical_df.columns:
               pass
          else:
               # Update the DataFrame with the abbreviated column name
               historical_df.rename(columns={col: abbr}, inplace=True)

db, c = getDB()

# %%
try:
     engine = create_engine('mysql+mysqlconnector://root@127.0.0.1/apra_etl', connect_args={'connect_timeout': 120})

     historical_df.to_sql('datos_historicos', con=engine, if_exists='replace', chunksize=1000)
except Exception as e:
     print(e)


