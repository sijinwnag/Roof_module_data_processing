import pyodbc
from sqlalchemy import create_engine
import csv

# create the connection
msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
# print(msa_drivers)
con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\sijin wang\Desktop\Thesis\RA\Module_data_project\data\raw_data\2022-05-31_2022-07-01.accdb;'
conn = pyodbc.connect(con_string)

# Create the cursor object
cur = conn.cursor()
cur.execute('SELECT * FROM 2022_5_31IV')

# inside this table:
for row in cur.fetchall():
    
