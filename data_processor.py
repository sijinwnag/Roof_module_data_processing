# %%-- Import:
import pyodbc
from sqlalchemy import create_engine
import csv
from data_processor_object import *
# %%-

# create the connection
msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
# print(msa_drivers)
con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\sijin wang\Desktop\Thesis\RA\Module_data_project\data\raw_data\2022-05-31_2022-07-01.accdb;'
conn = pyodbc.connect(con_string)

# Create the cursor object to read how many table we got:
cur = conn.cursor()
IV_table_names = []
for row in cur.tables():
    # if the table name ends with "IV"
    if row.table_name[-2:] == 'IV':
        # print(row.table_name)
        IV_table_names.append(row.table_name)

# Create the cursor object to load the data for each
cur = conn.cursor()
cur.execute('SELECT * FROM 2022_5_31IV')

# inside this table:
for row in cur.fetchall():
    print(row)

# close the connection
cur.close()

# %%-- Test the object:
data1 = module_data_processor(path = r'C:\Users\sijin wang\Desktop\Thesis\RA\Module_data_project\data\raw_data\2022-05-31_2022-07-01.accdb')
data1.table_name_reader()[0]
data1.data_reader_day(date='2022_5_31')
# %%-
