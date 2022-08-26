# %%-- todo:
'''
convert add two datetime column together.
'''
# %%-

# %%-- Import:
import pyodbc
from sqlalchemy import create_engine
import csv
from data_processor_object import *
# %%-


# %%-- Test the object:
data1 = module_data_processor(path = r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\raw_data\2022-05-31_2022-07-01.accdb')
# data1.table_name_reader()[0]
# data1.data_reader_day(date='2022_5_31')
data1.date_selector(starting_date = '2022_5_31', ending_date = '2022_7_1')
# %%-
