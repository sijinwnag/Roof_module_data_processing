# %%-- todo:
'''
Filter the data including both date and time.
'''
# %%-

# %%-- Import:
import pyodbc
from sqlalchemy import create_engine
import csv
from data_processor_object import *
# %%-


# %%-- Test the object:
starting_day = '2022_6_1'
ending_day = '2022_6_4'
data1 = module_data_processor(path = r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\raw_data\2022-05-31_2022-07-01.accdb', starting_day = starting_day, ending_day = ending_day)
# data1.table_name_reader()[0]
# data1.data_reader_day(date='2022_5_31')
data1.date_selector()
# %%-
