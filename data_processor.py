# %%-- todo:
'''
Try to do inter variable plotting
'''
# %%-

# %%-- Import:
import pyodbc
from sqlalchemy import create_engine
import csv
from data_processor_object import *
import matplotlib.pyplot as plt
# %%-


# %%-- Test the object:
starting_day = '2022_6_11'
ending_day = '2022_6_15'
starting_time = '8:00:00 AM'
ending_time = '9:00:32 PM'
data1 = module_data_processor(path = r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\raw_data\2022-05-31_2022-07-01.accdb', starting_day = starting_day, ending_day = ending_day, starting_time = starting_time, ending_time = ending_time)
# data1.table_name_reader()[0]
# data1.data_reader_day(date='2022_5_31')
data1.date_selector()
# data1.zero_remover()
# data1.data_ploter(target_name = 'Voc')
# %%-
