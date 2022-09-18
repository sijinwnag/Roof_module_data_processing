# %%-- todo:
'''
-in the date_selector(self) function of the object: when extracting the date table from the file, create a function that automatically choose the correct path that contains this date.
'''
# %%-

# %%-- Import:
import pyodbc
from sqlalchemy import create_engine
import csv
from data_processor_object import *
import matplotlib.pyplot as plt
# %%-


# %%-- Define the object:
starting_day = '2022_5_31'
ending_day = '2022_6_3'
starting_time = '8:00:00 AM'
ending_time = '9:00:32 PM'
data1 = module_data_processor(path = r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\2022-05-31_2022-07-01.accdb', starting_day = starting_day, ending_day = ending_day, starting_time = starting_time, ending_time = ending_time)
# %%-


# %%--
# create a list of dates
data1.file_date_reader()
data1.list_of_date
# %%-

# data1.file_path_locator('2022_3_11IV')


# %%--
data1.zero_remover()
data1.df_nonzero
data1.module_selector(module_num_list=[1, 2])
# data1.df_nonzero
# data1.module_df_list[1]
# %%-


# %%-- resample for each module:
data1.multi_module_resampler(sample_length='day')
# data1.module_df_sampled[0]
# data1.module_df_sampled[1]
# %%-


# %%--plot with time:
data1.data_ploter_with_time_multimodule(target_name='Isc')

# %%-

# %%-- plot with parameters.
data1.data_parameter_plot_multimodule(x_name='AT', y_name='MT')
# %%-
