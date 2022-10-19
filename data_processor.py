# %%-- todo:
'''
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
starting_day = '2022_2_15'
ending_day = '2022_8_15'
starting_time = '8:00:00 AM'
ending_time = '9:00:32 PM'
data1 = module_data_processor(path = r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\2022-05-31_2022-07-01.accdb', starting_day = starting_day, ending_day = ending_day, starting_time = starting_time, ending_time = ending_time)
# %%-


# %%-- create a list of list of dates in object.
# create a list of dates
data1.file_date_reader()
# data1.list_of_date # this is a list of list of dates for each file for each day.
# %%-

# data1.file_path_locator('2022_3_11IV')


# %%--
data1.zero_remover()
data1.df_nonzero
data1.module_selector(module_num_list=[4, 5]) # module range from 1 to 6.
# data1.df_nonzero
# data1.module_df_list[1]
# %%-


# %%-- resample for each module:
data1.multi_module_resampler(sample_length='month')
# data1.module_df_sampled[0]
# data1.module_df_sampled[1]
# %%-


# %%--plot with time:
data1.data_ploter_with_time_multimodule(target_name='Voc')
# %%-

# %%-- plot with parameters.
data1.data_parameter_plot_multimodule(x_name='AH', y_name='Isc')
# %%-
