# %%-- Cell 0.
'''
Program structure:
1. Remove zero: this step plot all non-zero data, but the empty one will still be zero.
2. Module selector: this step select the module we need.
3. Resampler: this step resample the data by different periods and quantile value.
4. Outlier removal: remove outlier inside the data.
5. Data selection: select the data based on 
5. Interpolation (optional)
6. Linear fit (optional)
7. Color code the 3rd dimension (optional)
'''

# %% Cell 1: Imports
import pyodbc
from sqlalchemy import create_engine
import csv
from data_processor_object import *
import matplotlib.pyplot as plt

# %% Cell 2: The input cell
# the code will include both boundary for starting and ending date.
starting_day = '2022_1_2' 
ending_day = '2022_1_3'
starting_time = '8:00:00 AM'
ending_time = '9:00:32 PM'

# paths is a list of paths for the related access database
paths = [
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\22-01-25_22-02-28.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\22-03-01_22-03-31.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\22-04-01_22-05-02.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\22-05-02_22-05-31.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\22-05-31_22-07-01.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\22-07-01_22-07-31.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\22-08-01-22_09-01.accdb', # the above are 2022 data.
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2021\SP1053 (2021.8.10-2021.9.15).accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2021\SP1053 (2021.11.2 - 2022.1.3).accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2021\SP1053(2021.4.1-2021.5.4).accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2021\SP1053(2021.5.4-2021.6.16).accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2021\SP1053(2021.6.16-2021.8.9).accdb', # the above are 2021 data.
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\02 June 2020 - 06 July 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\03 Sep 2020 - 07 Oct 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\04 March 2020 - 05 Apr 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\05 May 2020 - 02 June 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\06 Apr 2020 - 05 May 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\06 July 2020 - 20 July 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\06 Nov 2020 - 26 Nov 2020\SP1053.accdb',
        # r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\07 Oct 2020 - 19 Oct 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\17 Aug 2020 - 03 Sep 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\19 Dec 2020 - 28 Jan 2021\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\20 Oct 2020 - 05 Nov 2020\SP1053.accdb',
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\27 Nov 2020 - 18 Dec 2020\SP1053.accdb',
        r"C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\09-01-2022_10-04-2022.accdb",
        r"C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\10-04-2022_10-26-2022.accdb",
        r"C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\10-26-2022_12-02-2022.accdb",
        r"C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\12-02-2022_01-05-2023.accdb",
        ]

# module_number_list is a list of module we want to investigates, there are 6 modules and the correponding numbers are from 1 to 6
module_number_list = [1, 2]

# sample_length is a string representing the period we want to sample
# the options are: 'hour', 'day', 'month'
sample_length = 'hour'

# percentile is a number ranging from 0 to 100, selecting the top xx percent of electrical data
percentile = 90

# T_central is a float representing the centre value of the temperature 
# we want to represent, the selected data should be within the range of 
# T_central +- dT
# the unit is degree C
T_central=45
dT = 100

# Ir_central is a float representing the centre value of the temperature 
# we want to represent, the selected data should be within the range of 
# T_central +- dIr
# the unit is W/m2
Ir_central = 800
dIr = 1000

# %% Cell 3: define the object
data1 = module_data_processor(path = paths, starting_day=starting_day, ending_day=ending_day, starting_time=starting_time, ending_time=ending_time)

# %% Cell 4: Run this cell to create the pd data frame based on database files data

# create a list of list for all dates to select paths.
data1.file_date_reader()

# zero removing: this part remove the zero data, but not the empty ones.
data1.zero_remover(removezero=True)

# %% Cell 5: Select module and resampling period.

# select the module to plot: range from 1 to 6
data1.module_selector(module_num_list=module_number_list)

# resample the module: options are 'hour', 'day', 'month', put anything else it will not resample.
data1.multi_module_resampler(sample_length=sample_length, quantile_v=percentile/100)

# % Operation for T and irradiance selection

# apply parameter selection, the meaning for each symbols are are:
'''
self.column_name_dict = {'AH':'Absolute humidity %',
'AT':'Ambient temperature (\u00B0C)',
'MT':'Module temperature (\u00B0C)',
'IR_BEV':'Irradiance (W/m2)'
'Voc': 'Voc(V)',
'Isc':'Isc (A)',
'Vm':'Maximum power voltage (V)',
'Im': 'Maximum power current (A)',
'Pm': 'Maximum power',
'FF':'Fill factor (%)'}
'''
# print out the medium and std value for T and irr to help us select T and Irr range
print(data1.module_df_sampled[0]['MT'].median())
print(data1.module_df_sampled[0]['IR_BEV'].median())

# Select Temperature and Irradiance range
data1.bin_selector(param_name='MT', centre_value=T_central, rangevalue=dT)
data1.bin_selector(param_name='IR_BEV', centre_value=Ir_central, rangevalue=dIr)

# apply temperature correction: Voc Isc and Pmpp, FF
# data1.temperature_correction()

# apply irradiance correction: Voc Isc and Pmpp FF
# data1.irradiance_correction()

# remove zeros again at the end:
data1.zero_removal2()

# remove outlier, define the outlier range here, if not defined, the default value is 10.
data1.iqr_width = 1.5

# plot the histogram of T and irradiance (data visualization)
# data1.hist_visual(bins=100, param='MT')
# data1.hist_visual(bins=100, param='IR_BEV')
print('The std of Isc is ' + str(data1.module_df_sampled[0]['Isc'].std()))
print('The mean of Isc is ' + str(data1.module_df_sampled[0]['Isc'].mean()))
print('The std of Voc is ' + str(data1.module_df_sampled[0]['Voc'].std()))
print('The mean of Voc is ' + str(data1.module_df_sampled[0]['Voc'].mean()))
print('The std of Pm is ' + str(data1.module_df_sampled[0]['Pm'].std()))
print('The mean of Pm is ' + str(data1.module_df_sampled[0]['Pm'].mean()))
print('The std of eff is ' + str(data1.module_df_sampled[0]['eff'].std()))
print('The mean of eff is ' + str(data1.module_df_sampled[0]['eff'].mean()))
print('The std of FF is ' + str(data1.module_df_sampled[0]['FF'].std()))
print('The mean of FF is ' + str(data1.module_df_sampled[0]['FF'].mean()))
# %% Cell 6: plot the data:

# plot the parameter with time
data1.data_ploter_with_time_multimodule(target_name='Isc', linear_fit=False, color_code=False, color_name='IR_BEV', interpol=True)
# data1.data_ploter_with_time_multimodule(target_name='IR_BEV', linear_fit=True)
# data1.data_ploter_with_time_multimodule(target_name='MT', linear_fit=True)
# data1.data_ploter_with_time_multimodule(target_name='Voc', linear_fit=True, color_code=True, color_name='IR_BEV', interpol=False)
# data1.data_ploter_with_time_multimodule(target_name='Pm', linear_fit=True, color_code=True, color_name='IR_BEV', interpol=False)
# data1.data_ploter_with_time_multimodule(target_name='FF', linear_fit=True, color_code=True, color_name='IR_BEV', interpol=False)
# data1.data_ploter_with_time_multimodule(target_name='eff', linear_fit=True, color_code=True, color_name='IR_BEV', interpol=False) # the efficinecy is no known because we don't know hte module area

# plot the parameter with another parameter
# data1.data_parameter_plot_multimodule(x_name='MT', y_name='Isc', linear_fit=True, color_code=True)
# data1.data_parameter_plot_multimodule(x_name='MT', y_name='Voc', linear_fit=True, color_code=True)
# data1.data_parameter_plot_multimodule(x_name='MT', y_name='Pm', linear_fit=True)
# data1.data_parameter_plot_multimodule(x_name='MT', y_name='IR_BEV', linear_fit=True)
# data1.data_parameter_plot_multimodule(x_name='IR_BEV', y_name='Isc', linear_fit=False)