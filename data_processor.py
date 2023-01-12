# %%-- todo: make the interpolate function work for non-resampled data.
'''
Program structure:
1. Remove zero: this step plot all non-zero data, but the empty one will still be zero.
2. Module selector: this step select the module we need.
3. Resampler: this step resample the data by different options.
4. Outlier removal: remove outlier inside the data.
5. Plot the data.
6. Interpolate only works if you have resampling, otherwise the data will have no defined period (sometimes 8s and sometimes 9s)
7. Interpolate will not work if the resampling period is month.

to do: add irrdiance as a 3rd dimension
'''

# %%-- Import & get the data ready:
import pyodbc
from sqlalchemy import create_engine
import csv
from data_processor_object import *
import matplotlib.pyplot as plt
# Define the object:
# the code will include both boundary for starting and ending date.
starting_day = '2022_1_1' 
ending_day = '2022_9_3'
starting_time = '8:00:00 AM'
ending_time = '9:00:32 PM'
data1 = module_data_processor(path = r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2022\2022-05-31_2022-07-01.accdb', starting_day = starting_day, ending_day = ending_day, starting_time = starting_time, ending_time = ending_time)

# create a list of list for all dates to select paths.
data1.file_date_reader()

# zero removing: this part remove the zero data, but not the empty ones.
data1.zero_remover(removezero=True)


#%%  Select module and resampling period.

# select the module to plot: range from 1 to 6
data1.module_selector(module_num_list=[2])

# resample the module: options are 'hour', 'day', 'month', put anything else it will not resample.
data1.multi_module_resampler(sample_length='day', quantile_v=0.9)

# %% Operation for T and irradiance.
# apply parameter selection, the meaning are:
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
print(data1.module_df_sampled[0]['MT'].median())
print(data1.module_df_sampled[0]['IR_BEV'].median())
# data1.bin_selector(param_name='MT', centre_value=45, rangevalue=5)
# data1.bin_selector(param_name='IR_BEV', centre_value=800, rangevalue=100)
data1.bin_selector(param_name='MT', centre_value=35, rangevalue=5)
data1.bin_selector(param_name='IR_BEV', centre_value=678, rangevalue=100)

# apply temperature correction: Voc Isc and Pmpp FF
# data1.temperature_correction()

# apply irradiance correction: Voc Isc and Pmpp FF
# data1.irradiance_correction()

# remove zeros at the end:
data1.zero_removal2()

# remove outlier, define the outlier range here, it not defined, the default value is 10.
data1.iqr_width = 1.5

# plot the histogram
data1.hist_visual(bins=100, param='MT')
data1.hist_visual(bins=100, param='IR_BEV')

# %% plot the data: the interpolation only works if you do the resampling to mininmum of hour, otherwise there will be too many interpolation points
data1.data_ploter_with_time_multimodule(target_name='Isc', linear_fit=True, color_code=False, interpol=False)
# data1.data_ploter_with_time_multimodule(target_name='IR_BEV', linear_fit=True)
# data1.data_ploter_with_time_multimodule(target_name='MT', linear_fit=True)
data1.data_ploter_with_time_multimodule(target_name='Voc', linear_fit=True, color_code=False, color_name='AT', interpol=False)
data1.data_ploter_with_time_multimodule(target_name='Pm', linear_fit=True)

# data1.data_parameter_plot_multimodule(x_name='MT', y_name='Isc', linear_fit=True, color_code=True)
# data1.data_parameter_plot_multimodule(x_name='MT', y_name='Voc', linear_fit=True, color_code=True)
# data1.data_parameter_plot_multimodule(x_name='MT', y_name='Pm', linear_fit=True)
# data1.data_parameter_plot_multimodule(x_name='MT', y_name='IR_BEV', linear_fit=True)
# data1.data_parameter_plot_multimodule(x_name='IR_BEV', y_name='Isc', linear_fit=False)
