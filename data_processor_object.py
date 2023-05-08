# %%-- import the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pyodbc
# from sqlalchemy import create_engine
import datetime
import matplotlib.dates as mdates
import pickle
import os
import glob
from PIL import Image
import matplotlib.image as mpimg
import cv2
# from os import walk
# %%-


class module_data_processor:
    """
    This is an object that reads and process the module data from an access databse file.
    """

    def __init__(self, path, starting_day, ending_day, starting_time, ending_time, iqr_width=1.5, resample_name='IR_BEV', module_Area = 2.578716):
        """
        1. Write the path, starting date and ending date into the object.
        2. Input:
            pathlist: a list of databse access path we want to load.
            starting date: the first day to start.
            ending date: the end of the time to look at.
            starting_time: a string representing the hourly time of the first day to start including data
            ending_timme: the tie of the last day to stop including data
            iqr_width: the number of iqr to include as "not outlier"
            resample_name: a string showing which colume to resample on when resampling
            module_Area: a number corresponding to the area of each module.
        """
        # data for 2022.
        self.path = path


        # self.path = path
        self.starting_day = starting_day
        self.ending_day = ending_day
        self.starting_time = starting_time
        self.ending_time = ending_time

        # convert the input to datetime.
        year_st, month_st, day_st = starting_day.split('_')
        year_ed, month_ed, day_ed = ending_day.split('_')
        hour_st, min_st, sec_st = starting_time.split(':')
        sec_st, AMPM_st = sec_st.split(' ')
        # convert the hour if it is PM:
        hour_st = str(int(hour_st) + 12 * int(AMPM_st[0] == 'P'))
        hour_ed, min_ed, sec_ed = ending_time.split(':')
        sec_ed, AMPM_ed = sec_ed.split(' ')
        hour_ed = str(int(hour_ed) + 12 * int(AMPM_ed[0] == 'P'))
        # starting_datetime = starting_day + ' ' + starting_time
        # ending_datetime = ending_day + ' ' + ending_time

        # generate the datetime string back:
        starting_datetime = year_st + '_' + month_st + '_' + day_st + ' ' + hour_st + ':' + min_st + ':' + sec_st
        ending_datetime = year_ed + '_' + month_ed + '_' + day_ed + ' ' + hour_ed + ':' + min_ed + ':' + sec_ed

        self.starting_datetime = datetime.datetime.strptime(starting_datetime, "%Y_%m_%d %H:%M:%S")
        self.ending_datetime = datetime.datetime.strptime(ending_datetime, "%Y_%m_%d %H:%M:%S")
        # # print(self.starting_datetime)
        # # print(self.ending_datetime)
        self.years_available = range(int(year_st), int(year_ed) + 1)
        # # print(list(self.years_available))

        # define a dictionary that tranlate the column name from raw data to more understandable names:
        self.column_name_dict = {'AH':'Absolute humidity %',
        'AT':'Ambient temperature (\u00B0C)',
        'MT':'Module temperature (\u00B0C)',
        'IR_BEV':'Irradiance (W/m2)',
        'Voc': 'Voc(V)',
        'Isc':'Isc (A)',
        'Vm':'Maximum power voltage (V)',
        'Im': 'Maximum power current (A)',
        'Pm': 'Maximum power (W)',
        'FF':'Fill factor (%)',
        'eff': 'Efficiency (%)'}

        self.param_name_dict = {
            'Absolute humidity %': 'AH',
            'Ambient temperature (C)': 'AT',
            'Module temperature (C)': 'MT',
            'Irradiance (W/m2)': 'IR_BEV',
            'Voc(V)': 'Voc',
            'Isc(A)': 'Isc',
            'Maximum power voltage (V)': 'Vm',
            'Maximum power current (A)': 'Im',
            'Maximum power (W)': 'Pm',
            'Fill factor (%)': 'FF',
            'Efficiency (%)': 'eff'
        }

        # define a list of colour correspond to each module: blue, green red, magenta, and yellow.
        self.colour_list = ['b', 'g', 'r', 'c', 'm', 'y']

        # define the whisker width for outlier removeal
        self.iqr_width = iqr_width

        # define the resampling parameter name: default is irradiance
        self.resample_name = resample_name

        # define the module area
        self.module_area = module_Area

        # set the default: we don't fill the night data with zero
        self.interpolate_night = False


    def table_name_reader(self, path):
        """
        Input:
        path: a string which is the paths of the access file.

        output: a list of string which contains the IV tables of each day
        """

        # read the path from the object:
        # path = self.path

        # create the connection
        # msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + str(path) + ';'
        conn = pyodbc.connect(con_string)

        # Create the cursor object to read how many table we got:
        cur = conn.cursor()
        IV_table_names = []
        for row in cur.tables():
            # if the table name ends with "IV" and start with a digit, collect it into the list.
            if row.table_name[-2:] == 'IV' and row.table_name[0].isdigit():
                # # print(row.table_name)
                IV_table_names.append(row.table_name)
        cur.close()

        # output:
        return IV_table_names


    def file_date_reader(self):
        '''
        input: the files paths directory from the object.
        output: a list of string of all dates corresponding to each file. (a list of list)
        '''
        files_date = []
        for path in self.path:
            # read the dates:
            dates = self.table_name_reader(path=path)
            files_date.append(dates)
        # store the result into the object.
        self.list_of_date = files_date


    def data_reader_day(self, date):
        """
        input:
        date: a string that correspond the day we want to look at the data.

        output:
        date_data: a panda dataframe of IV data of that table.
        """

        # read the path from the object:
        # path = self.path
        path = self.path_selector(date)

        # create the connection
        msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + str(path) + ';'
        conn = pyodbc.connect(con_string)

        # define the query:
        sql_query = str(date) # + 'IV'
        df = pd.read_sql('SELECT * FROM ' + str(date), conn)


        return df


    def date_selector(self):
        """
        input: a period of time that we are interested in.
        starting_date: year_month_date string.
        ending_date: year_month_date string.

        output: a panda dataframe of IV data of the selected date.
        """
        starting_date = self.starting_day
        ending_date = self.ending_day
        # convert the string into datetime format:
        starting_year, starting_month, starting_day = starting_date.split('_')
        starting_date = datetime.datetime(int(starting_year), int(starting_month), int(starting_day))
        # # print(starting_date)
        ending_year, ending_month, ending_day = ending_date.split('_')
        ending_date = datetime.datetime(int(ending_year), int(ending_month), int(ending_day))
        # # print(ending_date)

        # find all the date that are between these two:
        dates_between = np.array(pd.date_range(starting_date, ending_date, freq='D'))
        dates_between = np.sort(dates_between)
        # sort the dates in time order:

        # # print(dates_between)

        date_list = [] # a list of date to include, each date correspond to one table.
        for date in dates_between:
            # convert from numpy.datetime64 to string:
            date = str(date)
            # # print(date)
            # delete the time part:
            date = date.split('T')[0]
            # # print(date)
            # collect the year, month and date:
            year, month, day = date.split('-')
            # we cannot have month start with 0, for example, May should be 5 not 05
            if month[0] == '0':
                month = month[1:]
            # we cannot have day start with 0 as well.
            if day[0] == '0':
                day = day[1:]
            date = year + '_' + month + '_' + day
            # print(date)
            date = date + 'IV'
            # if date == '2021_12_10IV':
            #     continue
            date_list.append(date)

        # # print(date_list)
        # filter out the dates that are not contained in the files.
        # load the available dates from the object.
        available_dates = np.concatenate(self.list_of_date).flat
        # convert into numpy array to avoid using for loop:
        date_list = np.array(date_list)
        # check each element:
        availability = np.isin(date_list, available_dates)
        # # print(availability)
        # # print(date_list)
        # # print(available_dates)
        index = np.argwhere(availability)
        # keep the available dates only:
        date_list = date_list[index].flatten()
        # convert back to list:
        date_list = date_list.tolist()
        # # print(date_list)

        # now extract the IV data for the given table and concat into a single dataframe:
        # start with the first date in the list, you will need to select the correct path, the path that contains the table name str(date) + 'IV'
        df2 = pd.DataFrame()
        # # print(df2['datetime'])
        # use a for loop to concanate the rest of hte data:
        for date in date_list[0:]:
            # read the df
            print(date)
            df = self.data_reader_day(date)
            if df.empty:
                continue
            # # print(df)
            

            # sort the df by time.
            # remove all empty rows: it must start with a number.
            df = df.dropna()

            # # print(df)
            # # print(df['xts'].str.endswith(('M', 'A', 'P'))
            # the 2020 data uses 24 hour format instead of am pm.
            df['xday'] = df['xday'].astype(str).str.strip()
            if df['xts'].str.endswith(('M', 'A', 'P')).all():
                # # print('AM or PM detected')
                # some date that use A instead of AM, so lets add m at the end if anything not ending with an m:
                # remove all M:
                df['xts'] = df['xts'].str.replace("M", "")
                # add the M back: no matter wheter we had M before, this will results in everything having one M.
                df['xts'] = df['xts'].astype(str) + 'M'
                # we also need to have zero padding for the hour to match the %I.
                # find the ones that have only one digit for hour, we expect the xts column second element to be :
                df['xts'] = df['xts'].str.zfill(11)
                # # print(df['xts'])
                # # print(df['xday'])
                # convert the 00 to 12 to match with %I.
                # df['xts'].astype(str)
                df = df[df['xts']!='05:04:б9 AM']
                # # print(df['xts'])
                df['xts_datetime'] = pd.to_datetime(df['xts'].astype(str), format='%H:%M:%S %p') # this column is an intermedium column, the pm and am are not converted correctly, but it will be corrected in later lines.
                # # print(df['xts_datetime'])
                # # print(df['xday'])
                df['xday'] = pd.to_datetime(df['xday'].astype(str).str.strip(), format='%d/%m/%Y')
                # # print(df['xday'])
                # # print(df['xts'])
                # # print(df)
                # combine the datetime column:
                df['year'] = pd.DatetimeIndex(df['xday']).year
                df['month'] = pd.DatetimeIndex(df['xday']).month
                df['day'] = pd.DatetimeIndex(df['xday']).day
                df['hour'] = pd.DatetimeIndex(df['xts_datetime']).hour
                df['miutes'] = pd.DatetimeIndex(df['xts_datetime']).minute
                df['second'] = pd.DatetimeIndex(df['xts_datetime']).second
                df[['time', 'PM']] = df['xts'].str.split(' ', expand=True)
                # # print(df['PM'])
                df['hour'] = df['hour'] + 12*(df['PM'] == 'PM')
                df=df[df['hour']!=24]
                df['datetime'] = pd.to_datetime(df.year.astype(str) + ' ' + df.month.astype(str) + ' ' + df.day.astype(str) + ' ' + df.hour.astype(str) + ':' + df.miutes.astype(str) + ':' + df.second.astype(str), format = "%Y %m %d %H:%M:%S")
                # # print(df['datetime'])
                # delete intermedium column to produce the datetime column.
                df = df.drop(['xts_datetime', 'year', 'month', 'day', 'hour', 'miutes', 'second', 'time', ], axis=1)
                # # print(df)
                # sort the df by datetime column.
                df = df.sort_values(by='datetime')
                # # print(df)
                # filter out the data through datetime column:
                df = df[np.array(df['datetime']>self.starting_datetime) * np.array(df['datetime']<self.ending_datetime)]
                # # print(df)
                # store df into the object.
                # self.df_days = df

            else:
                # # print('no AM or PM detected.')
                # # print(df['xts'])
                # # print(df['xday'])
                df['xts_datetime'] = pd.to_datetime(df['xts'].astype(str), format='%H:%M:%S ')
                # find the ones that have only one digit for hour, we expect the xts column second element to be :
                # df['xday'] = df['xday'].str.zfill(11)
                df['xday'] = pd.to_datetime(df['xday'].astype(str), format='%Y/%m/%d')
                # combine the datetime column:
                df['year'] = pd.DatetimeIndex(df['xday']).year
                df['month'] = pd.DatetimeIndex(df['xday']).month
                df['day'] = pd.DatetimeIndex(df['xday']).day
                df['hour'] = pd.DatetimeIndex(df['xts_datetime']).hour
                df['miutes'] = pd.DatetimeIndex(df['xts_datetime']).minute
                df['second'] = pd.DatetimeIndex(df['xts_datetime']).second
                df['datetime'] = pd.to_datetime(df.year.astype(str) + ' ' + df.month.astype(str) + ' ' + df.day.astype(str) + ' ' + df.hour.astype(str) + ':' + df.miutes.astype(str) + ':' + df.second.astype(str), format = "%Y %m %d %H:%M:%S")
                # delete intermedium column to produce the datetime column.
                df = df.drop(['xts_datetime', 'year', 'month', 'day', 'hour', 'miutes', 'second'], axis=1)

                # sort the df by datetime column.
                df = df.sort_values(by='datetime')

                # filter out the data through datetime column:
                df = df[np.array(df['datetime']>self.starting_datetime) * np.array(df['datetime']<self.ending_datetime)]

            # concanate with original one:
            df2 = pd.concat([df2, df], axis=0)
        # print(df2['datetime'])

        # store df into the object.
        self.df_days = df2



        return df2


    def file_path_locator(self, date):
        '''
        input: a string of date.
        output: the path of the file that contains this day
        '''
        # add the name IV after, so it matches with the table name.
        date = str(date) + 'IV'
        # look through the list of dates for each file:
        counter = 0
        for files in self.list_of_date:
            # check whether it contains this day.
            # print(files)
            if date in files:
                # if the file have this date, break the loop
                # # print('true')
                break
            else:
                # otherwise update the counter
                counter = counter + 1
        # now we should get the counter that is the index of the file.
        # # print(counter)
        path = self.path[counter]
        # output the result:
        # print(path)
        return path


    def data_extractor(self, removezero=True, store_df=False):
        """
        This function takes the df_days and remove the zero outliers
        removezero: a boolean input, if true, remove the zeros, otherwise just read the data.
        store_df: a boolean input, if true, it will store the generated df as a pickle file
        """
        # run the code to extract the dates.
        df = self.date_selector()
        # # print(df)

        # use a new column to identify whether to delete it by multiplying everything together
        product = df['Voc'] * df['Isc'] * df['Vm'] * df['Im'] * df['Pm'] * df['FF']
        df['nonzero'] = (product != 0)
        # print(df)

        # compute the efficiency
        df['eff'] = df['Pm']/df['IR_BEV']/self.module_area*100

        # filter out the zero data.
        if removezero == True:
            df_nonzero = df[df['nonzero']==True]
        else:
            df_nonzero = df
        # # delete the extra label column.
        # df_nonzero = df_nonzero.drop('whether_keep')
        # store the data in the object
        # # print(df_nonzero)
        self.df_nonzero = df_nonzero
        if store_df == True:
            # the name of the file contains: start and ending datetime
            file_name = str(self.starting_day) + ' ' + str(self.ending_day)
            # delete the previous pickle file
            current_dir = os.getcwd()
            for files in os.listdir(current_dir):
                if files.endswith('.pkl'):
                    file_path = os.path.join(current_dir, files)
                    os.remove(file_path)
            # store the df_nonzero as a pickle file
            with open(str(file_name) + '.pkl', 'wb') as f:
                pickle.dump(df_nonzero, f)
                

    def module_selector(self, module_num_list = [1, 2, 3]):
        """
        This process is after the step of zero remover:
        input: it takes the pd dataframe after removal of zeros.
        output: filter the dataset to have only a specific module.
        """
        # read the df from object.
        df_nonzero = self.df_nonzero
        pd_list = []
        # apply the filtering:
        keep_data_list = np.zeros(np.shape(df_nonzero['cno'] == module_num_list[0]))
        for number in module_num_list:
            keep_data = np.array(df_nonzero['cno'] == number)
            keep_data_list = np.any([keep_data_list, keep_data], axis=0)
            pd_list.append(df_nonzero[keep_data])

        df_nonzero_module = df_nonzero[keep_data_list]
        # store the filtered data into the object:
        self.df_nonzero_module = df_nonzero_module

        # split the df_nonzero into a list of pd dataframe for each module:
        self.module_df_list = pd_list

        # store the module selection into the object.
        self.module_num_list = module_num_list


    def data_ploter_with_time_multimodule(self, target_name, linear_fit=False, color_code=False, color_name='IR_BEV', interpol=False, transparency=0.5, export_figure=False, module_title=False):
        '''
        This function will plot the parmeter with time but plot multiple module value on the same graph.
        input:
        1. target_name: the name of hte parameter to plot, which is correspond to the colume name given in access file
        2. linear_fit: a boolean input, if true, it will plot a linear fit and display the gradient.
        3. color_code: a boolean input, if true, the scatter plot will be color coded based on the parameter given by color_name (the color code feature is only usable when there is only one module)
        4. interpol: a boolean input, if true, the scatter plot will interpolate the empty values with lienar fit.
        '''
        # the interpolation does not work without resampling, so check whether resampline period is alid first
        if (self.resample_T != 'hour') and (self.resample_T != 'day'):
            interpol = False

        fig, ax = plt.subplots()
        counter = 0
        # define a color for each module
        color_list = ['blue', 'green', 'red', 'cyan', 'magenta', 'yellow', 'black']
        linelist = [] # a list to collect all the plot the will be included in the legend
        for module in self.module_df_sampled:
            # the counter counts for each module
            counter = counter + 1
            # select the pd frame for this module:
            pd_module = module
            # remove the outliers
            pd_module = self.subset_by_iqr(df=pd_module, column=target_name)
            # try to split the pd module by year:
            # # print(pd_module['datetime'])
            for year in self.years_available:
                y = pd_module[pd_module['datetime'].dt.year == year][target_name]
                x = pd_module[pd_module['datetime'].dt.year == year]['datetime']
                # select the x and y column names:
                # y = pd_module[target_name]
                # x = pd_module['datetime']
                line1 = ax.scatter(x, y, label='Module ' + str(self.module_num_list[counter-1]) + ' (' + str(year) + ')', s=10, color=color_list[counter-1], alpha=transparency)
                linelist.append(line1)
                # for plotting
                # if the time ranges for larger than a month, don't include time in x axis
                # if the time ranges for smaller than 3 days, don't include date in x axis
                if (self.ending_datetime - self.starting_datetime)<datetime.timedelta(days=3):
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                # elif (self.ending_datetime - self.starting_datetime)>datetime.timedelta(days=31):
                #     ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y %H:%M'))
                else:
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
                # ax.xaxis.set_tick_params(rotation=90)

                if color_code == True:
                    # add the colour code as a third colume:
                    plt.scatter(x, y, label='Module ' + str(self.module_num_list[counter-1]) + ' (' + str(year) + ')', c=pd_module[pd_module['datetime'].dt.year == year][color_name], s=10, cmap='jet', alpha=transparency)
                    plt.colorbar(label=self.column_name_dict[color_name])

                if interpol == True:
                    if self.resample_T == 'hour' or 'day' or 'month':
                        x_interpolate, y_interpolate, interpolate_ind = self.interpolator(x, y)

                        # make sure the y values are nonzero
                        x_interpolate = np.array(x_interpolate)
                        y_interpolate = np.array(y_interpolate)
                        
                        # plot the interpolated values.
                        plt.scatter(x_interpolate, y_interpolate, s=10, edgecolors=color_list[counter-1], facecolors='none')


                # add the linear plot: both the line and the equation.
                # x_fit = []
                # for dx in x:
                    # x_fit.append(datetime.datetime.fromtimestamp(dx) - datetime.date(self.years_available[0],1,1))
                if linear_fit == True:
                    # convert the datetime into a number
                    x2 = []
                    for dx in x:
                        total = int(dx.strftime('%S'))
                        total += (int(dx.strftime('%j')) - 1)
                        total += (int(dx.strftime('%Y')) - 2020) * 365
                        x2.append(total)

                    # make the x axis being zero during the start of the year
                    x2 = np.array(x2)
                    x2 = x2 - x2[0]
                    coef = np.polyfit(x2,y,1)
                    poly1d_fn = np.poly1d(coef)
                    xplot = x
                    yplot = poly1d_fn(x2)
                    # # print([xplot.iloc[0], xplot.iloc[-1]])
                    # # print([yplot[0], yplot[-1]])
                    xplot = [xplot.iloc[0], xplot.iloc[-1]]
                    yplot = [yplot[0], yplot[-1]]
                    # xplot = np.arange(np.array(x)[0], np.array(x)[-1])
                    # yplot = np.arange(np.array(poly1d_fn(x2))[0], np.array(poly1d_fn(x2))[-1])
                    plt.plot(xplot, yplot, '--k')
                    # print('The slope is ' + str(round(coef[0], 4)) + '/day' + ' for module ' + str(self.module_num_list[counter-1]))
                    # add the equation onto the plot
                    plt.text(0.1, 0.9, 'y = ' + str(round(coef[0], 4)) + ' x' + ' + ' + str(round(coef[1], 2)), fontsize=12, transform=plt.gca().transAxes, bbox=dict(facecolor='lightgray', edgecolor='black', pad=5))
                    # plt.title('y = ' + str(round(coef[0], 4)) + ' * days' + ' + ' + str(round(coef[1], 2)), fontsize=12)
        
        if interpol == True:
            plt.scatter(x_interpolate[0], y_interpolate[0], s=10, color='black', facecolors='none', label='Interpolated data')
        # look up the name from dictionary:
        target_name = self.column_name_dict[target_name]
        plt.xlabel('Time')
        plt.ylabel(target_name)
        # # print('the above works')
        if module_title:
            plt.title('Module ' + str(self.module_num_list))
        else:
            plt.title(' Between '+  str(self.starting_datetime) + ' and ' + str(self.ending_datetime))
        plt.gcf().autofmt_xdate()
        if color_code == False:
            plt.legend(loc='lower left')
        # export the figure if asked
        if export_figure:
            plt.savefig(str('Module ' + str(self.module_num_list)) + '.png')
        plt.show()


    def data_parameter_plot_multimodule(self, x_name, y_name, linear_fit=False, color_code=False, color_name='IR_BEV', module_title=False, transparency=0.5, export_figure=False):
        '''
        This function will plot the parmeter with parameter but plot multiple module value on the same graph.

        1. target_name: the name of hte parameter to plot, which is correspond to the colume name given in access file
        2. linear_fit: a boolean input, if true, it will plot a linear fit and display the gradient.
        3. color_code: a boolean input, it true, the scatter plot will be color coded based on the parameter given by color_name (the color code feature is only usable when there is only one module)
        4. export_figure: a boolean input, if true, it will export the figure.
        5. transparency: a number input, ranging from 0 to 1 (0 means totally transparent)
        '''
        plt.figure(facecolor='white')
        counter = 0
        for module in self.module_df_sampled:
            counter = counter + 1
            # select the pd frame for this module:
            pd_module = module
            # remove outliers
            pd_module = self.subset_by_iqr(df=pd_module, column=x_name)
            pd_module = self.subset_by_iqr(df=pd_module, column=y_name)
            for year in self.years_available:
                y = pd_module[pd_module['datetime'].dt.year == year][y_name]
                x = pd_module[pd_module['datetime'].dt.year == year][x_name]
                plt.scatter(x, y, label='Module ' + str(self.module_num_list[counter-1]) + ' (' + str(year) + ')', s=10, alpha=transparency)
                # add the color code feature for the 3rd dimension
                if color_code == True:
                    # add the colour code as a third colume:
                    plt.scatter(x, y, label='Module ' + str(self.module_num_list[counter-1]) + ' (' + str(year) + ')', c=pd_module[pd_module['datetime'].dt.year == year][color_name], s=10, cmap='jet', alpha=transparency)
                    plt.colorbar(label=self.column_name_dict[color_name])
                if linear_fit == True:
                    coef = np.polyfit(x,y,1)
                    poly1d_fn = np.poly1d(coef)
                    xplot = x
                    # yplot = poly1d_fn(x)
                    # only choose two points of line
                    xplot = [xplot.min(), xplot.max()]
                    yplot = poly1d_fn(xplot)
                    plt.plot(xplot, yplot, '--k')
                    # print('The slope is ' + str(round(coef[0], 4)) + ' for module ' + str(self.module_num_list[counter-1]))
                    plt.text(0.1, 0.9, 'y = ' + str(round(coef[0], 4)) + ' x' + ' + ' + str(round(coef[1], 2)), fontsize=12, transform=plt.gca().transAxes, bbox=dict(facecolor='lightgray', edgecolor='black', pad=5))
            # # select the x and y column names:
            # x = pd_module[x_name]
            # # select the y axis data:
            # y = pd_module[y_name]
            # plt.scatter(x, y, label='Module ' + str(self.module_num_list[counter-1]), s=10, c=self.colour_list[counter - 1])
        # look up the name from dictionary:
        x_name = self.column_name_dict[x_name]
        y_name = self.column_name_dict[y_name]
        plt.xlabel(x_name)
        plt.ylabel(y_name)
        if module_title:
            plt.title('Module ' + str(self.module_num_list))
        else:
            plt.title(' Between '+  str(self.starting_datetime) + ' and ' + str(self.ending_datetime))
        plt.gcf().autofmt_xdate()
        if color_code == False:
            plt.legend(loc='lower left')
        # export the figure if asked
        if export_figure:
            plt.savefig(str('Module ' + str(self.module_num_list)) + '.png')
        plt.show()


    def data_resampler(self, df, sample_length='hour', select=0.5, param_name='Isc'):
        '''
        input: This function takes the pd dataframe after removing all zeros, 
        output: then resample the data based on the requirement: minute, hour, day, month.
        '''
        df_nonzero=df
        if sample_length == 'hour':
            # we will resample the data based on the hour colume:
            # # print(df_nonzero)
            # convert the datetime column to hour:
            # df_datetime = df_nonzero['datetime']
            
            # compute the quantile value for selected parameter
            quantile_values = df_nonzero.resample('60min', on='datetime').quantile(q=select, interpolation='nearest')[self.resample_name]
            # define the headings
            quantile_values = pd.DataFrame(quantile_values, columns=[str(self.resample_name)])

            # select the rows where the selected column is equal to the quantile
            df_nonzero = df_nonzero.loc[np.in1d(np.array(df_nonzero[self.resample_name]), np.array(quantile_values[self.resample_name]))]

            # resample again to ensure the frequency
            df_nonzero = df_nonzero.resample('H', on='datetime').first()

            # df_nonzero = df_nonzero.resample('60min', on='datetime').quantile(q=select)
            df_nonzero['datetime'] = df_nonzero.index
            df_nonzero = df_nonzero.fillna(0)

        elif sample_length == 'day':
            # # compute the quantile value for selected parameter
            # quantile_values = df_nonzero.resample('D', on='datetime').quantile(q=select, interpolation='nearest')[self.resample_name]
            # # define the headings
            # quantile_values = pd.DataFrame(quantile_values, columns=[str(self.resample_name)])

            # # select the rows where the selected column is equal to the quantile
            # df_nonzero = df_nonzero.loc[np.in1d(np.array(df_nonzero[self.resample_name]), np.array(quantile_values[self.resample_name]))]

            # # resample again to ensure the frequency
            # df_nonzero = df_nonzero.resample('D', on='datetime').first()

            # # df_nonzero = df_nonzero.resample('60min', on='datetime').quantile(q=select)
            # df_nonzero['datetime'] = df_nonzero.index
            # df_nonzero = df_nonzero.fillna(0)

            quantile_values = df_nonzero.resample('D', on='datetime').quantile(q = select)

            df_nonzero = quantile_values

        elif sample_length == 'month':

            # compute the quantile value for selected parameter
            quantile_values = df_nonzero.resample('M', on='datetime').quantile(q=select, interpolation='nearest')[self.resample_name]

            # define the headings
            quantile_values = pd.DataFrame(quantile_values, columns=[str(self.resample_name)])

            # select the rows where the selected column is equal to the quantile
            df_nonzero = df_nonzero.loc[np.in1d(np.array(df_nonzero[self.resample_name]), np.array(quantile_values[self.resample_name]))]

            # resample again to ensure the frequency
            df_nonzero = df_nonzero.resample('M', on='datetime').first()

            # df_nonzero = df_nonzero.resample('60min', on='datetime').quantile(q=select)
            df_nonzero['datetime'] = df_nonzero.index
            df_nonzero = df_nonzero.fillna(0)
        
        elif sample_length == 'minute':

            quantile_values = df_nonzero.resample('T', on='datetime').quantile(q=select)
            df_nonzero = quantile_values

        else:
            df_nonzero=df

        # output the result:
        return df_nonzero


    def multi_module_resampler(self, sample_length='hour', quantile_v=0.5, param_name='Isc'):
        '''
        Input: This function uses the data_sampler function in this object 
        output: to resample the df for each module.
        '''
        # store the resampling period into the object
        self.resample_T = sample_length
        # load a list of pd dataframe from object for each module:
        module_df_list = self.module_df_list
        # create an empty list to collect the resampled df.
        module_df_sampled = []
        for module_df in module_df_list:
            # resample it:
            module_sampled = self.data_resampler(df=module_df, sample_length=sample_length, select=quantile_v)
            module_df_sampled.append(module_sampled)
            # # print(module_sampled)
        # save the result to the object:
        # # print(module_df_sampled)
        self.module_df_sampled = module_df_sampled


    def path_selector(self, table_name):
        '''
        input: the name of the table.

        output: the path of the output that contains this table.
        '''
        # create a list of list of table name correspond to each path.
        # load the result from the object:
        dates_list_of_list = self.list_of_date
        # # print(dates_list_of_list)
        # check which list contain the given table name:
        table_name = str(table_name)
        counter = 0
        for list in dates_list_of_list:
            # # print(list)
            # # print(table_name)
            if table_name in list:
                # print(table_name)
                # # print(list)
                # # print(counter)
                break
            # udpate the counter
            else:
                counter = counter + 1
        # # print(counter)
        # now counter should be the index of hte path containing the correct date.
        # if counter <= len(self.path):
        path = self.path[counter]
        # # print(counter)
        # # print(path)

        return path


    def subset_by_iqr(self, df, column, Q1_def=0.3, Q2_def=0.6):
        """Remove outliers from a dataframe by column, including optional
           whiskers, removing rows for which the column value are
           less than Q1-1.5IQR or greater than Q3+1.5IQR.
        Args:
            df (`:obj:pd.DataFrame`): A pandas dataframe to subset
            column (str): Name of the column to calculate the subset from.
            whisker_width (float): Optional, loosen the IQR filter by a
                                   factor of `whisker_width` * IQR.
        Returns:
            (`:obj:pd.DataFrame`): Filtered dataframe
        """
        # read the widker width from the object
        whisker_width = self.iqr_width
        # Calculate Q1, Q2 and IQR
        q1 = df[column].quantile(Q1_def)
        q3 = df[column].quantile(Q2_def)
        iqr = q3 - q1
        # Apply filter with respect to IQR, including optional whiskers
        filter = (df[column] >= q1 - whisker_width*iqr) & (df[column] <= q3 + whisker_width*iqr)
        # # print('filtered')
        return df.loc[filter]


    def bin_selector(self, param_name, centre_value=40, rangevalue=1):
        '''
        input:
        param_name: the name of the parameter to filter with
        centre_value: selected centre value.
        rangevalue: the chosen data will only include the selected value plus or minus rangevalue

        output: a dataframe after selecting the given paramter based on the given range.
        '''
        # load the data from object
        dflist = self.module_df_sampled
        dflist_filterd = []
        for df in dflist:
            # apply filtering
            filter = ((df[str(param_name)]>(centre_value-rangevalue)) & (df[str(param_name)]<(centre_value+rangevalue)))
            df = df.loc[filter]
            # store the object
            dflist_filterd.append(df)
        # store the object
        self.module_df_sampled = dflist_filterd


    def temperature_correction(self, Voc_coeff=-0.3, Isc_coeff=0.05, Pmpp_coeff=0.35, targetT = 25, Voc_coeff_unit = '%/C'):
        '''
        Input:
            Voc_coeff: open cirucit voltage temperature coefficient the default unit is %/C
            Isc_coeff: short circuit current temperature coefficient the default unit is %/C
            Pmpp_coeff: maximum power temperature coefficient the default unit is %/C
            target_T: the temperature to correct to, default is 25 C.

        Output:
            update Voc, Isc, Pmpp and FF only (not for Vmmp or Impp)
        '''
        # load the df from the object
        dflist = self.module_df_sampled
        dflist_T_corrected = []
        for df in dflist:
            # create a colume representing hte difference between real T and corrected T
            df['dT'] = df['MT'] - targetT

            # add the corrected Voc colume
            if Voc_coeff_unit == '%/C':
                df['Voc'] = df['Voc'] - df['Voc']*(Voc_coeff)/100*df['dT']
            else:
                # print('The Voc temperature coefficient unit assumed to be mV/C')
                df['Voc'] = df['Voc'] - (Voc_coeff)*df['dT']/1e3
            # # print(df['Voc_T_corrected'])

            # add the corrected Isc colume
            df['Isc'] = df['Isc'] - df['Isc']*(Isc_coeff)/100*df['dT']
            # # print(df['Isc_T_corrected'])

            # add the corrected Pmpp colume
            df['Pm'] = df['Pm'] - df['Pm']*(Pmpp_coeff)/100*df['dT']
            # # print(df['Pmpp_T_corrected'])

            # add the corrected FF colume
            df['FF'] = df['Pm']/df['Isc']/df['Voc']
            # # print(df['FF_T_corrected'])
            
            # store the update pd dataframe
            dflist_T_corrected.append(df)
        
        # store the temperature correction into the object
        self.module_df_sampled = dflist_T_corrected


    def irradiance_correction(self, alpha=1):
        '''
        input: alpha: irradiance correction factor for open circuit voltage.

        Output:
        Calcualte the difference from measured irradiance to 1000W/m2 (as a factor)
        
        '''
        # load the list of df from object
        dflist = self.module_df_sampled
        dflist_IR_corrected = []

        for df in dflist:
            # calcualte the factor between measured Ir with 1000
            df['Ir_factor'] = df['IR_BEV']/1e3

            # correct Isc
            df['Isc'] = df['Isc']/df['Ir_factor']

            # correct Voc: referrnece: "Correction procedures for temperature and irradiance of PV modules, Silvia Luciani..."
            df['Voc'] = df['Voc'] - df['Voc']*np.log(df['Ir_factor'])*alpha

            # correct Pm
            df['Pm'] = df['Pm']/df['Ir_factor']

            # correct FF
            df['FF'] = df['Pm']/df['Isc']/df['Voc']

            dflist_IR_corrected.append(df)

        self.module_df_sampled = dflist_IR_corrected


    def time_to_int(dateobj):
        '''
        Input: a datetime series

        Output: an integer series with unit of second.
        '''
        total = int(dateobj.strftime('%S'))
        total += int(dateobj.strftime('%M')) * 60
        total += int(dateobj.strftime('%H')) * 60 * 60
        total += (int(dateobj.strftime('%j')) - 1) * 60 * 60 * 24
        total += (int(dateobj.strftime('%Y')) - 1970) * 60 * 60 * 24 * 365
        return total


    def zero_removal2(self):
        '''
        Input: This function update the df frame named "module_df_sampled"
        Output: the module_df_sampled after removing zeros.
        '''
        # prepare a list to collect non zero values
        df_list2 = []
        for module in self.module_df_sampled:
            df_list2.append(module[module['Pm']!=0])
        # store the updated df list into the object
        # # print(df_list2)
        self.module_df_sampled = df_list2


    def hist_visual(self, param='MT', bins=100):
        '''
        Input:
            param: a string indicating which parameter to select on.
            bins: the number of bins to have for the histogram,
        
        Output:
        This funciton plot a histogram of self.module_df_sampled for each module
        '''
        # load the list of dataframe from the object
        df_list = self.module_df_sampled
        k = 0

        for module_df in df_list:
            # plot a histogram based on the given parameter
            plt.figure()
            plt.title('Module' + str(self.module_num_list[k]))
            module_df[str(param)].plot.hist(bins=bins)
            plt.xlabel(self.column_name_dict[param])
            k = k + 1


    def interpolator(self, x, y):
        '''
        Input:
        x, y are the series of data to include for plotting before doing the interpolation.

        Output:
        1. check if there is data missing.
        2. if there is data missing, interpolate the missing data with lienar fit.
        3. output:
            x_interpolate, y_interpolate: the series after interpolation, they include both original data and interpolated data
            interpolate_index: a list of interpolated index
            interpolate_night: a boolean index which if true, will fill the night time data with zero, otherwise leave it empty
        '''
        # read whether interpolate night data from the object
        interpolate_night = self.interpolate_night
        # find the time step based on the resampling period
        sampleT = self.resample_T
        if sampleT == 'hour' or sampleT == 'hours':
            # # print(x[1] - x[0])
            dt = datetime.timedelta(hours=1)
            # # print(dt)
        elif sampleT == 'day':
            dt = datetime.timedelta(days=1)
        elif sampleT == 'month':
            dt = datetime.timedelta(days=30)
        else:
            dt = datetime.timedelta(seconds=9)

        # use a while loop to start with the first time step
        # remove the inf and Nan
        # mask = x.isin([np.nan, np.inf, -np.inf]).any(axis=0)
        # x = x[mask]

        checking_x = x.iloc[0]
        x_interpolate = []
        y_interpolate = []
        # datetime_int = []
        interpolate_ind = []
        running_ind = 0
        while checking_x <= x.iloc[-1]:
            # while the checking_x is within the time range, see if is checking_x within the x series
            if sampleT == 'hour':
                if checking_x in x:
                    # empty_ = False
                    x_interpolate.append(checking_x)
                    y_interpolate.append(y[checking_x])
                elif (checking_x.hour>=5 and checking_x.hour<=21) and (sampleT == 'day' or sampleT == 'month' or sampleT == 'hour'):
                    # interpolate if it is day time and the data is empty, and we have a stable period
                    x_interpolate.append(checking_x)
                    y_interpolate.append(float('NAN'))
                    # datetime_int.append(checking_x)
                    interpolate_ind.append(running_ind)
                elif (interpolate_night == True) and (sampleT == 'day' or sampleT == 'month' or sampleT == 'hour'):
                    # if the checking datetime is during night time, fill with zeros
                    x_interpolate.append(checking_x)
                    y_interpolate.append(float(0))
                    interpolate_ind.append(running_ind)
                # else:
                    # this is the case when we don't want to interpolate the night time data, and the data are night emtpy time data just leave it empty
                    # x_interpolate.append(checking_x)
                    # y_interpolate.append(y[checking_x])
            else:
                if min(abs(x - checking_x)) <= dt:
                    # empty_ = False
                    x_interpolate.append(checking_x)
                    # input the nearest value
                    # Find the nearest timestamp in the Series
                    nearest_timestamp = (x - checking_x).astype('timedelta64[ns]').abs().idxmin()
                    # Get the value at the nearest timestamp
                    nearest_value = y.loc[nearest_timestamp]
                    y_interpolate.append(nearest_value)
                # if checking_x in x:
                #     x_interpolate.append(checking_x)
                #     y_interpolate.append(y[checking_x])

                elif (sampleT == 'day' or sampleT == 'month'):
                    # interpolate if it is day time and the data is empty, and we have a stable period.
                    x_interpolate.append(checking_x)
                    y_interpolate.append(np.nan)
                    # datetime_int.append(checking_x)
                    interpolate_ind.append(running_ind)

            # goes to the next time step
            checking_x = checking_x + dt
            running_ind += 1
        # apply interpolation to y_interpolate
        y_interpolate = pd.DataFrame(y_interpolate).interpolate()
        # print(np.shape(y_interpolate))
        return x_interpolate, y_interpolate, interpolate_ind


    def pickle_extractor(self):
        '''
        This function extracts the pickle files into pd dataframe if there is previous pickle files stored
        if there is no existing pickle file, it will do the extraction by itself
        '''
        pickle_files = glob.glob("*.pkl")
        if len(pickle_files) != 1:
            print("Error: There is not exactly one pickle file in the folder, so run the extraction again")
            # extract the data
            self.data_extractor(removezero=True, store_df=True)
        else:
            pickle_file_name = os.path.basename(pickle_files[0])
            print("Successfully found the pickle file, The pickle file name is:", pickle_file_name)
            # extract the starting and ending date
            starting_date, ending_date = pickle_file_name.split()
            ending_date = ending_date.split('.')[0]
            # if the date does match we just read the file from pickle
            if starting_date == self.starting_day and ending_date == self.ending_day:
                # read as pd dataframe
                print('The stored pickle file matches the date requirement, so just read pd dataframe from pickle file')
                self.df_nonzero = pd.read_pickle(pickle_file_name)
            else:
                # otherwise, rerun the extraction process
                print('The stored pickle file does not match the date requirement, so run the data extraction')
                self.data_extractor(removezero=True, store_df=True)

    
    def subplot_collector(self, delete_ori_im=True):
        '''
        This function collect the exported figures and create a subplot

        input:
            1. column_num: an integer input indicating the number of column to have in the subplot
            2. hspace: the distance between rows
            3. wspace: the distance between columns
            4. delete_ori_im: a boolean input, if true, will delete all other image file in the current folder before exporting the subplot
        '''

        # Define directory path
        dir_path = os.path.dirname(os.path.realpath(__file__))

        # Get list of PNG files in directory
        png_files = [f for f in os.listdir(dir_path) if f.endswith('.png')]

        # calculate the column number for the subplot
        if len(png_files) == 2:
            column_num = 2
            num_rows = 1
            hspace = -0.9
            wspace = -0.3
        elif len(png_files) == 3:
            column_num = 3
            num_rows = 1
            hspace = -0.9
            wspace = -0.3
        elif len(png_files) == 4:
            column_num = 2
            num_rows = 2
            hspace = -1.25
            wspace = -1.3
        elif len(png_files) == 5:
            column_num = 3
            num_rows = 2
            hspace = -1.55
            wspace = -0.3
        else:
            column_num = 3
            num_rows = 2
            hspace = -1.16
            wspace = -0.3

        # Create subplot with 2 columns and variable number of rows
        fig, axes = plt.subplots(nrows=num_rows, ncols=column_num, figsize=(24, 24*num_rows))

        # adjust the spacing
        fig.subplots_adjust(hspace=hspace, wspace=wspace)

        # Iterate over PNG files and plot each one on a separate axis
        for i, png_file in enumerate(png_files):
            row_idx = i // column_num
            col_idx = i % column_num
            if num_rows == 1:
                ax = axes[col_idx]
            else:
                ax = axes[row_idx, col_idx]

            ax.imshow(plt.imread(os.path.join(dir_path, png_file)))
            ax.axis('off')

        # for the 5 figure subplot, turn off the axis for the last plot and centre the last two images in the last row
        if len(png_files) == 5:
            axes[-1, -1].set_axis_off()

        # Adjust spacing between subplots and save figure
        fig.tight_layout()


        # delete the other image files if asked
        if delete_ori_im:
            self.image_deleter()

        # save the subplot
        plt.savefig('subplot.png')


    def image_deleter(self):
        '''
        This function delete all the image file in the current directory
        '''
        # Get the current directory path
        dir_path = os.getcwd()

        # Loop through all files in the directory
        for file_name in os.listdir(dir_path):
            # Check if the file is an image file (change the extension to the one you need)
            if file_name.endswith('.png') or file_name.endswith('.jpg') or file_name.endswith('.jpeg'):
                # Construct the full file path
                file_path = os.path.join(dir_path, file_name)
                # Delete the file
                os.remove(file_path)


    def five_image_crop(self, upper_percentage = 0.4, lower_percentage = 0.4, left_percentage = 0, right_percentage = 0, input_image = None):
        '''
        This function takes the subplot generated for five image and scrop and rescale the image
        '''
        if input_image == None:
            # Find the png file in the current directory
            file_path = glob.glob("*.png")[0]

            # open the image
            image = Image.open(file_path)
        else:
            image = input_image

        # get the size of image
        width, height = image.size

        # calculate the cropping box
        top = round(height * upper_percentage) # top third of impage
        bottom = round(height * (1-lower_percentage)) # bottom thrid of image
        left = round(width * left_percentage) # left edge of the image
        right = round(width * (1-right_percentage)) # right edge of the image

        # cropping
        image  = image.crop((left, top, right, bottom))

        # save the image
        # image.save('subplot_cropped.png')

        # show the image
        # plt.figure(figsize=(100, 300))
        # plt.axis('off')
        # plt.imshow(image)
        # plt.show()

        return image


    def five_image_subplot(self):
        '''
        Step 1: crop out the first line
        Step 2: crop out the second line
        Step 3: Put then together and adjust the last two image alginment
        Step 4: show and save
        '''

        # crop out the upper half
        upper_half = self.five_image_crop(upper_percentage=0.4, lower_percentage=0.495)
        lower_half = self.five_image_crop(upper_percentage=0.502, lower_percentage=0.398)

        # shift the image to the right by 1/2-1/3=1/6
        lower_array = np.array(lower_half)
        lower_width = np.shape(lower_array)[1]
        shifted_lower_array = np.roll(lower_array, round(lower_width/6), axis=1)

        # convert the array back to image
        shifted_lower = Image.fromarray(shifted_lower_array)

        # display the shifted image
        # plt.figure(figsize=(100, 300))
        # plt.axis('off')
        # plt.imshow(shifted_lower)
        # plt.show()

        # merge the upper half with the shifted lower half
        upper_array = np.array(upper_half)
        merged_array = np.concatenate([upper_array, shifted_lower], axis=0)

        # convert the array back to image
        merged_image = Image.fromarray(merged_array)

        # display the merged image
        plt.figure(figsize=(100, 300))
        plt.axis('off')
        plt.imshow(merged_image)
        plt.show()

        # export the merged image
        merged_image.save('five_image_subplot.png')


# %% Appendix A: this is the code to open all files in a folder.

# from os import walk

# mypath=('C:\\Users\\z5338154\\OneDrive - UNSW\\Desktop\\PhD\\0_Projects\\EL-PL\\IEEE data\\Griddler\\Raw Patterns\\')

# f = []

# for (dirpath, dirnames, filenames) in walk(mypath):

#     f.extend(filenames)

#     break
# %% Appendix B: these are the functions defined in the object but not endded up being used.

    def data_parameter_plot(self, x_name, y_name, module_number=2):
        '''
        This function takes the value from the selected module and plot inter parameter.
        '''
        # read the data from the object.
        df = self.df_nonzero
        # filter out the other module number
        df = self.module_selector(module_number=module_number, return_value=True)
        # select the x axis data;
        x_data = df[x_name]
        # select the y axis data:
        y_data = df[y_name]

        # look up the name to plot from object dictionary:
        x_name = self.column_name_dict[x_name]
        y_name = self.column_name_dict[y_name]

        # plot the data:
        plt.figure()
        plt.scatter(x_data, y_data)
        plt.xlabel(x_name)
        plt.ylabel(y_name)
        plt.title(' Between '+  str(self.starting_datetime) + ' and ' + str(self.starting_datetime))
        # plt.gcf().autofmt_xdate()
        plt.show()


    def crop(self, filename):
        #Read the image
        img = cv2.imread(filename)
        #Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        #Separate the background from the foreground
        bit = cv2.bitwise_not(gray)
        #Apply adaptive mean thresholding
        amtImage = cv2.adaptiveThreshold(bit, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 35, 15)
        #Apply erosion to fill the gaps
        kernel = np.ones((15,15),np.uint8)
        erosion = cv2.erode(amtImage,kernel,iterations = 2)
        #Take the height and width of the image
        (height, width) = img.shape[0:2]
        #Ignore the limits/extremities of the document (sometimes are black, so they distract the algorithm)
        image = erosion[50:height - 50, 50: width - 50]
        (nheight, nwidth) = image.shape[0:2]
        #Create a list to save the indexes of lines containing more than 20% of black.
        index = []
        for x in range (0, nheight):
            line = []
                
            for y in range(0, nwidth):
                line2 = []
                if (image[x, y] < 150):
                    line.append(image[x, y])
            if (len(line) / nwidth > 0.2):    
                index.append(x)
        #Create a list to save the indexes of columns containing more than 15% of black.
        index2 = []
        for a in range(0, nwidth):
            line2 = []
            for b in range(0, nheight):
                if image[b, a] < 150:
                    line2.append(image[b, a])
            if (len(line2) / nheight > 0.15):
                index2.append(a)

        #Crop the original image according to the max and min of black lines and columns.
        img = img[min(index):max(index) + min(250, (height - max(index))* 10 // 11) , max(0, min(index2)): max(index2) + min(250, (width - max(index2)) * 10 // 11)]
        #Save the image
        cv2.imwrite('res_' + filename, img)
        # display the image
        img.show
# end of the function