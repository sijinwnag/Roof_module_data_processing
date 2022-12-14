'''
Object structure:
1. file_date_reader generate a list of list, which correspond to the date include in each access file.
2. zero_removr: collect all the non-zero datapoint into: df_nonzero
3. module selector: input is df_nonzero, then it generates:
    -updated df_nonzero: a pd dataframe filtered out the modules not selected.
    -module_df_list: a list of pd dataframe correspond to each selected module.
    -module_num_list: a list of number correspond to the selected module 
4. multi_module_resampler: input is module_num_list and module_df_list, then output is:module_df_resampled (a list of pd dataframe for each module after resampling)
5. The plot is plot based on module_df_resampled. (applying outlier removal based on self.iqr_width)


to do:
1. add another zero removal at the end before plotting.
'''

# %%-- import the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import datetime
# %%-


class module_data_processor:
    """
    This is an object that reads and process the module data from an access databse file.
    """


    def __init__(self, path, starting_day, ending_day, starting_time, ending_time, iqr_width=10):
        """
        1. Write the path, starting date and ending date into the object.
        2. Input:
            pathlist: a list of databse access path we want to load.
            starting date: the first day to start.
            ending date: the end of the time to look at.
        """

        # self.pathlist = pathlist
        # self.starting_time = starting_time
        # self.ending_time = ending_time
        # the path is a list of string correspond to the path of each files.
        # data for 2022.
        self.path = [
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
        r'C:\Users\sijin wang\Desktop\research\RA\Module_data_project\data\2020\27 Nov 2020 - 18 Dec 2020\SP1053.accdb'
        ]

        # self.path = path
        self.starting_day = starting_day
        self.ending_day = ending_day

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
        # print(self.starting_datetime)
        # print(self.ending_datetime)
        self.years_available = range(int(year_st), int(year_ed) + 1)
        # print(list(self.years_available))

        # define a dictionary that tranlate the column name from raw data to more understandable names:
        self.column_name_dict = {'AH':'Absolute humidity %',
        'AT':'Ambient temperature (\u00B0C)',
        'MT':'Module temperature (\u00B0C)',
        'IR_BEV':'Irradiance (W/m2)',
        'Voc': 'Voc(V)',
        'Isc':'Isc (A)',
        'Vm':'Maximum power voltage (V)',
        'Im': 'Maximum power current (A)',
        'Pm': 'Maximum power',
        'FF':'Fill factor (%)'}

        # define a list of colour correspond to each module: blue, green red, magenta, and yellow.
        self.colour_list = ['b', 'g', 'r', 'c', 'm', 'y']

        # define the whisker width for outlier removeal
        self.iqr_width = iqr_width


    def table_name_reader(self, path):
        """
        Input:
        path: a string which is the paths of the access file.

        output: a list of string which contains the IV tables of each day
        """

        # read the path from the object:
        # path = self.path

        # create the connection
        msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        # print(msa_drivers)
        con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + str(path) + ';'
        conn = pyodbc.connect(con_string)

        # Create the cursor object to read how many table we got:
        cur = conn.cursor()
        IV_table_names = []
        for row in cur.tables():
            # if the table name ends with "IV" and start with a digit, collect it into the list.
            if row.table_name[-2:] == 'IV' and row.table_name[0].isdigit():
                # print(row.table_name)
                IV_table_names.append(row.table_name)
        cur.close()

        # output:
        return IV_table_names


    def file_date_reader(self):
        '''
        input: the files
        output: a list of string of date corresponding to each file. (a list of list)
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
        # print(path)

        # create the connection
        msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        # print(msa_drivers)
        con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + str(path) + ';'
        conn = pyodbc.connect(con_string)

        # define the query:
        sql_query = str(date) # + 'IV'
        df = pd.read_sql('SELECT * FROM ' + str(date), conn)
        # print(date)
        # print(sql_query)
        # print(df)

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
        # print(starting_date)
        ending_year, ending_month, ending_day = ending_date.split('_')
        ending_date = datetime.datetime(int(ending_year), int(ending_month), int(ending_day))
        # print(ending_date)

        # find all the date that are between these two:
        dates_between = np.array(pd.date_range(starting_date, ending_date, freq='D'))
        dates_between = np.sort(dates_between)
        # sort the dates in time order:

        # print(dates_between)

        date_list = [] # a list of date to include, each date correspond to one table.
        for date in dates_between:
            # convert from numpy.datetime64 to string:
            date = str(date)
            # print(date)
            # delete the time part:
            date = date.split('T')[0]
            # print(date)
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

        # print(date_list)
        # filter out the dates that are not contained in the files.
        # load the available dates from the object.
        available_dates = np.concatenate(self.list_of_date).flat
        # convert into numpy array to avoid using for loop:
        date_list = np.array(date_list)
        # check each element:
        availability = np.isin(date_list, available_dates)
        # print(availability)
        # print(date_list)
        # print(available_dates)
        index = np.argwhere(availability)
        # keep the available dates only:
        date_list = date_list[index].flatten()
        # convert back to list:
        date_list = date_list.tolist()
        # print(date_list)

        # now extract the IV data for the given table and concat into a single dataframe:
        # start with the first date in the list, you will need to select the correct path, the path that contains the table name str(date) + 'IV'
        df2 = self.data_reader_day(date_list[0])
        # print(df2)
        # use a for loop to concanate the rest of hte data:
        for date in date_list[1:]:
            # read the df
            # print(date)
            df = self.data_reader_day(date)
            if df.empty:
                continue
            # print(df)
            

            # sort the df by time.
            # remove all empty rows: it must start with a number.
            df = df.dropna()

            # print(df)
            # print(df['xts'].str.endswith(('M', 'A', 'P'))
            # the 2020 data uses 24 hour format instead of am pm.
            df['xday'] = df['xday'].astype(str).str.strip()
            if df['xts'].str.endswith(('M', 'A', 'P')).all():
                # print('AM or PM detected')
                # some date that use A instead of AM, so lets add m at the end if anything not ending with an m:
                # remove all M:
                df['xts'] = df['xts'].str.replace("M", "")
                # add the M back: no matter wheter we had M before, this will results in everything having one M.
                df['xts'] = df['xts'].astype(str) + 'M'
                # we also need to have zero padding for the hour to match the %I.
                # find the ones that have only one digit for hour, we expect the xts column second element to be :
                df['xts'] = df['xts'].str.zfill(11)
                # print(df['xts'])
                # print(df['xday'])
                # convert the 00 to 12 to match with %I.
                # df['xts'].astype(str)
                df = df[df['xts']!='05:04:??9 AM']
                # print(df['xts'])
                df['xts_datetime'] = pd.to_datetime(df['xts'].astype(str), format='%H:%M:%S %p') # this column is an intermedium column, the pm and am are not converted correctly, but it will be corrected in later lines.
                # print(df['xts_datetime'])
                # print(df['xday'])
                df['xday'] = pd.to_datetime(df['xday'].astype(str).str.strip(), format='%d/%m/%Y')
                # print(df['xday'])
                # print(df['xts'])
                # print(df)
                # combine the datetime column:
                df['year'] = pd.DatetimeIndex(df['xday']).year
                df['month'] = pd.DatetimeIndex(df['xday']).month
                df['day'] = pd.DatetimeIndex(df['xday']).day
                df['hour'] = pd.DatetimeIndex(df['xts_datetime']).hour
                df['miutes'] = pd.DatetimeIndex(df['xts_datetime']).minute
                df['second'] = pd.DatetimeIndex(df['xts_datetime']).second
                df[['time', 'PM']] = df['xts'].str.split(' ', expand=True)
                # print(df['PM'])
                df['hour'] = df['hour'] + 12*(df['PM'] == 'PM')
                df=df[df['hour']!=24]
                df['datetime'] = pd.to_datetime(df.year.astype(str) + ' ' + df.month.astype(str) + ' ' + df.day.astype(str) + ' ' + df.hour.astype(str) + ':' + df.miutes.astype(str) + ':' + df.second.astype(str), format = "%Y %m %d %H:%M:%S")
                # print(df['datetime'])
                # delete intermedium column to produce the datetime column.
                df = df.drop(['xts_datetime', 'year', 'month', 'day', 'hour', 'miutes', 'second', 'time', ], axis=1)
                # print(df)
                # sort the df by datetime column.
                df = df.sort_values(by='datetime')
                # print(df)
                # filter out the data through datetime column:
                df = df[np.array(df['datetime']>self.starting_datetime) * np.array(df['datetime']<self.ending_datetime)]
                # print(df)
                # store df into the object.
                # self.df_days = df

            else:
                # print('no AM or PM detected.')
                # print(df['xts'])
                # print(df['xday'])
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
            df2 = pd.concat([df, df2], axis=0)
        # print(df2)

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
            print(files)
            if date in files:
                # if the file have this date, break the loop
                # print('true')
                break
            else:
                # otherwise update the counter
                counter = counter + 1
        # now we should get the counter that is the index of the file.
        # print(counter)
        path = self.path[counter]
        # output the result:
        print(path)
        return path


    def zero_remover(self, removezero=True):
        """
        This function takes the df_days and remove the zero outliers
        removezero: a boolean input, if true, remove the zeros, otherwise just read the data.
        """
        # run the code to extract the dates.
        df = self.date_selector()
        # print(df)
        # use a new column to identify whether to delete it by multiplying everything together
        product = df['Voc'] * df['Isc'] * df['Vm'] * df['Im'] * df['Pm'] * df['FF']
        df['nonzero'] = (product != 0)
        # print(df)
        # filter out the zero data.
        if removezero == True:
            df_nonzero = df[df['nonzero']==True]
        else:
            df_nonzero = df
        # # delete the extra label column.
        # df_nonzero = df_nonzero.drop('whether_keep')
        # store the data in the object
        # print(np.min(df_nonzero))
        self.df_nonzero = df_nonzero


    def module_selector(self, module_num_list = [1, 2, 3]):
        """
        This process is after the step of zero remover:
        it takes the pd dataframe after removal of zeros.
        and filter the dataset to have only a specific module.
        """
        # read the df from object.
        df_nonzero = self.df_nonzero
        pd_list = []
        # apply the filtering:
        keep_data_list = np.zeros(np.shape(df_nonzero['cno'] == module_num_list[0]))
        for number in module_num_list:
            keep_data = np.array(df_nonzero['cno'] == number)
            # print(keep_data)
            # print(keep_data_list)
            keep_data_list = np.any([keep_data_list, keep_data], axis=0)
            pd_list.append(df_nonzero[keep_data])

        # print(np.sum(keep_data_list))
        df_nonzero_module = df_nonzero[keep_data_list]
        # store the filtered data into the object:
        self.df_nonzero = df_nonzero_module

        # split the df_nonzero into a list of pd dataframe for each module:
        self.module_df_list = pd_list

        # store the module selection into the object.
        self.module_num_list = module_num_list


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


    def data_ploter_with_time_multimodule(self, target_name, linear_fit=False):
        '''
        This function will plot the parmeter with time but plot multiple module value on the same graph.
        '''
        plt.figure()
        counter = 0
        for module in self.module_df_sampled:
            counter = counter + 1
            # select the pd frame for this module:
            pd_module = module
            # remove the outliers
            pd_module = self.subset_by_iqr(df=pd_module, column=target_name)
            # try to split the pd module by year:
            # print(pd_module)
            for year in self.years_available:
                y = pd_module[pd_module['datetime'].dt.year == year][target_name]
                x = pd_module[pd_module['datetime'].dt.year == year]['datetime']
            # select the x and y column names:
            # y = pd_module[target_name]
            # x = pd_module['datetime']
                plt.plot(x, y, label='Module ' + str(self.module_num_list[counter-1]) + ' (' + str(year) + ')')
                # add the linear plot: both the line and the equation.
                # print(x)
                # print(x[0])
                # x_fit = []
                # for dx in x:
                    # print(datetime.datetime.fromtimestamp(dx))
                    # print(datetime.date(self.years_available[0],1,1).timestamp())
                    # x_fit.append(datetime.datetime.fromtimestamp(dx) - datetime.date(self.years_available[0],1,1))
                if linear_fit == True:
                    x2 = []
                    for dx in x:
                        # print(dx)
                        # print(str(dx))
                        # print(self.time_to_int(str(dx)))
                        total = int(dx.strftime('%S'))
                        # print(total)
                        # total += int(dx.strftime('%M')) * 60
                        # print(total)
                        # total += int(dx.strftime('%H')) * 60 * 60
                        # print(total)
                        total += (int(dx.strftime('%j')) - 1)
                        # print(total)
                        total += (int(dx.strftime('%Y')) - 2020) * 365
                        # print(total)
                        x2.append(total)

                    # print(y.type())
                    coef = np.polyfit(x2,y,1)
                    poly1d_fn = np.poly1d(coef)
                    plt.plot(x, poly1d_fn(x2), '--k')
                    print('The slope is ' + str(round(coef[0], 4)) + '/day' + ' for module ' + str(self.module_num_list[counter-1]))

        # look up the name from dictionary:
        target_name = self.column_name_dict[target_name]
        plt.xlabel('Time')
        plt.ylabel(target_name)
        plt.title(' Between '+  str(self.starting_datetime) + ' and ' + str(self.ending_datetime))
        plt.gcf().autofmt_xdate()
        plt.legend()
        plt.show()


    def data_parameter_plot_multimodule(self, x_name, y_name, linear_fit=False):
        '''
        This function will plot the parmeter with parameter but plot multiple module value on the same graph.
        '''
        plt.figure()
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
                plt.scatter(x, y, label='Module ' + str(self.module_num_list[counter-1]) + ' (' + str(year) + ')', s=10)
                if linear_fit == True:
                    coef = np.polyfit(x,y,1)
                    poly1d_fn = np.poly1d(coef)
                    plt.plot(x, poly1d_fn(x), '--k')
                    print('The slope is ' + str(round(coef[0], 4)) + ' for module ' + str(self.module_num_list[counter-1]))
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
        plt.title(' Between '+  str(self.starting_datetime) + ' and ' + str(self.ending_datetime))
        plt.gcf().autofmt_xdate()
        plt.legend()
        plt.show()


    def data_resampler(self, df, sample_length='hour', select='mean'):
        '''
        This function takes the pd dataframe after removing all zeros, then resample the data based on the requirement: minute, hour, day, month.
        '''
        df_nonzero=df
        if sample_length == 'hour':
            # we will resample the data based on the hour colume:
            # print(df_nonzero)
            # convert the datetime column to hour:
            df_datetime = df_nonzero['datetime']
            if select == 'mean':
                df_nonzero = df_nonzero.resample('60min', on='datetime').mean()
            else:
                df_nonzero = df_nonzero.resample('60min', on='datetime').max()
            df_nonzero['datetime'] = df_nonzero.index
            df_nonzero = df_nonzero.fillna(0)
        elif sample_length == 'day':
            df_datetime = df_nonzero['datetime']
            if select == 'mean':
                df_nonzero = df_nonzero.resample('D', on='datetime').mean()
            else:
                df_nonzero = df_nonzero.resample('D', on='datetime').max()
            df_nonzero['datetime'] = df_nonzero.index
            df_nonzero = df_nonzero.fillna(0)
        elif sample_length == 'month':
            df_datetime = df_nonzero['datetime']
            if select == 'mean':
                df_nonzero = df_nonzero.resample('M', on='datetime').mean()
            else:
                df_nonzero = df_nonzero.resample('M', on='datetime').max()
            df_nonzero['datetime'] = df_nonzero.index
            df_nonzero = df_nonzero.fillna(0)
        else:
            df_nonzero=df

        # output the result:
        return df_nonzero


    def multi_module_resampler(self, sample_length='hour', select='mean'):
        '''
        This function uses the data_sampler function in this object to resample the df for each module.
        '''
        # load a list of pd dataframe from object for each module:
        module_df_list = self.module_df_list
        # create an empty list to collect the resampled df.
        module_df_sampled = []
        for module_df in module_df_list:
            # resample it:
            module_sampled = self.data_resampler(df=module_df, sample_length=sample_length, select=select)
            module_df_sampled.append(module_sampled)
            # print(module_sampled)
        # save the result to the object:
        self.module_df_sampled = module_df_sampled


    def path_selector(self, table_name):
        '''
        input: the name of the table.

        output: the path of the output that contains this table.
        '''
        # create a list of list of table name correspond to each path.
        # self.file_date_reader()
        # load the result from the object:
        dates_list_of_list = self.list_of_date
        # print(dates_list_of_list)
        # check which list contain the given table name:
        table_name = str(table_name)
        counter = 0
        for list in dates_list_of_list:
            # print(list)
            # print(table_name)
            if table_name in list:
                print(table_name)
                # print(list)
                # print(counter)
                break
            # udpate the counter
            else:
                counter = counter + 1
        # print(counter)
        # now counter should be the index of hte path containing the correct date.
        # if counter <= len(self.path):
        path = self.path[counter]
        # print(counter)
        # print(path)

        return path


    def subset_by_iqr(self, df, column):
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
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        # Apply filter with respect to IQR, including optional whiskers
        filter = (df[column] >= q1 - whisker_width*iqr) & (df[column] <= q3 + whisker_width*iqr)
        # print('filtered')
        return df.loc[filter]
    

    def bin_selector(self, param_name, centre_value=40, rangevalue=1):
        '''
        param_name: the name of the parameter to filter with
        centre_value: selected centre value.
        rangevalue: the chosen data will only include the selected value plus or minus rangevalue
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

        What it does:
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
                print('The Voc temperature coefficient unit assumed to be mV/C')
                df['Voc'] = df['Voc'] - (Voc_coeff)*df['dT']/1e3
            # print(df['Voc_T_corrected'])

            # add the corrected Isc colume
            df['Isc'] = df['Isc'] - df['Isc']*(Isc_coeff)/100*df['dT']
            # print(df['Isc_T_corrected'])

            # add the corrected Pmpp colume
            df['Pm'] = df['Pm'] - df['Pm']*(Pmpp_coeff)/100*df['dT']
            # print(df['Pmpp_T_corrected'])

            # add the corrected FF colume
            df['FF'] = df['Pm']/df['Isc']/df['Voc']
            # print(df['FF_T_corrected'])
            
            # store the update pd dataframe
            dflist_T_corrected.append(df)
        
        # store the temperature correction into the object
        self.module_df_sampled = dflist_T_corrected


    def irradiance_correction(self, alpha=1):
        '''
        input: alpha: irradiance correction factor for open circuit voltage.

        What it does:
        1. Calcualte the difference from measured irradiance to 1000W/m2 (as a factor)
        
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
        This function tries to convert datetime into a number (seconds)
        '''
        total = int(dateobj.strftime('%S'))
        total += int(dateobj.strftime('%M')) * 60
        total += int(dateobj.strftime('%H')) * 60 * 60
        total += (int(dateobj.strftime('%j')) - 1) * 60 * 60 * 24
        total += (int(dateobj.strftime('%Y')) - 1970) * 60 * 60 * 24 * 365
        return total


    def zero_removal2(self):
        '''
        This function update the df frame named "module_df_sampled"
        '''
        # prepare a list to collect non zero values
        df_list2 = []
        for module in self.module_df_sampled:
            df_list2.append(module[module['Pm']!=0])
        # store the updated df list into the object
        self.module_df_sampled = df_list2