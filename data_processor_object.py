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


    def __init__(self, path):
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
        self.path = path


    def table_name_reader(self):
        """
        Input:
        path: a string which is the path of the access file.

        output: a list of string which contains the IV tables of each day
        """

        # read the path from the object:
        path = self.path

        # create the connection
        msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        # print(msa_drivers)
        con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + str(path) + ';'
        conn = pyodbc.connect(con_string)

        # Create the cursor object to read how many table we got:
        cur = conn.cursor()
        IV_table_names = []
        for row in cur.tables():
            # if the table name ends with "IV" collect it into the list.
            if row.table_name[-2:] == 'IV':
                # print(row.table_name)
                IV_table_names.append(row.table_name)
        cur.close()

        # output:
        return IV_table_names


    def data_reader_day(self, date):
        """
        input:
        date: a string that correspond the day we want to look at the data.

        output:
        date_data: a panda dataframe of IV data of that table.
        """

        # read the path from the object:
        path = self.path

        # create the connection
        msa_drivers = [x for x in pyodbc.drivers() if 'ACCESS' in x.upper()]
        # print(msa_drivers)
        con_string = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + str(path) + ';'
        conn = pyodbc.connect(con_string)

        # define the query:
        sql_query = str(date) + 'IV'
        df = pd.read_sql('SELECT * FROM ' + str(date) + 'IV', conn)

        return df


    def date_selector(self, starting_date, ending_date):
        """
        input: a period of time that we are interested in.
        starting_date: year_month_date string.
        ending_date: year_month_date string.

        output: a panda dataframe of IV data of the selected date.
        """

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

        date_list = []
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
            date_list.append(date)

        # now extract the IV data for the given table and concat into a single dataframe:
        # start with the first date in the list:
        # print(date_list[0])
        df = self.data_reader_day(date_list[0])
        # use a for loop to concanate the rest of hte data:
        for date in date_list[1:]:
            # read the df
            date_df = self.data_reader_day(date)
            # concanate with original one:
            df = pd.concat([df, date_df], axis=0)

        # sort the df by time.
        # some date that use A instead of AM, so lets add m at the end if anything not ending with an m:
        # remove all M:
        df['xts'] = df['xts'].str.replace("M", "")
        # add the M back: no matter wheter we had M before, this will results in everything having one M.
        df['xts'] = df['xts'].astype(str) + 'M'
        # we also need to have zero padding for the hour to match the %I.
        # find the ones that have only one digit for hour, we expect the xts column second element to be :
        df['xts'] = df['xts'].str.zfill(11)
        # convert the 00 to 12 to match with %I.
        # df['xts'].astype(str)
        df['xts_datetime'] = pd.to_datetime(df['xts'].astype(str), format='%H:%M:%S %p')
        # convert from string to datetime format.
        df['xday'] = pd.to_datetime(df['xday'].astype(str), format='%d/%m/%Y ')

        # combine the datetime column:
        df['year'] = pd.DatetimeIndex(df['xday']).year
        df['month'] = pd.DatetimeIndex(df['xday']).month
        df['day'] = pd.DatetimeIndex(df['xday']).day
        df['hour'] = pd.DatetimeIndex(df['xts_datetime']).hour
        df['miutes'] = pd.DatetimeIndex(df['xts_datetime']).minute
        df['second'] = pd.DatetimeIndex(df['xts_datetime']).second
        df[['time', 'PM']] = df['xts'].str.split(' ', expand=True)
        df['hour'] = df['hour'] + 12*(df['PM'] == 'PM')
        df['datetime'] = pd.to_datetime(df.year.astype(str) + ' ' + df.month.astype(str) + ' ' + df.day.astype(str) + ' ' + df.hour.astype(str) + ':' + df.miutes.astype(str) + ':' + df.second.astype(str), format = "%Y %m %d %H:%M:%S")
        # delete intermedium column to produce the datetime column.
        df = df.drop(['xts_datetime', 'year', 'month', 'day', 'hour', 'miutes', 'second', 'time', ], axis=1)

        # sort the df by datetime column.
        df = df.sort_values(by='datetime')

        # store df into the object.
        self.df_days = df

        return df


    def data_ploter(self, target_name):
        """
        This function takes the df stored in the object and plot the object with time.
        """
        plt.figure()
        plt.plot(self.df_days[target_name])
        plt.show()
