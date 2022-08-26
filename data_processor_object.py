# %%-- import the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
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
