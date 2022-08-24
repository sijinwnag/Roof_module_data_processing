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


    def __init__(self, pathlist, starting_time, ending_time):
        """
        1. Write the path, starting date and ending date into the object.
        2. Input:
            pathlist: a list of databse access path we want to load.
            starting date: the first day to start.
            ending date: the end of the time to look at.
        """

        self.pathlist = pathlist
        self.starting_time = starting_time
        self.ending_time = ending_time


    def dataloader(self):
        """
        What this function does:
        1. Read the path list we want to get the date from.
        2. Read the starting and ending time we are interested in.
        3. Load the corresponding database and put it into a pd.dataframe
        """
