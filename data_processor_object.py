# %%-- import the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
# %%-


class module_data_processor:
    """
    This is an object that reads and process the module data from an access databse file.
    """


    def __init__(self, path, starting_date, ending_date):
        """
        1. Read the dataset within the given period.
        """

        # read the access file with the given path
