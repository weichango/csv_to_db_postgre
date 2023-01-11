# import libraries

import os
import numpy as np
import pandas as pd
import psycopg2
import shutil

# main
from csv_import_functions import *

#settings
dataset_dir = 'datasets'

csv_files = csv_files()
configure_datset_directory(csv_files, dataset_dir)
df = create_df(dataset_dir, csv_files)


# open database connection
database = "postgres"
user = "weidb"
password = "7b3tdKEkpVNP9gP"
host = "database-1.c4xtj4naopjn.us-east-1.rds.amazonaws.com"
port = '5432'

for k in csv_files:
    # call dataframe
    dataframe = df[k]
    
    # clean table name
    tbl_name = clean_tbl_name(k)

    # clean column names
    col_str, dataframe.columns = clean_colname(dataframe)
    
    upload_to_db(host, 
                database, 
                user, 
                password, 
                port, 
                tbl_name, 
                col_str, 
                file = k, 
                dataframe = dataframe, 
                dataframe_columns = dataframe.columns)