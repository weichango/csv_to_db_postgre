# %% [markdown]
# ## Import CSV file into a postgres database
# ### Steps
# * Import CSV file into pandas df
# * clean table name and remove all extra symbols, spaces, and capital leters
# * clean the column headers and remove all extra symbols, spaces, and captial letters
# * write the create table SQL statement
# * import the data into the db
# 
# reference: https://www.youtube.com/watch?v=wqBFgaMgFQA&t=0s

# %%
# import libraries

import os
import numpy as np
import pandas as pd
import psycopg2
import shutil

# %% [markdown]
# ## Find CSV files in directory

# %%
# find CSV files in current working directory
# isolate only the CSV files

def csv_files ():
    csv_files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith('csv'):
            csv_files.append(file)
    return csv_files       

# %%
# make a new directory
# move CSV files in the new directory

def configure_datset_directory(csv_files, dataset_dir):
    
    #make a new directory
    try:
        mkdir = 'mkdir {0}'.format(dataset_dir)
        os.system(mkdir)
    except:
        pass

    #mv filename directory
    for csv in csv_files:
        mv_file = "{0}/{1}".format(dataset_dir, csv)
        shutil.move(csv, mv_file)
        print(mv_file)
    
    return 

# %% [markdown]
# ## Create the pandas df from the CSV file

# %%
def create_df (dataset_dir, csv_files):
    
    # path to the csv file
    data_path =  os.getcwd()+'/' + dataset_dir + '/'

    # loop through the files and create the dataframe
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(data_path+file)
        except UnicodeDecodeError:
            df[file] = pd.read_csv(data_path+file, encoding = "ISO-8859-1")
    
    return df

# %% [markdown]
# #### Clean table names and column names

# %%
def upload_to_db(host, database, user, password, port, tbl_name, col_str, file, dataframe, dataframe_columns):
    # open database connection
    engine = psycopg2.connect(
        database = database ,
        user = user,
        password = password,
        host = host,
        port = port)
    cursor = engine.cursor()
    print('opened database successfully')

    # drop table with the same name
    cursor.execute("drop table if exists %s;" %(tbl_name))

    # create table
    cursor.execute("CREATE TABLE %s (%s);" %(tbl_name, col_str))

    #insert value to table

    #save df to csv file
    dataframe.to_csv(file, header = dataframe_columns, index = False, encoding = 'utf-8')

    #open the csv file, save it as an object 
    my_file = open(file)
    print('file open in memory')

    # upload to db
    SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """
    cursor.copy_expert(sql = SQL_STATEMENT % tbl_name, file=my_file)
    print("file copied to db")

    cursor.execute("grant select on table %s to public" % tbl_name)
    engine.commit()

    cursor.close()
    print('table {0} imported to db completed'.format(tbl_name))

    return


