import pandas as pd
import pyarrow
from time import time
import os

#---------------------------------------------------------------------------------------------------------------------
# MAJOR TODO: Implement df typing to improve performance and prevent errors with 2 and 4 week versions of the dataset
#---------------------------------------------------------------------------------------------------------------------

experiment_runs = 3  # Number of times to repeat the test

df_fileprefix = 'compBench.'  # Central name for all files produced during benchmark, includes '.' for later attachment to extensions in extensionList

df_source = ['1week.csv'] # List of dataframes to run analysis on, will help name folder

extensionList = ['csv', 'parquet', 'pickle', 'hdf5'] # List of file extension formats to run the benchmark on. Works with csv, parquet, pickle, and hdf5

df_tryNum = str(round(time()))
os.mkdir(df_tryNum + 'masterBench')   # Make and nav to dir for all dirs/files made during this bench
os.chdir(df_tryNum + 'masterBench')

#report_file = open('../../' + df_tryNum + 'benchReport.txt')  # Txt at original location will hold all info from all file types and sizes. 

for datafile in df_source:  
    for extension in extensionList:
        exec('w_' + extension + datafile[:-4] + ' = []')  # Make empty list for every combo of dataset, file type and measurement type (read/write)
        exec('r_' + extension + datafile[:-4] + ' = []')
        exec('writeFile_' + extension + datafile[:-4] + ' = open("../w_' + extension + datafile[:-4] + '.txt", "a")')  # Make/open file for every dataset/file type combo
        exec('readFile_' + extension + datafile[:-4] + ' = open("../r_' + extension + datafile[:-4] + '.txt", "a")')  # File will be shared with other day's scripts


# Function takes a csv dataframe and the desired extention as input, and benchmarks writing a file of that extension from the given dataframe
# Possible extensions are the strings 'csv', 'parquet', 'pickle', and 'hdf5'
def write_bench(df_in, extension):
    print('\nWriting ' + extension + '...')
    filename = df_fileprefix + extension
    savetime = time()

    # TODO: Find way to change function by extension string, rather than have this mess of if and elif statements.
    if extension == 'csv':
        df_in.to_csv(filename)
    elif extension == 'parquet':
        df_in.to_parquet(filename)
    elif extension == 'pickle':
        df_in.to_pickle(filename)
    elif extension == 'hdf5':
        df_in.to_hdf(filename, filename[:-4])   # TODO: Implement proper typing of dataset to improve overrall performance.
    
    return time()-savetime

# Function takes one of the four file extensions, finds a file from the previous function with that extension and reads it into df
def read_bench(df_in, extension):
    print('\nReading ' + extension + ' file...')
    filename = df_fileprefix + extension
    savetime = time()

    # TODO: Find way to change function by extension string, rather than have this mess of if and elif statements.
    if extension == 'csv':
        df = pd.read_csv(filename)
    elif extension == 'parquet':
        df = pd.read_parquet(filename)
    elif extension == 'pickle':
        df = pd.read_pickle(filename)
    elif extension == 'hdf5':
        df = pd.read_hdf(filename)

    return time()-savetime

    
for i in range(experiment_runs):
    df_tryNum = str(round(time()))
    os.mkdir(df_tryNum + 'compBench')   # Make and nav to dir for all dirs/files made during this bench
    os.chdir(df_tryNum + 'compBench')

    # This takes a csv dataframe and a list of extensions, makes a folder and benchmarks all file types for that database.
    for dataFile in df_source:
        currentDF = pd.read_csv('../../' + dataFile, dtype=object, low_memory=False)
        os.mkdir(df_tryNum + dataFile[:-4]) # Make folder for all files based on name of source df
        os.chdir(df_tryNum + dataFile[:-4])
        print('\nDataframe and directory made, write test beginning.')

        for fileType in extensionList:
            currentWrite = write_bench(currentDF, fileType)
            currentRead = read_bench(currentDF, fileType)
            exec('w_' + fileType + datafile[:-4] + '.append(' + str(currentWrite) + ')') # Add read/write times to respective lists
            exec('r_' + fileType + datafile[:-4] + '.append(' + str(currentRead) + ')')

        os.chdir('..')

    os.chdir('..')

for datafile in df_source:  
    for extension in extensionList:
        dList = []

        exec('dList = w_' + extension + datafile[:-4])  # Set temp list var to list of data for specific combo of ext and df
        dAverage = (sum(dList))/len(dList)
        exec('writeFile_' + extension + datafile[:-4] + '.write(" ' + str(dAverage) + '")')
        exec('writeFile_' + extension + datafile[:-4] + '.close()')

        exec('dList = r_' + extension + datafile[:-4])  # Repeat for read data
        dAverage = (sum(dList))/len(dList)
        exec('readFile_' + extension + datafile[:-4] + '.write(" ' + str(dAverage) + '")')
        exec('readFile_' + extension + datafile[:-4] + '.close()')




"""
eval('w_' + extension + datafile[:-4] + ' = []')  # Make empty list for every combo of dataset, file type and measurement type (read/write)
eval('r_' + extension + datafile[:-4] + ' = []')
"""