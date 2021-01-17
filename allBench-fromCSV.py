import pandas as pd
import pyarrow
from time import time
import os

beginTime = time()

df_tryNum = input("\nInput attempt ID here: ")  # Unique identifier for this test run, will be put at beginning of all files and will name directory
df_fileprefix = 'compressionBenchmark.'  # Central name for all files produced during benchmark


df_source = ['compressTest1week.csv']
extensionList = ['csv', 'parquet', 'pickle', 'hdf5']

savetime = time()
print("\nBeginning benchmark, time is: " + str(time()))



# Now, write this df to one of each format type

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
        df_in.to_hdf(filename, filename[:-4])
    
    print(extension + ' file written, total write time was ' + str(time()-savetime))

# Function takes one of the four file extensions, finds a file from the previous function with that extension and reads it into df
def read_bench(extension):
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

    print(extension + ' file read to dataframe, total read time was ' + str(time()-savetime))

    
    
# This takes a csv dataframe and a list of extensions, makes a folder and benchmarks all file types for that database.
for dataFile in df_source:
    currentDF = pd.read_csv(dataFile, low_memory=False)
    os.mkdir(df_tryNum + dataFile[:-4]) # Make folder for all files based on name of source df
    os.chdir(df_tryNum + dataFile[:-4])
    print('\nDataframe and directory made, write test beginning.')

    for fileType in extensionList:
        write_bench(currentDF, fileType)
        read_bench(fileType)

print("\n------------------------------------------\nBenchmark complete, total runtime was " + str(time()-beginTime()) + " seconds.\n------------------------------------------\n")


"""
#Write csv
print('\nWriting csv...')
savetime = time()
df.to_csv(df_fileprefix + '.csv')
print('csv written, write time was ' + str(time()-savetime))

# Write parquet
print('\nWriting parqet...')
savetime = time()
df.to_parquet(df_fileprefix + '.parquet')
print("Parquet written, write time was: " + str(time()-savetime))

# Write Pickle
print('\nWriting parqet...')
savetime = time()
df.to_parquet(df_fileprefix + '.parquet')
print("Pickle written, write time was: " + str(time()-savetime))





# Writing dataframe to csv format file
print("\nPandas dataframe complete, writing to csv file. Time is: " + str(time.time()))
savetime = time.time()
df.to_csv(df_filename)
print("\nCSV file complete. File write time: " + str(time.time() - savetime))


# Now read the file from parquet for the final benchmark.
print("\nCSV file located, beginning read to pd dataframe. Time is: " + str(time.time()))
savetime = time.time()
df_read = pd.read_csv(df_filename)
print("\nCSV read and loaded into pandas df. Load and read time: " + str(time.time() - savetime))
print("\n------------------------------------------\n Benchmark complete. Total runtime:" + str(time.time() - beginTime) + "\n------------------------------------------\n")


# Test dataframe accuracy 
print(df_read)
"""
