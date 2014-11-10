"""Parse a folder of .dat files recorded using the Labjack LJStreamUD
National Instruments program and return a Pandas DataFrame.
See http://labjack.com/support/ud/ljstreamud for details.

Use:
>> df = lj_dataframe(folder="data_folder",
                     column_names=("a","b","y1","y2")

Copyright (c) 2014 Colin Dietrich (code@wildjuniper.com)
"""


import os
import time

import numpy as np
import pandas as pd


def walk_dir(root_folder):
    """Walks through a directory and compiles a numpy array of the data.

    Parameters
    ----------
    root_folder : string, absolute path to archive

    Returns
    -------
    full_file_list : List of absolute paths to all files in directory
    full_dir_list :  List of absolute paths to all directories in directory
    """

    rf = os.path.normpath(root_folder)

    try:
        # walk through the directory and find all the files and directories
        root_folder, directories, file_list = os.walk(rf).next()
    except ValueError:
        return False, 'Unable to walk directory'

    # sorting gets the .dat files as recorded in time series order
    file_list.sort()
    directories.sort()

    # walk through each file
    full_file_list = [root_folder + os.path.sep + one_file for one_file in file_list]

    # walk through each folder
    full_dir_list = [root_folder + os.path.sep + one_folder for one_folder in directories]

    return full_file_list, full_dir_list


def read_specific_lines(file_object, indexes_to_read):
    """Read specific lines from a file.

    Parameters
    ----------
    file_object : any iterable
    lines_to_read : list, containing int values of indexes to return

    Returns
    -------
    Generator of values at indexes

    """

    lines = set(indexes_to_read)
    last = max(lines)
    for n, line in enumerate(file_object):
        if n in lines:
            yield line
        if n > last:
            return


def header_date_time(labjack_daq_file):
    """Converts the first two lines of a Labjack NI Stream data file
    to seconds since the Unix epoch time such as:
    12/9/2013
    7:28:06 PM

    Converts to:
    1386646086.0

    Parameters
    ----------
    labjack_daq_file : str, path to Labjack created .dat data file

    Returns
    -------
    t : float64, unix time of beginning of data recording

    """
    with open(labjack_daq_file, 'r') as f:
        l_12 = [s for s in read_specific_lines(f, [0, 1])]

    t = time.strptime(l_12[0].strip() + l_12[1].strip(), '%m/%d/%Y%I:%M:%S %p')
    t = np.float64(time.mktime(t))

    return t


def lj_dataframe(folder, col_names=None):
    """Combine Labjack data into one data frame.

    Labjack appends an incrementer suffix as data is collected, however it is
    not zero padded.  If necessary, files are zero padded and then sorted
    by file name which creates a chronologically correct file order for
    importing to a data frame.

    Parameters
    ----------
    folder : str, absolute file paths to folder containing files from one
        data collection session
    col_names : list, strings for labeling data columns
    Returns
    -------
    data_frame : pandas data frame of data, labeled with col_names

    """

    # get a list of all files
    file_list, dir_list = walk_dir(folder)

    # default column names
    if col_names is None:
        col_names = ['a', 'b', 'c', 'd', 'e', 'f',
                     'y0', 'y1', 'y2', 'y3', 'y4', 'y5']
    
    col_names = ['time'] + col_names

    # access the first file
    df = pd.read_csv(filepath_or_buffer=file_list[0],
                     sep='\t',
                     skiprows=12,
                     names=col_names)

    # append each sequential data file into one dataframe
    for file_name in file_list[1:]:
        df_new = pd.read_csv(filepath_or_buffer=file_name,
                             sep='\t',
                             skiprows=12,
                             names=col_names)
        df = df.append(df_new)

    # sort all rows based on column 0 as Time
    df = df.sort(columns='time', axis=0)

    # add unix time column
    t0 = header_date_time(file_list[0])
    df['time_unix'] = df.iloc[:, 1] + t0

    return df
