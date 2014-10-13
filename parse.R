# Parse a folder of .dat files recorded using the Labjack LJStreamUD
# National Instruments program and return a Pandas DataFrame.
# See http://labjack.com/support/ud/ljstreamud for details.
# 
# Use:
# >> df = parse(folder='data_folder',
#                      column_names=('a', 'b', 'y1', 'y2')
# 
# Copyright (c) 2014 Colin Dietrich (code@wildjuniper.com)


parse = function(folder, extension = '.dat', 
                 default_header = FALSE,
                 channel_names = FALSE,
                 transform_names = FALSE){
    
#     Parameters
#     ----------
#     folder : character, path to folder containing files
#     extension : character, file extension.  defaults to '.dat'
#     default_header : boolean, use default header in data file
#     channel_names : boolean or character, column names if default_header
#         is FALSE. Defaults to alphabetical characters.
#     transform_names : boolean or character, column names if default_header
#         is FALSE. Defaults to 'yN' characters.
#     
#     Returns
#     -------
#     data.frame : merged data sorted by time axis in first column
    
    # list all files in folder
    files = list.files(path=folder, pattern=paste0('*', extension))
    files = paste0(folder, files)
    
    # load the first file once to get the start time
    df = read.csv(file=files[1], header=FALSE, sep='\t')
    
    # convert the first two rows to POSIXlt
    t0 = strptime(paste0(df[1,1], '_', df[2,1]), format='%m/%d/%Y_%I:%M:%S %p')
    
    # specific parameters for Labjack data files
    rcsv = function(f) {
        # import .dat file as saved by LabjackSteamUD
        return(read.csv(file=f,
                        sep='\t',
                        skip=11))
    }
    
    # load the first file a second time to append rows to
    df = rcsv(f=files[1])
    
    # row bind each data file to the data.frame
    for (m in 2:length(files)){
        df = rbind(df, rcsv(files[m]))
    }
    
    # because the files might not be ordered sequential by file name
    # due to non-zero padding file name creation,
    # sort the complete data.frame by the time value
    df = df[order(df$Time), ]
    
    # rename columns if there were custom names given
    if (default_header == FALSE){
        
        col_names = c('Time')
        
        if (channel_names == FALSE){
            channel_names = c('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 
                              'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p')
        }
        if (transform_names == FALSE){
            transform_names = c('y0', 'y1', 'y2', 'y3', 'y4', 'y5', 'y6', 'y7', 'y8',
                                'y9', 'y10', 'y11', 'y12', 'y13', 'y14', 'y15', 'y16')
        }
        
        n = (length(df[1,])-1)/2
        col_names = append(col_names, channel_names[1:n])
        col_names = append(col_names, transform_names[1:n])
        names(df) = col_names
    }
    
    df['time.POSIX'] = df$Time + t0

    return(df)
}
