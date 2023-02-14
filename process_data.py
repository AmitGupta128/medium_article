#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:39:08 2022

@author: amitgupta
"""


import pandas as pd
import csv
import gzip


def process_dataset(name,column_index,skip_rows_start=0,skip_rows_end=0):
    '''
    

    Parameters
    ----------
    name : str
        Name of the .dat.gz file with extension included.
    column_index : int
        Index/Position of the column "Number_of_Bundles".
    skip_rows_start : int, optional
        The number of rows to skip from the start. The default is 0.
    skip_rows_end : int, optional
        The number of rows to skip from the end. The default is 0.

    Returns
    -------
    df_final: dataframe
        Dataframe with the final values

    '''
    try:
        # Opening the file into dataframe
        df_table = pd.read_table(name,compression='gzip',header=None)
        df = df_table[0].str.split(',',expand=True)
        
        #Concatenating the data after the column_index provided
        # Storing into a series format
        df_series = df[list(df.columns)[column_index:]].apply(lambda x: ','.join(x.dropna()),axis=1)
        
        # Renaming the series data with column_index + 1 value so that there is no discrepancy when concatenating
        df_series.rename(column_index,inplace=True)
        # Joining the original dataframe and the series data
        df_final = pd.concat([df.iloc[:,:column_index],df_series],axis=1)

        # Skipping rows from the dataframe
        rows_to_skip = []
        rows_to_skip_from_front = []
        rows_to_skip_from_back = []
        if(skip_rows_start==0 and skip_rows_end==0):
            print('Nothing to be skipped')
            pass
        
        # Front part to be skipped but not the last part
        elif(skip_rows_start!=0 and skip_rows_end==0):
            rows_to_skip_from_front = [i for i in range(skip_rows_start)]
            rows_to_skip.extend(rows_to_skip_from_front)
            
        # Last part to be skipped irrespective of front part
        elif(skip_rows_end!=0):
            rows_to_skip_from_front = [i for i in range(skip_rows_start)]
            rows_to_skip_from_back = [i for i in range(len(df_final)-1,len(df_final)-1-skip_rows_end,-1)]
            rows_to_skip = rows_to_skip_from_front + rows_to_skip_from_back
        
        # Removing the skipped rows from the final dataframe
        df_final.drop(rows_to_skip,inplace=True)
        return df_final
    except Exception as e:
        print("Exception encountered:{}".format(e))
    
    


dfmain = process_dataset("testing1.dat.gz",4,1,1)
# dfmain.to_csv(name,index=None,header=None,compression='gzip',quoting=csv.QUOTE_MINIMAL)