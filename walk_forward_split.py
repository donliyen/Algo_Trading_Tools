# %%
import pandas as pd

def wfa_split(df,n,test_size,anchored = False ):
    
    wfa_df_train = []
    wfa_df_test = []


    test_len = int( len( df ) / ( 1 / test_size + n )  )
    train_len =  int( test_len / test_size )


    for m in range( 0 , n ):

        
        t1 = test_len * m
        t2 = ( t1 ) + train_len 
        t3 = t2 
        t4 = t3 + test_len

        if anchored :
            sp_df_1 = df.iloc[ 0 : t2 ]
        else:
            sp_df_1 = df.iloc[ t1 : t2 ]
            
        sp_df_2 = df.iloc[ t3 : t4 ]
        
        wfa_df_train.append( sp_df_1 )
        wfa_df_test.append( sp_df_2 )
        
    return [ wfa_df_train , wfa_df_test ]



