#재무제표 넓게 가져와서 자르기
import pandas as pd
import glob
import datetime

files = glob.glob('*.csv')
for file in files:
    df = pd.read_csv(file, header=None)


    df2 = df.replace(' \[*','' , regex=True )
    df2 = df2.replace('\]','' , regex=True )
    df2 = df2.T
    df2.columns = df2.loc[0,:]

    df3 = df2.apply(pd.to_numeric, errors= 'coerce')
    df3 = df3[df3.iloc[:,0]>20160218]
    #df4 = pd.concat((df2_col.T, df3), axis=0)
    df3.columns = df2.columns
    #print(df3.head)
    df3.to_csv('C:/stockauto/보조변환/%s' % file, index=False, encoding='UTF-8-sig')
    
'''
    df2.columns = df2.loc[0,:]
    df3 = df2.iloc[1:,0].reset_index(drop=True) #date
    df5 = df2.loc[1:, df2.columns != 'Date']
    df5 = df5.loc[::-1].reset_index(drop=True)

    df6 = pd.concat([df3,df5],axis=1)
    print(df2.columns)
    df6.columns = df2.columns
'''


