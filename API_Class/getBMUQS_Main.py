# -*- coding: utf-8 -*-
'''
Created on 2019年5月9日

@author: Administrator
'''
from getAPI import getStaWork
import pandas as pd
from mkdir import mkdir
from multiprocessing import Process

def getStackInfo(i,sta_codes,sta_names,sta_data_date,end_data_date):
    obj=getStaWork(sta_data_date, end_data_date,sta_code=sta_codes[i])
    #6-3-3 3pack sbox=5,ebox=6,sba=2,eba=3,scl=2,ecl=3,sp=2,ep=3
    rs=obj.getCellDataByPack()
    if len(rs)>0:
        df_rs=pd.DataFrame(rs)
        df_rs=pd.concat([pd.DataFrame([[sta_data_date,end_data_date]]),df_rs])
        df_rs.columns=['描述','电压']
        mkdir('./BMUQS/')
        df_rs.to_excel('./BMUQS/'+sta_data_date.split(' ')[0]+sta_names[i]+'.xlsx',index=False)
        print('发现如下问题[描述，12s电压]，已存入BMUQS目录下')
    else:
        print('所选时间段内没有发现任何问题')

def multiprocess(m,n,sta_codes,sta_names,sta_data_date,end_data_date):
    ps=[]
    for i in range(m,n):
        p = Process(target=getStackInfo,args=(i,sta_codes,sta_names,sta_data_date,end_data_date))
        ps.append(p)
    for i in range(len(ps)):  
        ps[i].start()
    for i in range(len(ps)):  
        ps[i].join()

if __name__ == '__main__':
    sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
    sta_codes,sta_names=sta_config['code'],sta_config['name']
    #海丰
    sta_data_date="2019-05-03 00:00:00"
    end_data_date="2019-05-04 11:30:00"
    multiprocess(5,6,sta_codes,sta_names,sta_data_date,end_data_date)
    #新丰，云河，准大
#     sta_data_date="2019-05-03 00:00:00"
#     end_data_date="2019-05-04 11:30:00"
#     multiprocess(0,4,sta_codes,sta_names,sta_data_date,end_data_date)
