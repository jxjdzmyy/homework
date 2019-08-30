# -*- coding: utf-8 -*-
'''
Created on 2019年5月9日

@author: Administrator
'''
from getAPI import getStaWork

sta_data_date="2019-02-01 00:00:00"
end_data_date="2019-04-20 00:00:00"

if __name__ == '__main__':
    obj=getStaWork(sta_data_date, end_data_date,sta_code='0005')
    rs_curs=obj.getDataByCLS(quickly=False,sbox=1,ebox=2,sba=1,eba=2,scl=0,ecl=2,mode='getCurs')
    rs_curs.columns=['时间','2箱2堆1簇','2箱2堆2簇']
    print(rs_curs)
    rs_curs.to_csv('电流数据2-2-1/2.csv')