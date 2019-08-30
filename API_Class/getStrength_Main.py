# -*- coding: utf-8 -*-
'''
Created on 2019年5月9日
通过接口，计算前一天所有电站强度
可用作定时任务
@author: Administrator
'''
from getAPI import getStaWork
import pandas as pd
from mkdir import mkdir
import time
from multiprocessing import Process
import datetime
import os
import sys
import traceback

os.chdir(sys.path[0])

now = datetime.datetime.today()
#"2019-04-19 00:00:00"
sta_data_date=(now+ datetime.timedelta(days = -1)).strftime("%Y-%m-%d")+" 00:00:00"
#"2019-04-20 00:00:00"
end_data_date=now.strftime("%Y-%m-%d")+" 00:00:00"

#根据参数范围查询遍历不同的电站
def getStackInfo(i,sta_codes,ah_stas,sta_names):
    print('***********正在计算“'+sta_names[i]+'”强度中***********')
    obj=getStaWork(sta_data_date, end_data_date,sta_code=sta_codes[i])
    rs_en=obj.getDataByBAS()
    rs_ah=obj.getDataByCLS(ah_sta=ah_stas[i])
    if rs_ah is not False and rs_en is not False and rs_en.ix[0,2]>0:
        #确保目录存在
        mkdir('./strengths/'+sta_names[i])
        write=pd.ExcelWriter(r'./strengths/'+sta_names[i]+'/'+sta_data_date.split(' ')[0]+'强度数据统计.xlsx')
        rs_ah.to_excel(write,'簇累计充放容量数据')
        rs_en.to_excel(write,'堆累计充放电量数据')
        write.save()
        print('√√√√√√√√√√√√ data is saved! √√√√√√√√√√√√')

def multiprocess(m,n,sta_codes,ah_stas,sta_names):
    ps=[]
#     lock = Lock()
    for i in range(m,n):
        p = Process(target=getStackInfo,args=(i,sta_codes,ah_stas,sta_names))
        ps.append(p)
    for i in range(len(ps)):  
        ps[i].start()#每个进程还是会执行函数外的变量，加上本身一次，总共4次     
    for i in range(len(ps)):  
        ps[i].join()

if __name__ == '__main__':
    try:
        start=time.time()
    #     obj=getStaWork(sta_data_date, end_data_date,sta_code='0005')
    #     all_sta_codes=obj.getAllStaCode()
    #     df=pd.DataFrame(all_sta_codes)
    #     df.to_excel('./assets/sta_cl_ah_config.xlsx')
        sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
        sta_codes,sta_names,ah_stas=sta_config['code'],sta_config['name'],sta_config['capacity']
        for i in range(0,len(sta_codes),5):
            multiprocess(i,min(i+5,len(sta_codes)),sta_codes,ah_stas,sta_names)
        end=time.time()
        infos="\n"+sta_data_date.split(' ')[0]+'查询强度数据代码运行成功！总计耗时：'+str(end-start)+"秒"
    except Exception as e:
        print(e)
        traceback.print_exc()
        infos="\n"+sta_data_date.split(' ')[0]+"查询强度数据代码运行失败！"
    with open(r'C:\Users\Administrator\Desktop\自动运行文件日志.log', 'a') as f:
        f.write(infos) 
