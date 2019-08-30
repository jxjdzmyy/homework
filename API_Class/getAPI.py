# -*- coding: utf-8 -*-
'''
Created on 2019年5月9日
从云的api获取每个电站的工作强度
1.获取每簇的每日的累计充放容量
2.获取每堆的累计充放电量
3.接口已开发
@author: 逻辑的使命
'''
import suds
import time
import datetime
import json
import numpy as np
from suds.client import Client
import pandas as pd
import os
import sys

os.chdir(sys.path[0])

sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
sta_codes,sta_names,ah_stas=sta_config['code'],sta_config['name'],sta_config['capacity']

class getStaWork(object):
    '''
        webservice接口函数使用类大全
    '''
    sta_data_date="2019-05-04 00:00:00"
    end_data_date="2019-05-04 15:00:00"
    sta_code="0010"#电站编号：海丰
    ratio=0.9#计算等效循环时系数
    url = "http://10.13.3.2:7031/DmsService?wsdl"
    client = suds.client.Client(url)
    clouId="CNSYB1"
    nowTime = int(round(time.time()*1000))
    code=clouId+str(nowTime)
    #接口编码，分别对应电池堆集合档案、电池堆编码结构、电池簇数据统计、电池簇综合曲线簇、电芯数据统计、电池簇综合曲线电芯接口,簇数据，堆数据
    methods=["100001","100002",'100003',"100004",'100005','100006','100009','100011','100013']
    #箱子总数量，堆编码列表,每个模组中有多少Bmu,每个簇中有多少模组,每个Bmu中有多少cell
    box_num,bmsCodes,pack_bmu_num,pack_num,cell_num=0,0,0,0,0
    ### 
    def comvs(self,*agrs):
        if len(agrs)>=2:
            flag=True
            for i in range(0,len(agrs)-1):
                if abs(agrs[i]-agrs[i+1])<=0.003:
    #                 print([agrs[i],agrs[i+1]])
                    continue
                else:
                    flag=False
                    break
            return flag
        else:
            print('输出参数至少为两个')
    #根据条数，划分时间得序列，有些最大支持200，有些支持5000
    #ds=[]#存放时间范围，xx-xx-xx 00:00:00,xx-xx-xx 12:00:00
    def getDTs(self,max=5000):
        if max==5000:
            freq='12H'
        elif max==200:
            freq='0.5H'
        else:
            freq='1H'
        ds=pd.date_range(self.sta_data_date, self.end_data_date, freq=freq).astype('str')
        if len(ds)==1:
#             help(ds)
#             ds.iloc[0]=self.sta_data_date
            ds=ds.delete(0)
            ds=ds.insert(0, self.sta_data_date)
        if ds[-1]!=self.end_data_date:
            ds=ds.insert(len(ds), self.end_data_date)
        return ds
    #100002 获取某箱某堆的所有簇编码
    def getClusterCodes(self,box,ba):
        bmsCode=self.bmsCodes[box][ba]
        strJson=str({'bms_code':bmsCode})
        clusterJson=json.loads(self.client.service.getDoc(self.code,self.methods[1],strJson))
        try:
            #注有些电站返回不合理，没有相关字段，比如天长市美好乡村智能微电网
            #查询一个包中有多少个bmu,同一电站的包bmu数量是一样的，故只取一个即可
            self.pack_bmu_num=clusterJson["data"][0]['clusters_data'][0]['packs_data'][0]['pack_bmu_num']
            self.pack_num=clusterJson["data"][0]['clusters_data'][0]['pack_num']
            self.cell_num=clusterJson["data"][0]['clusters_data'][0]['packs_data'][0]['bmus_data'][0]['cell_num']
            clusterCodes=[it['cluster_code'] for it in clusterJson['data'][0]['clusters_data']]
            return clusterCodes
        except:
            return False
#         for i in range(len(ds)-1):
#             print([ds[i],ds[i+1]])
    #使用100011接口(按簇取，不需要bmu编号)，获取各个簇详细数据，以bmu为最小单元，获取时间段内所有数据
    def getBMUDataByClS(self):
        pageSize=200
        ds=self.getDTs(pageSize)
        allrs=[]
        for box in range(self.box_num):
            for ba in range(len(self.bmsCodes[box])):
                clusterCodes=self.getClusterCodes(box,ba)
                if clusterCodes is False:
                    return False
                for cluster_code in clusterCodes:
                    rs=[]
                    for i in range(len(ds)-1):
                        sta_data_date=ds[i]
                        end_data_date=ds[i+1]
                        dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "cluster_code": cluster_code, 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                        }
                        try:
                            result=json.loads(self.client.service.getData(self.code,self.methods[7],str(dataJson)))
                        except:
                            time.sleep(6)
                            result=json.loads(self.client.service.getData(self.code,self.methods[7],str(dataJson)))
                        rs.append(result)
                    allrs.append(rs)
        return allrs
    #获取堆的数据100013接口,某箱某堆,quickly快捷版不需要任何中间数据
    def getDataByBAS(self,sbox=0,ebox=-1,sba=0,eba=-1,quickly=True):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_c_ens=[]#累计充电量kWh
        all_d_ens=[]#累计放电量kWh
        vols=[]#堆电压
        curs=[]#堆电流
        pws=[]#堆总功率
        socs=[]#堆soc
        data_date=[]
        rs=[]#返回计算后的结果
        Flag=False#记录是否查询所有
        for box in range(sbox,ebox):
            if eba==-1:
                Flag=True
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                print('-------正在处理第'+str(box+1)+"箱第"+str(ba+1)+"堆的数据----------")
                temp_c_ens,temp_d_ens,temp_vols,temp_curs,temp_pws,temp_socs=[],[],[],[],[],[]
                j=0
                if quickly:
                    rge=[0,len(ds)-2]
                else:
                    rge=range(len(ds)-1)
                for i in rge:
                    sta_data_date=ds[i]
                    end_data_date=ds[i+1]
                    print("处理"+sta_data_date)
                    dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                    }
                    try:
                        result=json.loads(self.client.service.getData(self.code,self.methods[8],str(dataJson)))
                    except:
                        time.sleep(6)
                        result=json.loads(self.client.service.getData(self.code,self.methods[8],str(dataJson)))
    #               print(result)
                    #完全版，False
                    if quickly==False:
                        if(result['jszt']=='0' and len(result['data'])>0):
                            temp_c_ens=np.concatenate((temp_c_ens,[it['all_inenergy'] for it in result['data']]))
                            temp_d_ens=np.concatenate((temp_d_ens,[it['all_outenergy'] for it in result['data']]))
                            temp_vols=np.concatenate((temp_vols,[it['voltage'] for it in result['data']]))
                            temp_curs=np.concatenate((temp_curs,[it['current'] for it in result['data']]))
                            temp_pws=np.concatenate((temp_pws,[it['power'] for it in result['data']]))
                            temp_socs=np.concatenate((temp_pws,[it['soc'] for it in result['data']]))
                    #快捷版,只处理最开始第一个与结束最后一个
                    else:
#                         print(result)
                        if(result['jszt']=='0' and len(result['data'])>0):
                            temp_c_ens.append([it['all_inenergy'] for it in result['data']][j])
                            temp_d_ens.append([it['all_outenergy'] for it in result['data']][j])
                        else:
                            temp_c_ens.append(0)
                            temp_d_ens.append(0)
                        j=j-1
                if quickly==False:
                    all_c_ens.append(temp_c_ens)
                    all_d_ens.append(temp_d_ens)
                    vols.append(temp_vols)
                    curs.append(temp_curs)
                    pws.append(temp_pws)
                    socs.append(temp_socs)
    #                 print(all_c_ens)
                if len(temp_d_ens)>1 and len(temp_c_ens)>1:
                    #快捷版可能存在两段时间内没有数据，从而进入这里
                    if temp_c_ens[0]==0:
                        print('注：开始时间段内数据缺失，计算暂用0代替') 
                    elif temp_c_ens[-1]==0:
                        print('注：截止时间段内数据缺失，计算暂用0代替')                       
                    #需要计算其累计电量的差值
                    print(['截止累计充放电量',[temp_c_ens[-1],temp_d_ens[-1]]])
                    dc=temp_c_ens[-1]-temp_c_ens[0]
                    dd=temp_d_ens[-1]-temp_d_ens[0]
                    rs.append([dc,dd])
                else:
                    print('选择时间范围内数据完全缺失，请重试！')
            if Flag:
                #如果是查询所有则恢复设置成-1
                eba=-1
        if len(rs)==0:
            return False
        df_rs=pd.DataFrame(rs)
        sum_c_en,sum_d_en=df_rs.sum()
        df_rs=pd.concat([df_rs,pd.DataFrame([sum_d_en])],axis=1)
        df_rs.columns=['各堆累计充电量/kWh','各堆累计放电量/kWh','总计累计放电量/kWh']
#         print(df_rs)
        return df_rs
    #获取簇数据，接口100004，单元为簇
    def getDataByCLS(self,ah_sta=120,sbox=0,ebox=-1,sba=0,eba=-1,scl=0,ecl=-1,quickly=True,mode='getStrenths'):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_c_ah,all_d_ah,all_vols,all_curs,all_socs,all_warn,all_prot=[],[],[],[],[],[],[]
        data_date=[]
        rs=[]#返回最后结果
        Flag=False
        for box in range(sbox,ebox):
            if eba==-1:
                Flag=True
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                clusterCodes=self.getClusterCodes(box,ba)
                if clusterCodes is False:
                    return False
                if ecl!=-1 and ecl>scl>=0:
                    clusterCodes=clusterCodes[scl:ecl]
                for cluster_code in clusterCodes:
                    print('-------正在处理第'+cluster_code+"的数据----------")
                    #累计充电安时，放电安时，簇电压，簇电流，簇soc,告警状态字，保护状态字
                    temp_c_ah,temp_d_ah,temp_vols,temp_curs,temp_socs,temp_warn,temp_prot=[],[],[],[],[],[],[]
                    temp_data_date=[]
                    j=0
                    if quickly:
                        rge=[0,len(ds)-2]
                    else:
                        rge=range(len(ds)-1)

                    for i in rge:
                        sta_data_date=ds[i]
                        end_data_date=ds[i+1]
                        print("处理中"+sta_data_date)
                        dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "cluster_code": cluster_code, 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                        }
                        try:
                            result=json.loads(self.client.service.getData(self.code,self.methods[3],str(dataJson)))
                        except:
                            time.sleep(6)
                            result=json.loads(self.client.service.getData(self.code,self.methods[3],str(dataJson)))
#                         print(result)                                            
                        #完全版，False
                        if quickly==False:
                            if(result['jszt']=='0' and len(result['data'])>0):
                                temp_c_ah=np.concatenate((temp_c_ah,[it['charge_ah'] for it in result['data']]))
                                temp_d_ah=np.concatenate((temp_d_ah,[it['discharge_ah'] for it in result['data']]))
                                temp_vols=np.concatenate((temp_vols,[it['voltage'] for it in result['data']]))
                                temp_curs=np.concatenate((temp_curs,[it['current'] for it in result['data']]))
                                temp_socs=np.concatenate((temp_socs,[it['soc'] for it in result['data']]))
                                temp_warn=np.concatenate((temp_warn,[it['warn_st'] for it in result['data']]))
                                temp_prot=np.concatenate((temp_prot,[it['prot_st'] for it in result['data']]))
                                temp_data_date=np.concatenate((temp_data_date,[it['data_date'] for it in result['data']]))
                        #快捷版,只处理最开始第一个与结束最后一个
                        else:
                            if(result['jszt']=='0' and len(result['data'])>0):
                                temp_c_ah.append([it['charge_ah'] for it in result['data']][j])
                                temp_d_ah.append([it['discharge_ah'] for it in result['data']][j])
                            else:
                                temp_c_ah.append(0)
                                temp_d_ah.append(0)
                            j=j-1
                    if quickly==False:
                        all_c_ah.append(temp_c_ah)
                        all_d_ah.append(temp_d_ah)
                        all_vols.append(temp_vols)
                        all_curs.append(temp_curs)
                        all_warn.append(temp_warn)
                        all_prot.append(temp_prot)
                        all_socs.append(temp_socs)
                        data_date=temp_data_date       
                    if len(temp_d_ah)>1 and len(temp_c_ah)>1:
                        #快捷版可能存在两段时间内没有数据，从而进入这里
                        if temp_c_ah[0]==0:
                            print('注：开始时间段内数据缺失，计算暂用0代替') 
                        elif temp_c_ah[-1]==0:
                            print('注：截止时间段内数据缺失，计算暂用0代替')                       
                        #需要计算其累计容量的差值
                        print(['截止累计充放容量',[temp_c_ah[-1],temp_d_ah[-1]]])
                        dc=temp_c_ah[-1]-temp_c_ah[0]
                        dd=temp_d_ah[-1]-temp_d_ah[0]
                        rs.append([dc,dd])
                    else:
                        print('选择时间范围内数据完全缺失，请重试！')
            if Flag:
                eba=-1
        if len(rs)==0:
            return False             
        df_rs=pd.DataFrame(rs)
        aver_c_ah,aver_d_ah=df_rs.mean(axis=0)
        aver_cyc=(aver_c_ah+aver_d_ah)/2/ah_sta/self.ratio
        df_cyc=df_rs.mean(axis=1)/ah_sta/self.ratio
        df_rs=pd.concat([df_rs,df_cyc,pd.DataFrame([[aver_c_ah,aver_d_ah,aver_cyc]])],axis=1)
        df_rs.columns=['各簇累计充电容量/Ah','各簇累计放电容量/Ah','各簇平均充放容量/Ah','平均充/Ah','平均放/Ah','平均等效循环/次']
#         print(df_rs)
        if mode=='getStrenths':
            return df_rs
        elif mode=='getCurs':
            #  返回电流值
            df_curs=pd.DataFrame(all_curs).T
            df_curs=pd.concat([pd.DataFrame(data_date),df_curs],axis=1)
            return df_curs
        
    #200002接口，获取PCS数据，频率，有功功率，直流电压、电流，直流功率
    def getPCSData(self,sbox=0,ebox=-1,sba=0,eba=-1):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_p_udc=[]#直流功率
        all_i_udc=[]#直流电流
        all_u_udc=[]#直流电压
        all_pz=[]#有功率
        all_p_rate=[]#电网频率
        data_date=[]
        rs=[]#返回计算后的结果
        for box in range(sbox,ebox):
            if eba==-1:
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                print('-------正在处理第'+str(box+1)+"箱第"+str(ba+1)+"堆的数据----------")
                temp_p_udc,temp_i_udc,temp_u_udc,temp_pz,temp_p_rate,temp_data_date=[],[],[],[],[],[]
                rge=range(len(ds)-1)
                for i in rge:
                    sta_data_date=ds[i]
                    end_data_date=ds[i+1]
                    print('处理中'+sta_data_date)
                    dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                    }
                    try:
                        result=json.loads(self.client.service.getData(self.code,self.methods[10],str(dataJson)))
                    except:
                        time.sleep(6)
                        result=json.loads(self.client.service.getData(self.code,self.methods[10],str(dataJson)))
    #               print(result)
                    if(result['jszt']=='0' and len(result['data'])>0):
                        temp_p_udc=np.concatenate((temp_p_udc,[it['p_udc'] for it in result['data']]))
                        temp_i_udc=np.concatenate((temp_i_udc,[it['i_udc'] for it in result['data']]))
                        temp_u_udc=np.concatenate((temp_u_udc,[it['u_udc'] for it in result['data']]))
                        temp_pz=np.concatenate((temp_pz,[it['pz'] for it in result['data']]))
                        temp_p_rate=np.concatenate((temp_p_rate,[it['p_rate'] for it in result['data']]))
                        temp_data_date=np.concatenate((temp_data_date,[it['data_date'] for it in result['data']]))
                    all_p_udc.append(temp_p_udc)
                    all_i_udc.append(temp_i_udc)
                    all_u_udc.append(temp_u_udc)
                    all_pz.append(temp_pz)
                    all_p_rate.append(temp_p_rate)
                    data_date.append(temp_data_date)
                rs.append({'data_date':data_date,'p_udc':all_p_udc,'i_udc':all_i_udc,'u_udc':all_u_udc,'pz':all_pz,'p_rate':all_p_rate})
        if len(rs)==0:
            return False
        print(rs)
        return 
    #100001 获取所有电站编码
    def getAllStaCode(self):
        strJson=str({'station_code':'ALL_DOC_CLOU'}) 
        bmsJson=json.loads(self.client.service.getDoc(self.code,self.methods[0],strJson))
        rs=[(it['station_code'],it['station_name']) for it in bmsJson["data"]]
        return rs
    #判断BMU短路开路算法
    def getBMUQS(self,VS,cluster_code,j,pack_bmu_num):
        pack_ord=int(j/pack_bmu_num)+1
        #取单个bmu里边最大电压时刻的12颗电压值
        VSnp=np.array(VS)
        cols=np.argmax(VSnp,axis=1)
        x=np.argmax(np.max(VSnp,axis=1))
        y=cols[x]
#             print([x,y])
#             print(VSnp[x,y])
        #取12串这个时刻电压值
        s=VSnp[:,y]
        if s.max()<3.45:
            #则查找放电末端
            cols=np.argmin(VSnp,axis=1)
            x=np.argmin(np.min(VSnp,axis=1))
            y=cols[x]
            s=VSnp[:,y]
            if s.max()>3.0:
                return False
        print(s)
        isQ=True#假设存在问题
        #问题模式判别 MUX0_1_sc:0&1短路
        if self.comvs(s[1],s[2],s[3]) and self.comvs(s[5],s[6],s[7]) and self.comvs(s[9],s[10],s[11]):
            #是否需要还必须判断其余电芯电压不一样
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与1短路问题"
        elif self.comvs(s[2],s[4],s[6]) and self.comvs(s[3],s[5],s[7]) and s[10]==0 and s[11]==0:
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与2短路问题"
        elif s[4]==0 and self.comvs(s[4],s[5],s[6],s[7],s[8],s[9],s[10],s[11]):
            #5~12都有问题
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX2与3短路问题"
        elif self.comvs(s[0],s[2],s[4],s[8]) and self.comvs(s[1],s[3],s[5],s[9]) and s[6]==0 and self.comvs(s[6],s[7],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与2与3都短路问题"
        #开路或者未连接
        elif self.comvs(s[0],s[1]) and self.comvs(s[2],s[3]) and self.comvs(s[4],s[5]) and self.comvs(s[6],s[7]) and self.comvs(s[8],s[9]) and self.comvs(s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0开路问题"
        elif self.comvs(s[0],s[2]) and self.comvs(s[1],s[3]) and self.comvs(s[4],s[6]) and self.comvs(s[5],s[7]) and self.comvs(s[8],s[10]) and self.comvs(s[9],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1开路问题"
        elif self.comvs(s[0],s[4]) and self.comvs(s[1],s[5]) and self.comvs(s[2],s[6]) and self.comvs(s[3],s[7]) and s[8]==0 and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX2开路问题"
        elif self.comvs(s[0],s[8]) and self.comvs(s[1],s[9]) and self.comvs(s[2],s[10]) and self.comvs(s[3],s[11]) and s[4]==0 and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX3开路问题"
        elif self.comvs(s[0],s[1],s[2],s[3]) and self.comvs(s[4],s[5],s[6],s[7]) and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与1开路问题"
        elif self.comvs(s[0],s[1],s[4],s[5]) and self.comvs(s[2],s[3],s[6],s[7]) and s[8]==0 and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与2开路问题"
        elif self.comvs(s[0],s[1],s[8],s[9]) and self.comvs(s[2],s[3],s[10],s[11]) and s[4]==0 and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与3开路问题"
        elif self.comvs(s[0],s[2],s[4],s[6]) and self.comvs(s[1],s[3],s[5],s[7]) and s[8]==0  and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与2开路问题"
        elif self.comvs(s[0],s[2],s[8],s[10]) and self.comvs(s[1],s[3],s[9],s[11]) and s[4]==0  and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与3开路问题"
        elif self.comvs(s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7],s[8],s[9],s[10],s[11]) and s[0]==0:
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX2与3开路问题(0,2,3/1,2,3/0,1,2,3)"
        elif self.comvs(s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7]) and s[8]==0 and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0,1,2开路问题"
        elif self.comvs(s[0],s[1],s[2],s[3],s[8],s[9],s[10],s[11]) and s[4]==0 and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0,1,3开路问题"
        else:
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" No problems found , OK!"
            isQ=False
        print(info)
        if isQ:
            return [info,s]
        else:
            return False
    #100006 获取电压及温度，并进行相关数据处理,可以详细到具体哪一包数据 quickly 设置是否保存完整数据 False：保存，mode数据处理方法
    def getCellDataByPack(self,sbox=0,ebox=-1,sba=0,eba=-1,scl=0,ecl=-1,sp=0,ep=-1,quickly=True,mode='getBMUQS'):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_vols,all_Ts=[],[]
        data_date=[]
        rs=[]#返回最后结果
        Flag=False
        for box in range(sbox,ebox):
            if eba==-1:
                Flag=True
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                clusterCodes=self.getClusterCodes(box,ba)
                if clusterCodes is False:
                    return False
                if ecl!=-1 and ecl>scl>=0:
                    clusterCodes=clusterCodes[scl:ecl]
                for cluster_code in clusterCodes:
                    print('-------正在处理第'+cluster_code+"的数据----------")
                    dataJson={
                        "bms_code": self.bmsCodes[box][ba], 
                        "cluster_code": cluster_code, 
                        "package_data_order": 1, 
                        "cell_order": 1, 
                        "sta_data_date": '', 
                        "end_data_date": '', 
                        "pageNo": 1, 
                        "pageSize": 4320, #pageNo 1.上午的数据 2.下午的数据
                        "_idAsc": 1
                    }
                    if ep==-1:
                        rgeP=range(self.pack_bmu_num*self.pack_num)
                    else:
                        rgeP=range(self.pack_bmu_num*sp,self.pack_bmu_num*ep)
                    #循环每个bmu
                    for j in rgeP:
                        dataJson['package_data_order']=j+1
                        VS,TS=[],[]
                        get_Ts_num=True#温度采集点数量
                        for i in range(self.cell_num):
                            #时间循环查询
                            rge=range(len(ds)-1)
                            vols,ts=[],[]
                            for k in rge:
                                sta_data_date=ds[k]
                                end_data_date=ds[k+1]
                                print("处理中"+sta_data_date)
                                dataJson['sta_data_date']=sta_data_date
                                dataJson['end_data_date']=end_data_date
                                dataJson['cell_order']=i+1
                                try:
                                    result=json.loads(self.client.service.getData(self.code,self.methods[5],str(dataJson)))
                                except:
                                    time.sleep(6)
                                    result=json.loads(self.client.service.getData(self.code,self.methods[5],str(dataJson)))
                                if(result['jszt']=='0' and len(result['data']['items'])>0):
                                    vols=np.concatenate((vols,[float(item.get('voltage')) for item in result['data']['items']]))
                                    #前几个有数据，只需循环只需采集点数量
                                    if get_Ts_num:
                                        try:
                                            ts=np.concatenate((ts,[float(item.get('humid')) for item in result['data']['items']]))
                                        except:
                                            get_Ts_num=False
                                else:
                                    print("第%s串电芯电压没有数据，是否是采集数据有问题！"%(i+1))
                                    break
                            VS.append(vols)
                            TS.append(ts)
                        if quickly==False:
                            #每个bmu数据进行保存
                            all_vols.append(VS)
                            all_Ts.append(TS)
                        if mode=='getBMUQS':
                            r=self.getBMUQS(VS,cluster_code,j,self.pack_bmu_num)
                            if r is not False:
                                rs.append(r)
                        #对每个BMU温度数据进行操作,Pack为单位，即合并pack_bmu_num个合并
                        elif mode=='getDTsByPack':
                            #将bmu温度数据合并成模组，进行求解最高温度，最大温差，最低温度，最大平均温度
                            rs.append(TS)
                        #以簇为单位，即合并所有pack_bmu_num*pack_num
                        elif mode=='getDTsByCl':
                            rs.append(TS)
                        #同理，以堆为单位合并
                        elif mode=='getDTsByBA':
                            rs.append(TS)
            if Flag:
                eba=-1                            
        return rs
    #100009 获取事件数据
    def getEventDataByBAS(self):
        
        return
    def __init__(self,sta_data_date, end_data_date,sta_code='0010'):
        '''
        Constructor
        '''
        self.sta_data_date=sta_data_date
        self.end_data_date=end_data_date
        strJson=str({'station_code':sta_code})
        bmsJson=json.loads(self.client.service.getDoc(self.code,self.methods[0],strJson))
        self.box_num=bmsJson["data"][0]['box_num']#电站箱子数量
        boxes_data=bmsJson["data"][0]["boxes_data"]
        self.bmsCodes=[[item['bms_code'] for item in boxes_data[box]['bmss_data']] for box in range(self.box_num)]
#         print(self.bmsCodes)

# if __name__=='__main__':

