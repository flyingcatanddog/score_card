# -*- coding: utf-8 -*-
"""
Created on Mon Sep 25 08:47:59 2017

@author: zhangzerong
"""
# 自动化分箱工具
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import pylab as pl
from scipy.stats import spearmanr

def woe_function(df,column,woe_type='d',DC='default',q=5,method=0,corr=0.99):
    '''
    df 要被处理的数据集
    column 要被处理的列名
    dtype = 'd' 离散 ;dtype = 'c' 连续
    q = 5 默认连续型变量取值将分为5段，如果需要修改最终分段数，则传入一个数字；如果需要按照既定的规则分段，则传入一个列表(注意列表格式，要分成3段，只需要传入两个参数即可，不需要上下界)
    DC = 'default' 默认离散型变量按照原始类别分段，如果需要按照既定的规则分段，则传入一个字典
    method = 'spearmanr' 连续型变量分箱收敛方法，'spearmanr'为斯皮尔曼相关系数；'chi-square'为卡方；'minus'为直接两者相减
    '''
    if woe_type == 'd':
        df_woe,column_iv = d_plot(df,column,DC)
    elif woe_type == 'c':
        df_woe,column_iv = c_plot(df,column,q,method,corr)
    df.drop([column+'_fillna'],axis=1,inplace=True)
    woe_plot(df_woe,column_iv)
    return df_woe,column_iv
    
def d_plot(df,column,DC):
    '''
    如果需要对离散型变量的分类做处理，则传入一个字典DC；不传则默认按照原始的分类来处理
    '''
    df[column+'_fillna'] = df[column].fillna('missing_value')
    if DC =='default':
        df_woe,column_iv = cal_iv(df,column+'_fillna')        
    if DC !='default':
        DC['missing_value']='missing_value'
        df[column+'_fillna'] = df[column+'_fillna'].map(DC)
        df[column+'_fillna'].fillna('others',inplace=True)
        df_woe,column_iv = cal_iv(df,column+'_fillna')
    return df_woe,column_iv

def c_plot(df,column,q,method,corr):
    if method == 'spearmanr':
        q = spearmanr_q(df,column,corr)
    elif method == 'chi-square':
        q = chi_square_q(df,column)
    elif method == 'minus':
        q = chi_square_q(df,column)
    q.insert(0,-np.inf)
    q.append(np.inf)
    df[column+'_fillna'] = pd.cut(df[column],q)
    df[column+'_fillna'] = df[column+'_fillna'].astype(object).fillna('missing_value')
    df_woe,column_iv = cal_iv(df,column+'_fillna')
    return df_woe,column_iv

def spearmanr_q(df,column,corr):
    '''
    spearmanr分箱方法
    原理：
        初始用qcut方法分成n段，实际有可能出现少于n段的情况（例如某一段占比大于1/n)
        按照这样的分段计算出df_woe，判断woe的spearman相关系数是否大于corr，如果大于，则停止循环，否则n减少1
        最终必然收敛（在分成两段的时候）
    '''
    r = 0
    n = 15
    while np.abs(r) < corr:
        df[column+'_fillna'],q = pd.qcut(df[column],n,retbins=True,duplicates='drop')
        df[column+'_fillna'] = df[column+'_fillna'].astype(object)
        df[column+'_fillna'] = df[column+'_fillna'].fillna('missing_value')
        df_woe,column_iv = cal_iv(df,column+'_fillna')
        if 'missing_value' in df_woe:    
            df_woe.drop('missing_value',inplace=True)
        r = spearmanr(range(len(df_woe)),df_woe['woe']).correlation
        q = q.tolist()
        n -= 1
    return q
        
def chi_square_q(df,column):
    '''通过chi_square来确定最优的分箱'''
    n_begin = 50
    df[column+'_fillna'],q = pd.qcut(df[column],n_begin,retbins=True,duplicates='drop')
    df[column+'_fillna'] = df[column+'_fillna'].astype(object).fillna('missing_value')
    df_woe,column_iv = cal_iv(df,column+'_fillna')
    if 'missing_value' in df_woe:
        df_woe.drop('missing_value',inplace=True)
    q = q.tolist()
    q.pop(0)
    q.pop()
    i = len(q)+1
    while i>5:
        q.insert(0,-np.inf)
        q.append(np.inf)
        df[column+'_fillna'] = pd.cut(df[column],q)
        df[column+'_fillna'] = df[column+'_fillna'].astype(object).fillna('missing_value')
        df_woe,column_iv = cal_iv(df,column+'_fillna')
        if 'missing_value' in df_woe:
            df_woe.drop('missing_value',inplace=True)
        temp={}
        for i in range(len(df_woe)-1):
            a=df_woe.ix[i,'bad_count']
            b=df_woe.ix[i,'good_count']
            c=df_woe.ix[i+1,'bad_count']
            d=df_woe.ix[i+1,'good_count']
            temp[i] = (a+b+c+d)*(a*d-b*c)**2/((a+b)*(c+d)*(a+c)*(b+d))
        q.pop(min(temp, key=temp.get)+1) #合并卡方值最小的那一个分类
        q.pop(0)
        q.pop()
        i=len(q)+1
    return q       

def minus_q(df,column):
    '''通过相邻的woe两两比较大小来确定最优的分箱'''
    n_begin = 50
    df[column+'_fillna'],q = pd.qcut(df[column],n,retbins=True,duplicates='drop')
    df[column+'_fillna'] = df[column+'_fillna'].astype(object).fillna('missing_value')
    df_woe,column_iv = cal_iv(df,column+'_fillna')
    if 'missing_value' in df_woe:
        df_woe.drop('missing_value',inplace=True)
    q = q.tolist()
    q.pop(0)
    q.pop()
    i = len(q)+1
    while i>5:
        q.insert(0,-np.inf)
        q.append(np.inf)
        df[column+'_fillna'] = pd.cut(df[column],q)
        df[column+'_fillna'] = df[column+'_fillna'].astype(object).fillna('missing_value')
        df_woe,column_iv = cal_iv(df,column+'_fillna')
        if 'missing_value' in df_woe:
            df_woe.drop('missing_value',inplace=True)
        temp={}
        for i in range(len(df_woe)-1):
            temp[i]=abs(df_woe.ix[i,'woe']-df_woe.ix[i+1,'woe'])
        q.pop(min(temp, key=temp.get)+1) #合并woe差值最小的那一个分类
        q.pop(0)
        q.pop()
        i=len(q)+1
    return q
    

def cal_iv(df,column):
    df_bad = df[df['y_5']==1].groupby(df[column]).size().to_frame(name='bad_count')
    df_good = df[df['y_5']==0].groupby(df[column]).size().to_frame(name='good_count')
    df_woe = pd.merge(df_bad,df_good,how='outer',left_index=True,right_index=True)
    df_woe.fillna(0.001,inplace=True) #为了保证不出现空值
    df_woe['n_group']=df_woe['bad_count']+df_woe['good_count']
    df_woe['bad_ratio'] = df_woe['bad_count']/sum(df_woe['bad_count'])
    df_woe['good_ratio'] = df_woe['good_count']/sum(df_woe['good_count'])
    df_woe['woe'] = np.log(df_woe['bad_ratio']/df_woe['good_ratio'])
    df_woe['iv'] = (df_woe['bad_ratio']-df_woe['good_ratio'])*df_woe['woe']
    column_iv = sum(df_woe['iv'])
    return df_woe,column_iv

'''以下要重新自己写一遍,图形太难看'''
def woe_plot(df_woe,column_iv):
    matplotlib.rcParams.update({'font.size': 12})
    plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus']=False
    fig,ax1 = plt.subplots()
    ax=ax1.twinx()
    ax1.bar(range(len(df_woe)),df_woe.n_group,color='silver')
    ax1.set_ylabel('# of rows',fontsize=11)
    fig.set_size_inches(6,4)
    ax.plot(range(len(df_woe)),df_woe.woe,marker='d',color='purple')
    plt.annotate('IV=%s' % "{:2.3f}".format(column_iv), xy=(0.05, 0.90), xycoords='axes fraction',fontsize=12)
    lables=[i for i in df_woe.index]
    pl.xticks(range(len(df_woe)),lables)#,rotation=)
    ax.set_ylabel('woe')
    ax.grid(b=True)
    ax.set_title('%s(WoE grouping)' % df_woe.columns[0])

    
    
        
        