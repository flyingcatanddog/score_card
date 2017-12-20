# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 08:56:17 2017

@author: zhangzerong
"""

# 自动化分箱工具
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import pylab as pl
from scipy.stats import spearmanr

def woe_function(df,column,woe_type='d',q=5,method='spearmanr',corr=0.95,add_woe=False,woe_sort=False):
    '''
    df 要被处理的数据集
    column 要被处理的列名
    dtype = 'd' 离散，默认为离散 ;dtype = 'c' 连续
    q = 5 针对连续型和离散型变量的设定不一样。
          针对连续型变量：1）默认连续型变量取值将分为5段，如果需要修改最终分段数，则传入一个数字；
                         2）如果需要按照既定的规则分段，则传入一个列表(注意列表格式，要分成3段，只需要传入两个参数即可，不需要上下界)
          针对离散型变量：1）默认按照离散变量的原始标签类进行分段
                         2）如果需要按照既定的规则分段，则传入一个字典（字典key为原始标签，value为既定规则后的分类）         
    method = 连续型变量分箱收敛方法
            'spearmanr'为斯皮尔曼相关系数，默认为'spearmanr'方法；
            'chi-square'为计算相邻两组卡方值，每次循环合并卡方值最小的两组；
            'minus'为计算相邻两组woe，每次循环合并woe差最小的两组
    add_woe = False 表示是否将woe编码后的列加入到df中，默认False
    woe_sort = False 表示是否将df_woe按照woe的值从小到大排列index，主要针对离散变量
    '''
    if woe_type == 'd':
        df_woe,column_iv = d_plot(df,column,q,woe_sort)
    elif woe_type == 'c':
        df_woe,column_iv = c_plot(df,column,q,method,corr) 
    if add_woe:
        woe_dict = dict(zip(df_woe.index,df_woe['woe']))
        df[column+'_woe'] = df[column+'_fillna'].map(woe_dict)
    df.drop([column+'_fillna'],axis=1,inplace=True)
    woe_plot(df_woe,column_iv)
    return df_woe
    
def d_plot(df,column,q,woe_sort):
    '''
    如果需要对离散型变量的分类做处理，则传入一个字典DC；不传则默认按照原始的分类来处理
    '''
    df[column+'_fillna'] = df[column].fillna('missing_value')       
    if type(q) == dict:
        q['missing_value']='missing_value'
        df[column+'_fillna'] = df[column+'_fillna'].map(q)
        df[column+'_fillna'].fillna('others',inplace=True)
        df_woe,column_iv = cal_iv(df,column+'_fillna')
    else:
        df_woe,column_iv = cal_iv(df,column+'_fillna')
    
    if woe_sort:
        '''这里将无序离散变量，按照woe排序'''
        df_woe = df_woe.sort_values('woe')
    return df_woe,column_iv

def c_plot(df,column,q,method,corr):
    if type(q) != list:
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
    df_woe,column_iv = cal_iv(df,column+'_fillna',index_sort=True)
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
        df_woe,column_iv = cal_iv(df,column+'_fillna',index_sort=True)
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
    df_woe,column_iv = cal_iv(df,column+'_fillna',index_sort=True)
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
        df_woe,column_iv = cal_iv(df,column+'_fillna',index_sort=True)
        if 'missing_value' in df_woe:
            df_woe.drop('missing_value',inplace=True)
        temp={}
        for i in range(len(df_woe)-1):
            a=df_woe.iloc[i]['bad_count']
            b=df_woe.iloc[i]['good_count']
            c=df_woe.iloc[i+1]['bad_count']
            d=df_woe.iloc[i+1]['good_count']
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
    df_woe,column_iv = cal_iv(df,column+'_fillna',index_sort=True)
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
        df_woe,column_iv = cal_iv(df,column+'_fillna',index_sort=True)
        if 'missing_value' in df_woe:
            df_woe.drop('missing_value',inplace=True)
        temp={}
        for i in range(len(df_woe)-1):
            temp[i]=abs(df_woe.iloc[i]['woe']-df_woe.iloc[i+1]['woe'])
        q.pop(min(temp, key=temp.get)+1) #合并woe差值最小的那一个分类
        q.pop(0)
        q.pop()
        i=len(q)+1
    return q
    

def cal_iv(df,column,index_sort=False):
    df_bad = df[df['y_5']==1].groupby(df[column]).size().to_frame(name='bad_count')
    df_good = df[df['y_5']==0].groupby(df[column]).size().to_frame(name='good_count')
    df_woe = pd.merge(df_bad,df_good,how='outer',left_index=True,right_index=True)
    df_woe.fillna(0.001,inplace=True) #为了保证不出现空值
    df_woe['n_group']=df_woe['bad_count']+df_woe['good_count']
    df_woe['bad_rate']=df_woe['bad_count']/df_woe['n_group']
    df_woe['bad_ratio'] = df_woe['bad_count']/sum(df_woe['bad_count'])
    df_woe['good_ratio'] = df_woe['good_count']/sum(df_woe['good_count'])
    df_woe['woe'] = np.log(df_woe['bad_ratio']/df_woe['good_ratio'])
    df_woe['iv'] = (df_woe['bad_ratio']-df_woe['good_ratio'])*df_woe['woe']
    '''分组后可能出现分段不排序的问题，这里将“连续变量”的分段，做一个排序'''
    if index_sort:
        index_list=df_woe.index.tolist()
        if 'missing_value' in index_list:
            index_list.remove('missing_value')
            index_list.sort()
            index_list.append('missing_value')
        else:
            index_list.sort()
        df_woe = df_woe.reindex(index_list)
    column_iv = sum(df_woe['iv'])
    df_woe.drop(['bad_ratio','good_ratio'],axis=1,inplace=True)
    return df_woe,column_iv

'''以下要重新自己写一遍,图形太难看'''
def woe_plot(df_woe,column_iv):
    matplotlib.rcParams.update({'font.size': 12})
    plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
    plt.rcParams['axes.unicode_minus']=False
    fig,ax1 = plt.subplots()
    ax=ax1.twinx()
    ax1.bar(range(len(df_woe)),df_woe.n_group,color='cadetblue')
    ax1.set_ylabel('group number',fontsize=11)
    fig.set_size_inches(10,4)
    ax.plot(range(len(df_woe)),df_woe.woe,marker='d',color='darkslateblue')
    plt.annotate('IV=%s' % "{:2.3f}".format(column_iv), xy=(0.05, 0.90), xycoords='axes fraction',fontsize=12)
    lables=[i for i in df_woe.index]
    pl.xticks(range(len(df_woe)),lables)#,rotation=)
    ax.set_ylabel('woe')
    #ax.grid(b=True)
    ax.set_title('groupy number——woe')
    plt.show()