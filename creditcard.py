# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 17:44:26 2017

@author: zhangzerong
"""

#模型结果及评价
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
from scipy import stats
def credit_card(df,columns,y):
    '''
    df : 要被处理的dataframe
    columns  : 已经woe转换过的列（对应woe_fuction）
    y : 标签列
    '''
    x_train,x_test,y_train,y_test = train_test_split(df[columns],df[y],test_size = 0.4,random_state = 99)
    clf = LogisticRegression()
    clf.fit(x_train,y_train)
    '''输出模型结果'''
    print_log(columns,clf.coef_[0],clf
              .intercept_[0])
    
    train_log_odds = x_train[columns].dot(clf.coef_[0]) + clf.intercept_[0]
    test_log_odds = x_test[columns].dot(clf.coef_[0]) + clf.intercept_[0]    
    train_prob = 1.0/(1+np.exp(-train_log_odds))
    test_prob = 1.0/(1+np.exp(-test_log_odds))
    '''计算分值并打印结果'''
    #normal_odds = len(df[df[y]==1])/len(df[df[y]==0])#初始的坏客户/好客户比率
    normal_odds = 1/20
    A,B = get_score(normal_odds)
    train_score = A + B * train_log_odds
    test_score = A + B * test_log_odds
    print('the score is given by : score = %s %s * ln(odds)' % (A,B))
    print('tips : odds = p/(1-p)')
    print('\n'+'  =￣ω￣=  ' * 10+'\n')
    '''输出ks值'''
    fig,ax=plt.subplots()
    aa=ks_chart(ax,train_score,y_train,'Train',style='-',set_ax=True)
    bb=ks_chart(ax,test_score,y_test,'Test',style='--',set_ax=False)
    ax.legend(aa[0]+bb[0],aa[1]+bb[1],loc='upper left')
    '''输出ROC曲线（附带AUC\GINI）'''
    ROC_curve([(y_train,train_prob,'train'),(y_test,test_prob,'test')]) 
    return clf.intercept_[0],clf.coef_[0],A,B
    
def print_log(columns,coef,intercept):
    print ('the logistic regression result is :')
    print ('ln[p/(1-p)] = %s' % intercept)
    for i in range(len(columns)):
        if coef[i]>=0:
            sign = '+'
        else:
            sign = ''
        print(' '* 14 + sign + '%s * %s' % (coef[i],columns[i]))

def ROC_curve(params):
    fig,ax=plt.subplots()
    text = ''
    for param in params:
        label,prob,name = param
        fpr,tpr,_ = metrics.roc_curve(label,prob)
        ax.plot(fpr,tpr,'-',label=name)
        AUC = metrics.auc(fpr,tpr)
        GINI = 2 * AUC - 1
        text = text + '%s AUC:  %s\n' % (name,"{:10.3f}".format(AUC))
        text = text + '%s GINI: %s\n' % (name,"{:10.3f}".format(GINI))
    ax.set_xlabel('false positive rate')
    ax.set_ylabel('true positive rate')
    ax.set_title('ROC Curve',fontsize=13)
    ax.legend(loc='center right')
    ax.text(0.6,-0.1,text)
    plt.show()

def get_score(normal_odds,base_score=500,pdo=100):
    '''
    基础分为500分，每降低100分，odds翻倍；分值越大说明用户越好（为了便于理解）
    '''
    b = - pdo / np.log(2)
    a = base_score - b*np.log(normal_odds)
    return a,b

def percentile(a,scores):
    percentiles=[]
    for s in scores:
        percentiles.append(stats.percentileofscore(a,s))
    return np.array(percentiles)

def ks_chart(ax,score,y,label, style='-', print_ks=False,set_ax=True):
    '''
    ax:    Matplotlib Axes
    score: score array (np array or pandas column)
    y:     good / bad lables
    print_ks: need print KS value on the plot or not
    set_ax : need make some setting for the plot or not
    '''
    ax1 = ax.twinx()
    bins = np.linspace(score.min(),score.max(),200)
    groups=score.groupby(y)
    cumlative_per=[]
    axes=[]
    labels=[]
    for group,data in groups:
        q=percentile(data,bins)
        cumlative_per.append(q)
        lab = '%s (%s)' % ("{:1.0f}".format(group),label)
        labels.append(lab)
        a=ax1.plot(bins,q,style,label=lab,lw=1.5)
        if isinstance(a,list):
            axes.append(a[0])
        else:
            axes.append(a)
    ks_values=cumlative_per[1]-cumlative_per[0]
    if len(ks_values[ks_values<0])>0.9*len(bins):
        ks_values = -ks_values
    max_ks=np.max(ks_values)
    lab='KS (%s)' % label
    labels.append(lab)
    a=ax.plot(bins,ks_values,style,label=lab,lw=0.5)
    if isinstance(a,list):
        axes.append(a[0])
    else:
        axes.append(a)
    ax.grid(b=True,linestyle='--')
    if print_ks:
        ax.text(0.8, 0.97,'Max K-S:%s' % "{:10.2f}".format(max_ks),  horizontalalignment='center',  verticalalignment='center', transform = ax.transAxes,fontsize=11)
    if set_ax:
        ax.set_title('KS plot',fontsize=13)
        ax.set_xlabel('score')
        ax.set_ylabel('K-S Value')
        ax1.set_ylabel('Cumulative percentage')
    print ('the ks of %s is %s' % (label,"{:1.2f}".format(max_ks)))
    return  axes,labels
from sklearn.model_selection import train_test_split