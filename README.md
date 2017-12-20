# score_card
programe of making a credit card

评分卡：
step 1：获取数据。通常需要在数据仓库中获取，如hapood、mysql、oracle。
step 2：清洗数据及筛选变量。本项目的方法主要是通过woe编码来进行清洗，同时根据iv值来进行变量的筛选。
step 3：建立模型。评分卡模型实质是逻辑回归，将woe编码后的数据直接入模。
