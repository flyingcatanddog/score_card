-- 合同中间表
set hive.auto.convert.join = false;
set hive.limit.optimize.enable=true;
set hive.exec.reducers.max=100;
set hive.exec.reducers.bytes.per.reducer=500000000;
-- set sep_date= '2017-04-01';
DROP TABLE IF EXISTS risk_features.zzr_model3_intermediate_intent_clean;
CREATE TABLE risk_features.zzr_model3_intermediate_intent_clean
AS
SELECT 
	t1.lproductid,
	t1.lsalesmanid,
	t1.strdeptcode,
	t1.lborrowerid,
	CAST(t1.lid AS INT) AS lid,
	CAST(t1.lamount AS int) AS lamount,
	t1.strloandate,
	t1.strborrowenddate,
	CASE WHEN datediff(t1.strloandate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_loandate_group,
	CASE WHEN -3<=months_between(t1.strloandate,m.sep_date) AND months_between(t1.strloandate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_loandate_3months_group,
	CASE WHEN -6<=months_between(t1.strloandate,m.sep_date) AND months_between(t1.strloandate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_loandate_6months_group,
	CASE WHEN -9<=months_between(t1.strloandate,m.sep_date) AND months_between(t1.strloandate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_loandate_9months_group,
	CASE WHEN -12<=months_between(t1.strloandate,m.sep_date) AND months_between(t1.strloandate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_loandate_12months_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_enddate_group,
	CASE WHEN -3<=months_between(t1.strborrowenddate,m.sep_date) AND months_between(t1.strborrowenddate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_enddate_3months_group,
	CASE WHEN -6<=months_between(t1.strborrowenddate,m.sep_date) AND months_between(t1.strborrowenddate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_enddate_6months_group,
	CASE WHEN -9<=months_between(t1.strborrowenddate,m.sep_date) AND months_between(t1.strborrowenddate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_enddate_9months_group,
	CASE WHEN -12<=months_between(t1.strborrowenddate,m.sep_date) AND months_between(t1.strborrowenddate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_enddate_12months_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<-1 THEN 1 ELSE 0 END AS model_observation_overdue1day_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<-5 THEN 1 ELSE 0 END AS model_observation_overdue5day_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<-10 THEN 1 ELSE 0 END AS model_observation_overdue10day_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<-30 THEN 1 ELSE 0 END AS model_observation_overdue30day_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<-60 THEN 1 ELSE 0 END AS model_observation_overdue60day_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<-90 THEN 1 ELSE 0 END AS model_observation_overdue90day_group,
	CASE WHEN datediff(t1.strborrowenddate,m.sep_date)<-180 THEN 1 ELSE 0 END AS model_observation_overdue180day_group,
	CASE WHEN datediff(t1.strborrowenddate,m.y_begin_date)>=0 AND datediff(t1.strborrowenddate,m.y_end_date)<0 THEN 1 ELSE 0 END AS y1_group,
	CASE WHEN datediff(t1.strborrowenddate,m.y_end_date)<-60 THEN 1 ELSE 0 END AS y1_observation_overdue60day_group,
	CASE WHEN datediff(t1.strloandate,m.y_begin_date)>=0 AND datediff(t1.strborrowenddate,add_months(m.y_end_date,1))<0 THEN 1 ELSE 0 END AS y2_group,
	CASE WHEN datediff(t1.strborrowenddate,m.y_begin_date)>=0 AND datediff(t1.strborrowenddate,add_months(m.y_end_date,1))<0 THEN 1 ELSE 0 END AS y4_group,
	CASE WHEN datediff(t1.strborrowenddate,add_months(m.y_end_date,1))<-60 THEN 1 ELSE 0 END AS y2_observation_overdue60day_group,
	CASE WHEN datediff(t1.strborrowenddate,add_months(m.y_end_date,1))<-30 THEN 1 ELSE 0 END AS y2_observation_overdue30day_group,
	t2.prepay30_amount,
	CASE WHEN t1.lamount - t2.prepay30_amount <=0 THEN 1 ELSE 0 END AS is_prepay30,
	t2.prepay_amount,
	CASE WHEN t1.lamount - t2.prepay_amount <=0 THEN 1 ELSE 0 END AS is_prepay,
	t2.ontime_amount,
    CASE WHEN t1.lamount - t2.ontime_amount <=0 THEN 1 ELSE 0 END AS is_ontime,
	t2.enddate_plus1_amount,
	CASE WHEN t1.lamount - t2.enddate_plus1_amount <=0 THEN 1 ELSE 0 END AS enddate_plus1_payoff,
	t2.enddate_plus5_amount,
	CASE WHEN t1.lamount - t2.enddate_plus5_amount <=0 THEN 1 ELSE 0 END AS enddate_plus5_payoff,
	t2.enddate_plus10_amount,
	CASE WHEN t1.lamount - t2.enddate_plus10_amount <=0 THEN 1 ELSE 0 END AS enddate_plus10_payoff,
	t2.enddate_plus30_amount,
	CASE WHEN t1.lamount - t2.enddate_plus30_amount <=0 THEN 1 ELSE 0 END AS enddate_plus30_payoff,
	t2.enddate_plus60_amount,
	CASE WHEN t1.lamount - t2.enddate_plus60_amount <=0 THEN 1 ELSE 0 END AS enddate_plus60_payoff,
	t2.enddate_plus90_amount,
	CASE WHEN t1.lamount - t2.enddate_plus90_amount <=0 THEN 1 ELSE 0 END AS enddate_plus90_payoff,
	t2.enddate_plus180_amount,
	CASE WHEN t1.lamount - t2.enddate_plus180_amount <=0 THEN 1 ELSE 0 END AS enddate_plus180_payoff,
	t1.lamount - t2.model_pay_amount AS model_left_amount,
	CASE WHEN t1.lamount - t2.model_pay_amount <=0 THEN 1 ELSE 0 END AS model_payoff, -- 这笔合同在回溯时是否还清
	CASE WHEN t1.lamount - t2.ontime_amount > 0 -- 确认逾期
			  AND t1.lamount - t2.pay_amount > 0 -- 现在还没有还清  
			  THEN datediff(CURRENT_DATE(),t1.strborrowenddate)
		 WHEN t1.lamount - t2.ontime_amount > 0 -- 确认逾期
		 	  AND t1.lamount - t2.pay_amount <= 0 -- 现在已经还清了
		 	  THEN datediff(t2.last_realrepaydate,t1.strborrowenddate)
		 ELSE NULL END AS overdue_days, -- 如果该笔合同未到期，也会算为"确认逾期、现在还没有还清"，但得到的逾期天数为负数
	CASE WHEN t1.lamount - t2.prepay_amount <=0 THEN datediff(t1.strborrowenddate,t2.last_realrepaydate) ELSE NULL END AS prepay_days
FROM ods_yundai.tbborrowintent t1
JOIN 
	(
	SELECT 
		a.lid,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=-30 THEN b.lprincipal ELSE 0 END) AS prepay30_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<0 THEN b.lprincipal ELSE 0 END) AS prepay_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=0 THEN b.lprincipal ELSE 0 END ) AS ontime_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=1 THEN b.lprincipal ELSE 0 END) AS enddate_plus1_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=5 THEN b.lprincipal ELSE 0 END) AS enddate_plus5_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=10 THEN b.lprincipal ELSE 0 END) AS enddate_plus10_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=30 THEN b.lprincipal ELSE 0 END) AS enddate_plus30_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=60 THEN b.lprincipal ELSE 0 END) AS enddate_plus60_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=90 THEN b.lprincipal ELSE 0 END) AS enddate_plus90_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,a.strborrowenddate)<=180 THEN b.lprincipal ELSE 0 END) AS enddate_plus180_amount,
		SUM(CASE WHEN datediff(b.strrealrepaydate,m.sep_date)<0 THEN b.lprincipal ELSE 0 END) AS model_pay_amount,
		SUM(CASE WHEN b.strrealrepaydate IS NOT NULL THEN b.lprincipal ELSE 0 END) AS pay_amount,
		MAX(b.strrealrepaydate) AS last_realrepaydate
	FROM
	ods_yundai.tbborrowintent a
	LEFT JOIN ods_yundai.tbborrowerbill b ON a.lid = b.lborrowintentid
	LEFT JOIN risk_features.model3_time_table m ON a.lborrowerid=m.luserid
	WHERE a.nstate IN (4,5,7,9) AND b.nstate <> 8
	GROUP BY a.lid
	)t2 ON t1.lid = t2.lid
LEFT JOIN risk_features.model3_time_table m ON t1.lborrowerid=m.luserid;