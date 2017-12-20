set hive.auto.convert.join = false;
set hive.limit.optimize.enable=true;
set hive.exec.reducers.max=100;
set hive.exec.reducers.bytes.per.reducer=500000000;
set sep_date=string(current_date);
set x_end_date=DATE_SUB(${hiveconf:sep_date},1);
set x_begin_date = add_months(${hiveconf:sep_date},-12);
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_time_table;
CREATE TABLE risk_features.model3_freeze_monitor_time_table
AS
	SELECT
		luserid,
		${hiveconf:sep_date} AS sep_date,
		${hiveconf:x_end_date} AS x_end_date,
		${hiveconf:x_begin_date} AS x_begin_date
	FROM ods_yundai.tbborrower;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_intermediate_intent_clean;
CREATE TABLE risk_features.model3_freeze_monitor_intermediate_intent_clean
AS
SELECT 
	t1.lproductid,
	t1.lsalesmanid,
	t1.strdeptcode,
	t1.lborrowerid,
	cast(t1.lid AS INT) AS lid,
	cast(t1.lamount AS INT) as lamount,
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
	LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON a.lborrowerid=m.luserid
	WHERE a.nstate IN (4,5,7,9) AND b.nstate <> 8
	GROUP BY a.lid
	)t2 ON t1.lid = t2.lid
LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON t1.lborrowerid=m.luserid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_intermediate_bill_clean;
CREATE TABLE risk_features.model3_freeze_monitor_intermediate_bill_clean
AS
	SELECT
	t1.lborrowerid,
	t2.lborrowintentid,
	t2.ntermindex,
	t2.model_term_payoff,
	t2.should_pay_amount,
	CASE WHEN datediff(strloandate,m.sep_date)<0 THEN 1 ELSE 0 END AS model_term_begindate_group,
	t2.model_term_enddate_group,
	t2.model_term_enddate_3months_group,
	t2.model_term_enddate_6months_group,
	t2.model_term_enddate_9months_group,
	t2.model_term_enddate_12months_group,
	t2.overdue_days
	FROM
	ods_yundai.tbborrowintent t1
	JOIN
	(
		SELECT
		lborrowintentid,
		ntermindex,
		MAX(CASE WHEN strrealrepaydate IS NULL OR strrealrepaydate='' OR datediff(strrealrepaydate,m.sep_date)>=0 THEN 1 ELSE 0 END) AS model_term_payoff,
		SUM(lprincipal)+SUM(linterest)+SUM(lhaierinterest) AS should_pay_amount,
		CASE WHEN MAX(datediff(strRepayDate,m.sep_date))<0 THEN 1 ELSE 0 END AS model_term_enddate_group,
		CASE WHEN -3<=MAX(months_between(strRepayDate,m.sep_date)) AND MAX(months_between(strRepayDate,m.sep_date))<0 THEN 1 ELSE 0 END AS model_term_enddate_3months_group,
		CASE WHEN -6<=MAX(months_between(strRepayDate,m.sep_date)) AND MAX(months_between(strRepayDate,m.sep_date))<0 THEN 1 ELSE 0 END AS model_term_enddate_6months_group,
		CASE WHEN -9<=MAX(months_between(strRepayDate,m.sep_date)) AND MAX(months_between(strRepayDate,m.sep_date))<0 THEN 1 ELSE 0 END AS model_term_enddate_9months_group,
		CASE WHEN -12<=MAX(months_between(strRepayDate,m.sep_date)) AND MAX(months_between(strRepayDate,m.sep_date))<0 THEN 1 ELSE 0 END AS model_term_enddate_12months_group,
		MAX(datediff(IF(strrealrepaydate IS NULL OR strrealrepaydate='' OR datediff(strrealrepaydate,m.sep_date)>=0,m.sep_date,strrealrepaydate),strrepaydate)) AS overdue_days
		FROM ods_yundai.tbborrowerbill a
		LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON a.lborrowerid=m.luserid
		WHERE nstate <> 8
		GROUP BY 
		lborrowintentid,
		ntermindex
	)t2 ON t1.lid=t2.lborrowintentid
	LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON t1.lborrowerid=m.luserid
	WHERE t1.nstate IN (4,5,7,9);
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_nterms_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_nterms_repayment
AS 
	SELECT
	lborrowerid,
	SUM(CASE WHEN model_term_enddate_group=1 AND ntermindex=1 THEN 1 ELSE 0 END) AS expire_1st_terms,
	SUM(CASE WHEN model_term_enddate_3months_group=1 AND ntermindex=1 THEN 1 ELSE 0 END) AS expire_1st_terms_last_3months,
	SUM(CASE WHEN model_term_enddate_6months_group=1 AND ntermindex=1 THEN 1 ELSE 0 END) AS expire_1st_terms_last_6months,
	SUM(CASE WHEN model_term_enddate_9months_group=1 AND ntermindex=1 THEN 1 ELSE 0 END) AS expire_1st_terms_last_9months,
	SUM(CASE WHEN model_term_enddate_12months_group=1 AND ntermindex=1 THEN 1 ELSE 0 END) AS expire_1st_terms_last_12months,
	SUM(CASE WHEN model_term_enddate_group=1 AND ntermindex=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_terms,
	SUM(CASE WHEN model_term_enddate_3months_group=1 AND ntermindex=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_3months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_terms_last_3months,
	SUM(CASE WHEN model_term_enddate_6months_group=1 AND ntermindex=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_6months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_terms_last_6months,
	SUM(CASE WHEN model_term_enddate_9months_group=1 AND ntermindex=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_9months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_terms_last_9months,
	SUM(CASE WHEN model_term_enddate_12months_group=1 AND ntermindex=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_12months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_terms_last_12months,
	SUM(CASE WHEN model_term_enddate_group=1 AND ntermindex=1 THEN should_pay_amount ELSE NULL END) AS expire_1st_term_amount,
	SUM(CASE WHEN model_term_enddate_3months_group=1 AND ntermindex=1 THEN should_pay_amount ELSE NULL END) AS expire_1st_term_amount_last_3months,
	SUM(CASE WHEN model_term_enddate_6months_group=1 AND ntermindex=1 THEN should_pay_amount ELSE NULL END) AS expire_1st_term_amount_last_6months,
	SUM(CASE WHEN model_term_enddate_9months_group=1 AND ntermindex=1 THEN should_pay_amount ELSE NULL END) AS expire_1st_term_amount_last_9months,
	SUM(CASE WHEN model_term_enddate_12months_group=1 AND ntermindex=1 THEN should_pay_amount ELSE NULL END) AS expire_1st_term_amount_last_12months,
	SUM(CASE WHEN model_term_enddate_group=1 AND ntermindex=1 AND overdue_days>0 THEN should_pay_amount
		     WHEN model_term_enddate_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_term_amount,
	SUM(CASE WHEN model_term_enddate_3months_group=1 AND ntermindex=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_3months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_term_amount_last_3months,
	SUM(CASE WHEN model_term_enddate_6months_group=1 AND ntermindex=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_6months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_term_amount_last_6months,
	SUM(CASE WHEN model_term_enddate_9months_group=1 AND ntermindex=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_9months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_term_amount_last_9months,
	SUM(CASE WHEN model_term_enddate_12months_group=1 AND ntermindex=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_12months_group=1 AND ntermindex=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_1st_term_amount_last_12months,
	SUM(model_term_enddate_group) AS expire_terms,
	SUM(model_term_enddate_3months_group) AS expire_terms_last_3months,
	SUM(model_term_enddate_6months_group) AS expire_terms_last_6months,
	SUM(model_term_enddate_9months_group) AS expire_terms_last_9months,
	SUM(model_term_enddate_12months_group) AS expire_terms_last_12months,
	SUM(CASE WHEN model_term_enddate_group=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_terms,
	SUM(CASE WHEN model_term_enddate_group=1 AND model_term_payoff=1 THEN 1 
		     WHEN model_term_enddate_group=1 AND model_term_payoff=0 THEN 0 ELSE NULL END)AS onway_overdue_terms,
	SUM(CASE WHEN model_term_enddate_3months_group=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_3months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_terms_last_3months,
	SUM(CASE WHEN model_term_enddate_6months_group=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_6months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_terms_last_6months,
	SUM(CASE WHEN model_term_enddate_9months_group=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_9months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_terms_last_9months,
	SUM(CASE WHEN model_term_enddate_12months_group=1 AND overdue_days>0 THEN 1 
		     WHEN model_term_enddate_12months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_terms_last_12months,
	SUM(CASE WHEN model_term_enddate_group=1 THEN should_pay_amount ELSE NULL END) AS expire_term_amount,
	SUM(CASE WHEN model_term_enddate_3months_group=1 THEN should_pay_amount ELSE NULL END) AS expire_term_amount_last_3months,
	SUM(CASE WHEN model_term_enddate_6months_group=1 THEN should_pay_amount ELSE NULL END) AS expire_term_amount_last_6months,
	SUM(CASE WHEN model_term_enddate_9months_group=1 THEN should_pay_amount ELSE NULL END) AS expire_term_amount_last_9months,
	SUM(CASE WHEN model_term_enddate_12months_group=1 THEN should_pay_amount ELSE NULL END) AS expire_term_amount_last_12months,
	SUM(CASE WHEN model_term_enddate_group=1 AND overdue_days>0 THEN should_pay_amount
		     WHEN model_term_enddate_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_term_amount,
	SUM(CASE WHEN model_term_enddate_3months_group=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_3months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_term_amount_last_3months,
	SUM(CASE WHEN model_term_enddate_6months_group=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_6months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_term_amount_last_6months,
	SUM(CASE WHEN model_term_enddate_9months_group=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_9months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_term_amount_last_9months,
	SUM(CASE WHEN model_term_enddate_12months_group=1 AND overdue_days>0 THEN should_pay_amount 
		     WHEN model_term_enddate_12months_group=1 AND overdue_days<=0 THEN 0 ELSE NULL END)AS overdue_term_amount_last_12months
	FROM risk_features.model3_freeze_monitor_intermediate_bill_clean
	GROUP BY lborrowerid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_first_1term_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_first_1term_repayment
AS
	SELECT
	t1.lborrowerid,
	t1.should_pay_amount AS first_1term_amount,
	t1.model_term_enddate_group AS first_1term_observation_overdue,
	CASE WHEN t1.model_term_enddate_group=1 AND t1.overdue_days>0 THEN 1
		 WHEN t1.model_term_enddate_group=1 AND t1.overdue_days<=0 THEN 0 ELSE NULL END AS first_1term_is_overdue,
	CASE WHEN t1.model_term_enddate_group=1 AND t1.overdue_days>0 THEN t1.overdue_days ELSE NULL END AS first_1term_overdue_days,
	CASE WHEN t1.model_term_enddate_group=1 AND t1.overdue_days>0 THEN t1.should_pay_amount
		 WHEN t1.model_term_enddate_group=1 AND t1.overdue_days<=0 THEN 0 ELSE NULL END AS first_1term_overdue_amount
	FROM 
	risk_features.model3_freeze_monitor_intermediate_bill_clean t1
	JOIN 
		(
		SELECT
		lborrowerid,
		MIN(CASE WHEN model_term_begindate_group=1 THEN lborrowintentid ELSE NULL END) AS first_1st_id
		FROM
		risk_features.model3_freeze_monitor_intermediate_bill_clean
		GROUP BY lborrowerid
		)t2 ON t1.lborrowerid=t2.lborrowerid AND t1.lborrowintentid=t2.first_1st_id
	WHERE t1.ntermindex=1;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_last_1term_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_last_1term_repayment
AS
	SELECT
	t1.lborrowerid,
	t1.should_pay_amount AS last_1term_amount,
	CASE WHEN t1.model_term_enddate_group=1 AND t1.overdue_days>0 THEN 1
		 WHEN t1.model_term_enddate_group=1 AND t1.overdue_days<=0 THEN 0 ELSE NULL END AS last_1term_is_overdue,
	CASE WHEN t1.model_term_enddate_group=1 AND t1.overdue_days>0 THEN t1.overdue_days ELSE NULL END AS last_1term_overdue_days,
	CASE WHEN t1.model_term_enddate_group=1 AND t1.overdue_days>0 THEN t1.should_pay_amount
		 WHEN t1.model_term_enddate_group=1 AND t1.overdue_days<=0 THEN 0 ELSE NULL END AS last_1term_overdue_amount
	FROM 
	risk_features.model3_freeze_monitor_intermediate_bill_clean t1
	JOIN 
		(
		SELECT
		lborrowerid,
		MAX(CASE WHEN model_term_enddate_group=1 THEN lborrowintentid ELSE NULL END) AS last_1st_id
		FROM
		risk_features.model3_freeze_monitor_intermediate_bill_clean
		GROUP BY lborrowerid
		)t2 ON t1.lborrowerid=t2.lborrowerid AND t1.lborrowintentid=t2.last_1st_id
	WHERE t1.ntermindex=1;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_audit;
CREATE TABLE risk_features.model3_freeze_monitor_audit
AS
SELECT 
	t.luserid,
	t.strname,
	t.nsex,
	t.ngrantstate,
	t2.identity,
	t2.mobile AS credit_mobile,
	t3.bank_reservemobile AS credit_bank_reservemobile,
	t3.emergency_tel AS credit_emergency_tel,
	CASE WHEN SUBSTRING(t2.mobile,1,3) IN (170,171,177) THEN 1 ELSE 0 END AS mobf3_in_170171177,
	CASE WHEN SUBSTRING(t3.emergency_tel,1,3) IN (170,171,177) THEN 1 ELSE 0 END AS emergency_contact_mobf3_in_170171177,
	t3.spouse_identity,
	t3.company_duty_name,
	datediff(t3.company_job_start_date,t3.audit_time) AS working_days_of_current_job,
	t3.monthly_income,
	t3.house_type_name,
	t3.marriage,
	t3.child_count,
	t3.qq,
	t3.wechat,
	t3.education_name,
	SUBSTRING(t2.identity,1,2) AS province,
	FLOOR(DATEDIFF(m.sep_date,concat(SUBSTRING(t2.identity,7,4),'-',SUBSTRING(t2.identity,11,2),'-',SUBSTRING(t2.identity,13,2)))/365) AS age,
	CASE WHEN t4.id_credit IS NOT NULL THEN 1 ELSE 0 END AS whether_fill_property_info,
	CASE WHEN t6.luserid IS NOT NULL THEN 1 ELSE 0 END AS whether_change_mobile_number,
	CASE WHEN t2.mobile=t3.bank_reservemobile THEN 1 ELSE 0 END AS mobile_number_equal_bank_mobile,
	t3.inner_sec_code,
	t3.borrow_type_name,
	LENGTH(t3.sale_remark) AS words_of_salesman,
	LENGTH(t3.mgr_audit_reason) AS words_of_manager,
	t3.audit_time AS credit_audit_time,
	t3.approval_amount AS credit_approval_amount,
	t3.salesman_id AS credit_salesman_id,
	t3.salesman_mobile AS credit_salesman_mobile,
	t3.salesman_name AS credit_salesman_name,
	t3.dept_code AS credit_dept_code,
	t3.dept_name AS credit_dept_name,
	t.lsalesmanid AS current_salesman_id,
	t.strdeptcode AS current_dept_id,
	t.strauditprimaryid,
	t.strauditprimaryname,
	t.strauditprimaryrole,
	t.strauditseniorid,
	t.strauditseniorname,
	t.strauditseniorrole,
	t5.returned_times,
	t5.refused_times,
	t7.overdue_amount AS credit_salesman_overdue_amount,
	t7.overdue_ratio AS credit_salesman_overdue_ratio,
	t8.overdue_amount AS current_salesman_overdue_amount,
	t8.overdue_ratio AS current_salesman_overdue_ratio,
	t9.overdue_amount AS credit_dept_overdue_amount,
	t9.overdue_ratio AS credit_dept_overdue_ratio,
	t10.overdue_amount AS current_dept_overdue_amount,
	t10.overdue_ratio AS current_dept_overdue_ratio
FROM ods_yundai.tbborrower t
LEFT JOIN 
	(
	SELECT a.user_id,MIN(b.id) AS id
	FROM ods_audit_yd.t_customer a
	JOIN ods_audit_yd.t_credit b ON a.id_credit = b.id
	WHERE b.apply_type=1 AND b.credit_status=1
	GROUP BY a.user_id
	) t1 ON t.luserid = t1.user_id --这条语句取出了客户“基础授信”且“通过”的最早一笔记录
LEFT JOIN ods_audit_yd.t_customer t2 ON t1.id = t2.id_credit
LEFT JOIN ods_audit_yd.t_credit t3 ON t1.id = t3.id
LEFT JOIN (SELECT DISTINCT id_credit FROM ods_audit_yd.t_asset) t4 ON t1.id = t4.id_credit
LEFT JOIN 
	(
	SELECT 
	a.user_id,
	SUM(CASE WHEN b.credit_status=2 THEN 1 ELSE 0 END) AS returned_times,
	SUM(CASE WHEN b.credit_status=3 THEN 1 ELSE 0 END) AS refused_times
	FROM ods_audit_yd.t_customer a 
	JOIN ods_audit_yd.t_credit b ON a.id_credit = b.id
	WHERE b.apply_type=1
	GROUP BY a.user_id
	)t5 ON t.luserid = t5.user_id
LEFT JOIN 
	(
	SELECT 
	DISTINCT a.luserid 
	FROM ods_yundai.tbmobilechangehistory a
	LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON a.luserid=m.luserid
	WHERE datediff(tsrefreshtime,m.sep_date)<0
	)t6 ON t.luserid=t6.luserid
LEFT JOIN 
	(
	SELECT 
	lsalesmanid,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.model3_freeze_monitor_intermediate_intent_clean 
	GROUP BY lsalesmanid
	)t7 ON t3.salesman_id=t7.lsalesmanid
LEFT JOIN 
	(
	SELECT 
	lsalesmanid,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.model3_freeze_monitor_intermediate_intent_clean 
	GROUP BY lsalesmanid
	)t8 ON t.lsalesmanid=t8.lsalesmanid
LEFT JOIN 
	(
	SELECT 
	strdeptcode,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.model3_freeze_monitor_intermediate_intent_clean 
	GROUP BY strdeptcode
	)t9 ON t3.dept_code=t9.strdeptcode
LEFT JOIN 
	(
	SELECT 
	strdeptcode,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.model3_freeze_monitor_intermediate_intent_clean 
	GROUP BY strdeptcode
	)t10 ON t.strdeptcode=t10.strdeptcode
LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON t.luserid=m.luserid
WHERE t.ngrantstate IN (2,3);
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_repayment
AS
	SELECT 
	lborrowerid,
	SUM(model_loandate_group) AS loan_times,
	SUM(model_loandate_3months_group) AS loan_times_last_3months,
	SUM(model_loandate_6months_group) AS loan_times_last_6months,
	SUM(model_loandate_9months_group) AS loan_times_last_9months,
	SUM(model_loandate_12months_group) AS loan_times_last_12months,
	SUM(CASE WHEN model_loandate_group=1 THEN lamount ELSE NULL END) AS loan_total_amount,
	SUM(CASE WHEN model_loandate_3months_group=1 THEN lamount ELSE NULL END) AS loan_total_amount_last_3months,
	SUM(CASE WHEN model_loandate_6months_group=1 THEN lamount ELSE NULL END) AS loan_total_amount_last_6months,
	SUM(CASE WHEN model_loandate_9months_group=1 THEN lamount ELSE NULL END) AS loan_total_amount_last_9months,
	SUM(CASE WHEN model_loandate_12months_group=1 THEN lamount ELSE NULL END) AS loan_total_amount_last_12months,
	MAX(CASE WHEN model_loandate_group=1 THEN lamount ELSE NULL END) AS loan_max_amount,
	MAX(CASE WHEN model_loandate_3months_group=1 THEN lamount ELSE NULL END) AS loan_max_amount_last_3months,
	MAX(CASE WHEN model_loandate_6months_group=1 THEN lamount ELSE NULL END) AS loan_max_amount_last_6months,
	MAX(CASE WHEN model_loandate_9months_group=1 THEN lamount ELSE NULL END) AS loan_max_amount_last_9months,
	MAX(CASE WHEN model_loandate_12months_group=1 THEN lamount ELSE NULL END) AS loan_max_amount_last_12months,
	SUM(model_enddate_group) AS expire_times,
	SUM(model_enddate_3months_group) AS expire_times_last_3months,
	SUM(model_enddate_6months_group) AS expire_times_last_6months,
	SUM(model_enddate_9months_group) AS expire_times_last_9months,
	SUM(model_enddate_12months_group) AS expire_times_last_12months,
	SUM(CASE WHEN model_enddate_group=1 AND is_ontime=1 THEN 1
			 WHEN model_enddate_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_times,
	SUM(CASE WHEN model_enddate_3months_group=1 AND is_ontime=1 THEN 1
			 WHEN model_enddate_3months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_times_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 AND is_ontime=1 THEN 1
			 WHEN model_enddate_6months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_times_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 AND is_ontime=1 THEN 1
			 WHEN model_enddate_9months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_times_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND is_ontime=1 THEN 1
			 WHEN model_enddate_12months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_times_last_12months,
	SUM(CASE WHEN model_enddate_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_group=1 AND is_ontime=0 THEN 1 ELSE NULL END) AS overdue_times,
	SUM(CASE WHEN model_enddate_3months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_3months_group=1 AND is_ontime=0 THEN 1 ELSE NULL END) AS overdue_times_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_6months_group=1 AND is_ontime=0 THEN 1 ELSE NULL END) AS overdue_times_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_9months_group=1 AND is_ontime=0 THEN 1 ELSE NULL END) AS overdue_times_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_12months_group=1 AND is_ontime=0 THEN 1 ELSE NULL END) AS overdue_times_last_12months,
	SUM(CASE WHEN model_enddate_group=1 AND is_prepay=1 THEN 1
			 WHEN model_enddate_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_times,
	SUM(CASE WHEN model_enddate_3months_group=1 AND is_prepay=1 THEN 1
			 WHEN model_enddate_3months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_times_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 AND is_prepay=1 THEN 1
			 WHEN model_enddate_6months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_times_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 AND is_prepay=1 THEN 1
			 WHEN model_enddate_9months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_times_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND is_prepay=1 THEN 1
			 WHEN model_enddate_12months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_times_last_12months,	
	SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END) AS expire_total_amount,
	SUM(CASE WHEN model_enddate_3months_group=1 THEN lamount ELSE NULL END) AS expire_total_amount_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 THEN lamount ELSE NULL END) AS expire_total_amount_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 THEN lamount ELSE NULL END) AS expire_total_amount_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 THEN lamount ELSE NULL END) AS expire_total_amount_last_12months,
	MAX(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END) AS expire_max_amount,
	MAX(CASE WHEN model_enddate_3months_group=1 THEN lamount ELSE NULL END) AS expire_max_amount_last_3months,
	MAX(CASE WHEN model_enddate_6months_group=1 THEN lamount ELSE NULL END) AS expire_max_amount_last_6months,
	MAX(CASE WHEN model_enddate_9months_group=1 THEN lamount ELSE NULL END) AS expire_max_amount_last_9months,
	MAX(CASE WHEN model_enddate_12months_group=1 THEN lamount ELSE NULL END) AS expire_max_amount_last_12months,
	SUM(CASE WHEN model_enddate_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_total_amount,
	SUM(CASE WHEN model_enddate_3months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_3months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_total_amount_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_6months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_total_amount_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_9months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_total_amount_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_12months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_total_amount_last_12months,
	MAX(CASE WHEN model_enddate_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_max_amount,
	MAX(CASE WHEN model_enddate_3months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_3months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_max_amount_last_3months,
	MAX(CASE WHEN model_enddate_6months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_6months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_max_amount_last_6months,
	MAX(CASE WHEN model_enddate_9months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_9months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_max_amount_last_9months,
	MAX(CASE WHEN model_enddate_12months_group=1 AND is_ontime=1 THEN lamount
			 WHEN model_enddate_12months_group=1 AND is_ontime=0 THEN 0 ELSE NULL END) AS ontime_max_amount_last_12months,
	SUM(CASE WHEN model_enddate_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_total_amount,
	SUM(CASE WHEN model_enddate_3months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_3months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_total_amount_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_6months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_total_amount_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_9months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_total_amount_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_12months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_total_amount_last_12months,
	MAX(CASE WHEN model_enddate_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_max_amount,
	MAX(CASE WHEN model_enddate_3months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_3months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_max_amount_last_3months,
	MAX(CASE WHEN model_enddate_6months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_6months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_max_amount_last_6months,
	MAX(CASE WHEN model_enddate_9months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_9months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_max_amount_last_9months,
	MAX(CASE WHEN model_enddate_12months_group=1 AND is_ontime=1 THEN 0
			 WHEN model_enddate_12months_group=1 AND is_ontime=0 THEN lamount ELSE NULL END) AS overdue_max_amount_last_12months,
	SUM(CASE WHEN model_enddate_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_total_amount,
	SUM(CASE WHEN model_enddate_3months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_3months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_total_amount_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_6months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_total_amount_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_9months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_total_amount_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_12months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_total_amount_last_12months,
	MAX(CASE WHEN model_enddate_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_max_amount,
	MAX(CASE WHEN model_enddate_3months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_3months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_max_amount_last_3months,
	MAX(CASE WHEN model_enddate_6months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_6months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_max_amount_last_6months,
	MAX(CASE WHEN model_enddate_9months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_9months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_max_amount_last_9months,
	MAX(CASE WHEN model_enddate_12months_group=1 AND is_prepay=1 THEN lamount
			 WHEN model_enddate_12months_group=1 AND is_prepay=0 THEN 0 ELSE NULL END) AS prepay_max_amount_last_12months,
	SUM(CASE WHEN model_enddate_group=1 THEN prepay_amount ELSE NULL END) AS prepay_amount,
	SUM(CASE WHEN model_enddate_3months_group=1 THEN prepay_amount ELSE NULL END) AS prepay_amount_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 THEN prepay_amount ELSE NULL END) AS prepay_amount_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 THEN prepay_amount ELSE NULL END) AS prepay_amount_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 THEN prepay_amount ELSE NULL END) AS prepay_amount_last_12months,
	SUM(CASE WHEN model_enddate_group=1 THEN ontime_amount ELSE NULL END) AS ontime_amount,
	SUM(CASE WHEN model_enddate_3months_group=1 THEN ontime_amount ELSE NULL END) AS ontime_amount_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 THEN ontime_amount ELSE NULL END) AS ontime_amount_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 THEN ontime_amount ELSE NULL END) AS ontime_amount_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 THEN ontime_amount ELSE NULL END) AS ontime_amount_last_12months,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	SUM(CASE WHEN model_enddate_3months_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount_last_3months,
	SUM(CASE WHEN model_enddate_6months_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount_last_6months,
	SUM(CASE WHEN model_enddate_9months_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount_last_9months,
	SUM(CASE WHEN model_enddate_12months_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount_last_12months,
	SUM(model_observation_overdue1day_group) AS observation_overdue1day_times,
	SUM(model_observation_overdue5day_group) AS observation_overdue5day_times,
	SUM(model_observation_overdue10day_group) AS observation_overdue10day_times,
	SUM(model_observation_overdue30day_group) AS observation_overdue30day_times,
	SUM(model_observation_overdue60day_group) AS observation_overdue60day_times,
	SUM(model_observation_overdue90day_group) AS observation_overdue90day_times,
	SUM(CASE WHEN model_observation_overdue1day_group=1 AND enddate_plus1_payoff=0 THEN 1
	         WHEN model_observation_overdue1day_group=1 AND enddate_plus1_payoff=1 THEN 0 ELSE NULL END) AS overdue_1day_times,
	SUM(CASE WHEN model_observation_overdue5day_group=1 AND enddate_plus5_payoff=0 THEN 1
	         WHEN model_observation_overdue5day_group=1 AND enddate_plus5_payoff=1 THEN 0 ELSE NULL END) AS overdue_5day_times,
	SUM(CASE WHEN model_observation_overdue10day_group=1 AND enddate_plus10_payoff=0 THEN 1
	         WHEN model_observation_overdue10day_group=1 AND enddate_plus10_payoff=1 THEN 0 ELSE NULL END) AS overdue_10day_times,
	SUM(CASE WHEN model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN 1
	         WHEN model_observation_overdue30day_group=1 AND enddate_plus30_payoff=1 THEN 0 ELSE NULL END) AS overdue_30day_times,
	SUM(CASE WHEN model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN 1
	         WHEN model_observation_overdue60day_group=1 AND enddate_plus60_payoff=1 THEN 0 ELSE NULL END) AS overdue_60day_times,
	SUM(CASE WHEN model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN 1
	         WHEN model_observation_overdue90day_group=1 AND enddate_plus90_payoff=1 THEN 0 ELSE NULL END) AS overdue_90day_times,
	SUM(CASE WHEN model_observation_overdue1day_group=1 THEN lamount ELSE NULL END) AS observation_overdue1day_amount,
	SUM(CASE WHEN model_observation_overdue5day_group=1 THEN lamount ELSE NULL END) AS observation_overdue5day_amount,
	SUM(CASE WHEN model_observation_overdue10day_group=1 THEN lamount ELSE NULL END) AS observation_overdue10day_amount,
	SUM(CASE WHEN model_observation_overdue30day_group=1 THEN lamount ELSE NULL END) AS observation_overdue30day_amount,
	SUM(CASE WHEN model_observation_overdue60day_group=1 THEN lamount ELSE NULL END) AS observation_overdue60day_amount,
	SUM(CASE WHEN model_observation_overdue90day_group=1 THEN lamount ELSE NULL END) AS observation_overdue90day_amount,
	SUM(CASE WHEN model_observation_overdue1day_group=1 THEN lamount-enddate_plus1_amount ELSE NULL END) AS overdue1day_amount,
	SUM(CASE WHEN model_observation_overdue5day_group=1 THEN lamount-enddate_plus5_amount ELSE NULL END) AS overdue5day_amount,
	SUM(CASE WHEN model_observation_overdue10day_group=1 THEN lamount-enddate_plus10_amount ELSE NULL END) AS overdue10day_amount,
	SUM(CASE WHEN model_observation_overdue30day_group=1 THEN lamount-enddate_plus30_amount ELSE NULL END) AS overdue30day_amount,
	SUM(CASE WHEN model_observation_overdue60day_group=1 THEN lamount-enddate_plus60_amount ELSE NULL END) AS overdue60day_amount,
	SUM(CASE WHEN model_observation_overdue90day_group=1 THEN lamount-enddate_plus90_amount ELSE NULL END) AS overdue90day_amount,
	SUM(CASE WHEN model_observation_overdue1day_group=1 AND enddate_plus1_payoff=0 THEN lamount
			 WHEN model_observation_overdue1day_group=1 AND enddate_plus1_payoff=1 THEN 0 ELSE NULL END) AS overdue1day_total_amount,
	SUM(CASE WHEN model_observation_overdue5day_group=1 AND enddate_plus5_payoff=0 THEN lamount
			 WHEN model_observation_overdue5day_group=1 AND enddate_plus5_payoff=1 THEN 0 ELSE NULL END) AS overdue5day_total_amount,
	SUM(CASE WHEN model_observation_overdue10day_group=1 AND enddate_plus10_payoff=0 THEN lamount
			 WHEN model_observation_overdue10day_group=1 AND enddate_plus10_payoff=1 THEN 0 ELSE NULL END) AS overdue10day_total_amount,
	SUM(CASE WHEN model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN lamount
			 WHEN model_observation_overdue30day_group=1 AND enddate_plus30_payoff=1 THEN 0 ELSE NULL END) AS overdue30day_total_amount,
	SUM(CASE WHEN model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN lamount
			 WHEN model_observation_overdue60day_group=1 AND enddate_plus60_payoff=1 THEN 0 ELSE NULL END) AS overdue60day_total_amount,
	SUM(CASE WHEN model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN lamount
			 WHEN model_observation_overdue90day_group=1 AND enddate_plus90_payoff=1 THEN 0 ELSE NULL END) AS overdue90day_total_amount,			 
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 THEN 1 ELSE 0 END) AS observation_overdue30day_time_last_12months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN 1
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=1 THEN 0 ELSE NULL END) AS overdue_30day_times_last_12months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN lamount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=1 THEN 0 ELSE NULL END) AS overdue30day_total_amount_last_12months,
	MAX(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN lamount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=1 THEN 0 ELSE NULL END) AS overdue30day_max_total_amount_last_12months,			 
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN lamount - enddate_plus30_amount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=1 THEN 0 ELSE NULL END) AS overdue30day_amount_last_12months,	
	MAX(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN lamount - enddate_plus30_amount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue30day_group=1 AND enddate_plus30_payoff=1 THEN 0 ELSE NULL END) AS overdue30day_max_amount_last_12months,	
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 THEN 1 ELSE 0 END) AS observation_overdue60day_time_last_12months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN 1
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=1 THEN 0 ELSE NULL END) AS overdue_60day_times_last_12months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN lamount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=1 THEN 0 ELSE NULL END) AS overdue60day_total_amount_last_12months,
	MAX(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN lamount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=1 THEN 0 ELSE NULL END) AS overdue60day_max_total_amount_last_12months,			 
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN lamount - enddate_plus60_amount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=1 THEN 0 ELSE NULL END) AS overdue60day_amount_last_12months,	
	MAX(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN lamount - enddate_plus60_amount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue60day_group=1 AND enddate_plus60_payoff=1 THEN 0 ELSE NULL END) AS overdue60day_max_amount_last_12months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 THEN 1 ELSE 0 END) AS observation_overdue90day_time_last_12months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN 1
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=1 THEN 0 ELSE NULL END) AS overdue_90day_times_last_12months,
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN lamount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=1 THEN 0 ELSE NULL END) AS overdue90day_total_amount_last_12months,
	MAX(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN lamount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=1 THEN 0 ELSE NULL END) AS overdue90day_max_total_amount_last_12months,			 
	SUM(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN lamount - enddate_plus90_amount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=1 THEN 0 ELSE NULL END) AS overdue90day_amount_last_12months,	
	MAX(CASE WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN lamount - enddate_plus90_amount
			 WHEN model_enddate_12months_group=1 AND model_observation_overdue90day_group=1 AND enddate_plus90_payoff=1 THEN 0 ELSE NULL END) AS overdue90day_max_amount_last_12months,			 
	MAX(CASE WHEN model_payoff=0 AND model_enddate_group=1 THEN datediff(m.sep_date,strborrowenddate) ELSE 0 END) AS onway_overdue_days_max,
	SUM(model_loandate_group) - SUM(model_payoff) AS onway_loan_times,
	SUM(CASE WHEN model_payoff=0 AND model_enddate_group=1 THEN 1
			 WHEN model_payoff=0 AND model_enddate_group=0 THEN 0 ELSE NULL END) AS onway_overdue_times,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue1day_group=1 THEN 1
			 WHEN model_payoff=0 AND model_observation_overdue1day_group=0 THEN 0 ELSE NULL END) AS onway_overdue1day_times,	
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue5day_group=1 THEN 1
			 WHEN model_payoff=0 AND model_observation_overdue5day_group=0 THEN 0 ELSE NULL END) AS onway_overdue5day_times,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue10day_group=1 THEN 1
			 WHEN model_payoff=0 AND model_observation_overdue10day_group=0 THEN 0 ELSE NULL END) AS onway_overdue10day_times,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue30day_group=1 THEN 1
			 WHEN model_payoff=0 AND model_observation_overdue30day_group=0 THEN 0 ELSE NULL END) AS onway_overdue30day_times,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue60day_group=1 THEN 1
			 WHEN model_payoff=0 AND model_observation_overdue60day_group=0 THEN 0 ELSE NULL END) AS onway_overdue60day_times,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue90day_group=1 THEN 1
			 WHEN model_payoff=0 AND model_observation_overdue90day_group=0 THEN 0 ELSE NULL END) AS onway_overdue90day_times,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue180day_group=1 THEN 1
			 WHEN model_payoff=0 AND model_observation_overdue180day_group=0 THEN 0 ELSE NULL END) AS onway_overdue180day_times,
	SUM(CASE WHEN model_loandate_group=1 THEN model_left_amount ELSE NULL END) AS onway_amount,
	SUM(CASE WHEN model_payoff=0 AND datediff(strborrowenddate,m.sep_date)<30 AND model_enddate_group=0 THEN model_left_amount ELSE NULL END) AS onway_30day_toenddate_amount,
	SUM(CASE WHEN model_payoff=0 AND model_enddate_group=1 THEN model_left_amount 
			 WHEN model_payoff=0 AND model_enddate_group=0 THEN 0 ELSE NULL END) AS onway_overdue_amount,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue1day_group=1 THEN model_left_amount
			 WHEN model_payoff=0 AND model_observation_overdue1day_group=0 THEN 0 ELSE NULL END) AS onway_overdue1day_amount,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue5day_group=1 THEN model_left_amount
			 WHEN model_payoff=0 AND model_observation_overdue5day_group=0 THEN 0 ELSE NULL END) AS onway_overdue5day_amount,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue10day_group=1 THEN model_left_amount
			 WHEN model_payoff=0 AND model_observation_overdue10day_group=0 THEN 0 ELSE NULL END) AS onway_overdue10day_amount,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue30day_group=1 THEN model_left_amount
			 WHEN model_payoff=0 AND model_observation_overdue30day_group=0 THEN 0 ELSE NULL END) AS onway_overdue30day_amount,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue60day_group=1 THEN model_left_amount
			 WHEN model_payoff=0 AND model_observation_overdue60day_group=0 THEN 0 ELSE NULL END) AS onway_overdue60day_amount,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue90day_group=1 THEN model_left_amount
			 WHEN model_payoff=0 AND model_observation_overdue90day_group=0 THEN 0 ELSE NULL END) AS onway_overdue90day_amount,
	SUM(CASE WHEN model_payoff=0 AND model_observation_overdue180day_group=1 THEN model_left_amount
			 WHEN model_payoff=0 AND model_observation_overdue180day_group=0 THEN 0 ELSE NULL END) AS onway_overdue180day_amount,
	MAX(CASE WHEN model_enddate_12months_group=1 THEN lid ELSE NULL END) AS last_intentid,
	MIN(CASE WHEN model_enddate_group=1 THEN lid ELSE NULL END) AS first_intentid,
	MAX(CASE WHEN model_enddate_group=1 AND is_ontime=0 THEN lid ELSE NULL END) AS last_overdue_intentid,
	MIN(CASE WHEN model_enddate_group=1 AND is_ontime=0 THEN lid ELSE NULL END) AS first_overdue_intentid,
	MAX(datediff(m.sep_date,CASE WHEN model_observation_overdue1day_group=1 AND enddate_plus1_payoff=0 THEN strborrowenddate ELSE NULL END)) AS last_overdue1days_enddate_before_present,
	MAX(datediff(m.sep_date,CASE WHEN model_observation_overdue5day_group=1 AND enddate_plus5_payoff=0 THEN strborrowenddate ELSE NULL END)) AS last_overdue5days_enddate_before_present,
	MAX(datediff(m.sep_date,CASE WHEN model_observation_overdue10day_group=1 AND enddate_plus10_payoff=0 THEN strborrowenddate ELSE NULL END)) AS last_overdue10days_enddate_before_present,
	MAX(datediff(m.sep_date,CASE WHEN model_observation_overdue30day_group=1 AND enddate_plus30_payoff=0 THEN strborrowenddate ELSE NULL END)) AS last_overdue30days_enddate_before_present,
	MAX(datediff(m.sep_date,CASE WHEN model_observation_overdue60day_group=1 AND enddate_plus60_payoff=0 THEN strborrowenddate ELSE NULL END)) AS last_overdue60days_enddate_before_present,
	MAX(datediff(m.sep_date,CASE WHEN model_observation_overdue90day_group=1 AND enddate_plus90_payoff=0 THEN strborrowenddate ELSE NULL END)) AS last_overdue90days_enddate_before_present
FROM risk_features.model3_freeze_monitor_intermediate_intent_clean t1
LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON t1.lborrowerid=m.luserid
GROUP BY lborrowerid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_first_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_first_repayment
AS
SELECT
t1.lborrowerid,
t2.strloandate AS first_loandate,
t2.lamount AS first_loan_amount,
CASE WHEN t2.is_ontime=1 THEN 0 WHEN t2.is_ontime=0 THEN 1 ELSE NULL END AS first_is_overdue,
CASE WHEN t2.is_ontime=0 THEN IF(t2.overdue_days>datediff(m.sep_date,t2.strborrowenddate),datediff(m.sep_date,t2.strborrowenddate),t2.overdue_days) ELSE NULL END AS first_overdue_days,
t2.lamount - t2.ontime_amount AS first_overdue_amount,
t2.is_prepay AS first_is_prepay,
t2.prepay_days AS first_prepay_days,
CASE WHEN t2.is_prepay=1 THEN t2.lamount ELSE NULL END AS first_prepay_amount
FROM risk_features.model3_freeze_monitor_repayment t1
LEFT JOIN risk_features.model3_freeze_monitor_intermediate_intent_clean t2 ON t1.lborrowerid=t2.lborrowerid AND t1.first_intentid=t2.lid
LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON t1.lborrowerid=m.luserid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_last_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_last_repayment
AS
SELECT
t1.lborrowerid,
t2.strloandate AS last_loandate,
t2.lamount AS last_loan_amount,
CASE WHEN t2.is_ontime=1 THEN 0 WHEN t2.is_ontime=0 THEN 1 ELSE NULL END AS last_is_overdue,
CASE WHEN t2.is_ontime=0 THEN IF(t2.overdue_days>datediff(m.sep_date,t2.strborrowenddate),datediff(m.sep_date,t2.strborrowenddate),t2.overdue_days) ELSE NULL END AS last_overdue_days,
t2.lamount - t2.ontime_amount AS last_overdue_amount,
t2.is_prepay AS last_is_prepay,
t2.prepay_days AS last_prepay_days,
CASE WHEN t2.is_prepay=1 THEN t2.lamount ELSE NULL END AS last_prepay_amount
FROM risk_features.model3_freeze_monitor_repayment t1
LEFT JOIN risk_features.model3_freeze_monitor_intermediate_intent_clean t2 ON t1.lborrowerid=t2.lborrowerid AND t1.last_intentid=t2.lid
LEFT JOIN risk_features.model3_freeze_monitor_time_table m ON t1.lborrowerid=m.luserid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_last_overdue_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_last_overdue_repayment
AS
	SELECT
	t1.lborrowerid,
	t2.strloandate AS last_overdue_loandate,
	t2.lamount-t2.ontime_amount AS last_overdue_loan_amount,
	t2.lamount AS last_overdue_total_amount
FROM
risk_features.model3_freeze_monitor_repayment t1
LEFT JOIN risk_features.model3_freeze_monitor_intermediate_intent_clean t2 
ON t1.lborrowerid=t2.lborrowerid AND t1.last_overdue_intentid=t2.lid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_first_overdue_repayment;
CREATE TABLE risk_features.model3_freeze_monitor_first_overdue_repayment
AS 
	SELECT
	t1.lborrowerid,
	t2.strloandate AS first_overdue_loandate,
	t2.lamount-t2.ontime_amount AS first_overdue_loan_amount,
	t2.lamount AS first_overdue_total_amount
FROM
risk_features.model3_freeze_monitor_repayment t1
LEFT JOIN risk_features.model3_freeze_monitor_intermediate_intent_clean t2 
ON t1.lborrowerid=t2.lborrowerid AND t1.first_overdue_intentid=t2.lid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_spouse;
CREATE TABLE risk_features.model3_freeze_monitor_spouse
AS
	SELECT
	t1.luserid,
	CASE WHEN t2.luserid IS NOT NULL THEN 1 ELSE 0 END AS spouse_is_client,
	t3.loan_times AS spouse_loan_times,
	t3.loan_total_amount AS spouse_loan_total_amount,
	t3.overdue_times AS spouse_overdue_times,
	t3.onway_amount AS spouse_onway_amount,
	t4.first_1term_is_overdue AS spouse_first_1term_is_overdue
FROM
risk_features.model3_freeze_monitor_audit t1
LEFT JOIN risk_features.model3_freeze_monitor_audit t2 ON t1.spouse_identity=t2.identity
LEFT JOIN risk_features.model3_freeze_monitor_repayment t3 ON t2.luserid = t3.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_first_1term_repayment t4 ON t2.luserid = t4.lborrowerid;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_contact;
CREATE TABLE risk_features.model3_freeze_monitor_contact
AS
SELECT
	t.user_id,
	MAX(CASE WHEN t3.luserid IS NOT NULL THEN 1 ELSE 0 END) AS contact_is_client,
	SUM(t4.loan_times) AS contact_loan_times,
	SUM(t4.loan_total_amount) AS contact_loan_total_amount,
	SUM(t4.overdue_times) AS contact_overdue_times,
	SUM(t4.onway_amount) AS contact_onway_amount,
	MAX(t5.first_1term_is_overdue) AS contact_first_1term_is_overdue
FROM
	(
	SELECT a.user_id,MIN(b.id) AS id
	FROM ods_audit_yd.t_customer a
	JOIN ods_audit_yd.t_credit b ON a.id_credit = b.id
	WHERE b.apply_type=1 AND b.credit_status=1
	GROUP BY a.user_id
	)t
LEFT JOIN ods_audit_yd.t_customer t1 ON t.id=t1.id_credit 
LEFT JOIN 
	(
	SELECT
	id_credit,
	relation_name,
	regexp_replace(tel,'[^0-9]','') AS tel
	FROM
	ods_audit_yd.t_contact
	)t2 ON t.id=t2.id_credit
LEFT JOIN risk_features.model3_freeze_monitor_audit t3 ON t2.tel=t3.credit_mobile
LEFT JOIN risk_features.model3_freeze_monitor_repayment t4 ON t3.luserid = t4.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_first_1term_repayment t5 ON t3.luserid = t5.lborrowerid
WHERE t2.relation_name NOT LIKE "%本人%" AND t1.mobile <> t2.tel
GROUP BY t.user_id;
DROP TABLE IF EXISTS risk_features.model3_freeze_monitor_hive;
CREATE TABLE risk_features.model3_freeze_monitor_hive
AS
SELECT 
t1.luserid, --客户编号
t1.strname, --姓名
t1.ngrantstate, --客户目前的账户状态
t1.identity, --身份证号
t1.province, --身份证的省份
t1.spouse_identity, --配偶省份证号
t1.nsex, --性别{1:男,2:女}
t1.age, --年龄
t1.company_duty_name, --职务名称
t1.working_days_of_current_job, --基础授信时目前这份工作天数
t1.monthly_income, --月收入
t1.house_type_name, --住房类型
t1.marriage, --婚姻状态
t1.child_count, --子女数量
t1.qq, --qq号
t1.wechat, --微信号
t1.whether_fill_property_info, --是否填写资产信息
t1.education_name, --受教育水平
t1.credit_mobile, --基础授信时填写的手机号
t1.credit_bank_reservemobile, --银行卡预留手机号
t1.mobf3_in_170171177, --手机号前三位是否命中(170,171,177)
t1.whether_change_mobile_number, --是否更改过手机号
t1.mobile_number_equal_bank_mobile, --基础授信时手机号是否与银行卡预留手机号相同
t1.credit_emergency_tel, --紧急联系人手机号
t1.emergency_contact_mobf3_in_170171177, --紧急联系人手机号前三位是否命中(170,171,177)
t1.inner_sec_code, --客户来源（神秘编码）
t1.borrow_type_name, --借款类型
t1.words_of_salesman, --基础授信时销售员备注长度
t1.words_of_manager, --基础授信时经理备注长度
t1.refused_times, --基础授信通过时，曾被拒的次数
t1.returned_times, --基础授信通过时，曾被退回的次数
t1.credit_audit_time, --基础授信通过的时间
t1.credit_approval_amount, --基础授信通过时的额度
t1.credit_salesman_id, --基础授信时的业务员ID
t1.current_salesman_id, --目前的业务员ID
t1.credit_salesman_mobile, --基础授信时的业务员手机号
t1.credit_salesman_name, --基础授信时的业务员姓名
t1.current_dept_id, --目前的营业部ID
t1.credit_dept_code, --基础授信时的营业部编码
t1.credit_dept_name, --基础授信时的营业部名称
t1.strauditprimaryid, --基础授信时的初审ID
t1.strauditprimaryname, --基础授信时的初审姓名
t1.strauditprimaryrole, --基础授信时的初审职级
t1.strauditseniorid, --基础授信时的终审ID
t1.strauditseniorname, --基础授信时的终审姓名
t1.strauditseniorrole, --基础授信时的终审职级
t1.credit_salesman_overdue_ratio, --客户基础授信所属业务员本金逾期率（金额维度）
t1.credit_salesman_overdue_amount, --客户基础授信所属业务员逾期金额
t1.current_salesman_overdue_ratio, --客户目前所属业务员本金逾期率（金额维度）
t1.current_salesman_overdue_amount, --客户目前所属业务员逾期金额
t1.credit_dept_overdue_ratio, --客户基础授信所属营业部本金逾期率（金额维度）
t1.credit_dept_overdue_amount, --客户基础授信所属营业部逾期金额
t1.current_dept_overdue_ratio, --客户目前所属营业部本金逾期率（金额维度）
t1.current_dept_overdue_amount, --客户目前所属营业部逾期金额
t2.loan_times, --客户借款次数
t2.loan_times_last_3months, --客户最近3个月借款次数
t2.loan_times_last_6months, --客户最近6个月借款次数
t2.loan_times_last_9months, --客户最近9个月借款次数
t2.loan_times_last_12months, --客户最近12个月借款次数
t2.loan_total_amount, --客户总借款金额
t2.loan_total_amount_last_3months, --客户最近3个月借款金额
t2.loan_total_amount_last_6months, --客户最近6个月借款金额
t2.loan_total_amount_last_9months, --客户最近9个月借款金额
t2.loan_total_amount_last_12months, --客户最近12个月借款金额
t2.loan_max_amount, --客户单笔最大借款金额
t2.loan_max_amount_last_3months, --客户最近3个月单笔最大借款金额
t2.loan_max_amount_last_6months, --客户最近6个月单笔最大借款金额
t2.loan_max_amount_last_9months, --客户最近9个月单笔最大借款金额
t2.loan_max_amount_last_12months, --客户最近12个月单笔最大借款金额
t2.expire_times, --已到期借款的笔数
t2.expire_times_last_3months, --到期日在最近3个月内的借款的笔数
t2.expire_times_last_6months, --到期日在最近6个月内的借款的笔数
t2.expire_times_last_9months, --到期日在最近9个月内的借款的笔数
t2.expire_times_last_12months, --到期日在最近12个月内的借款的笔数
t2.ontime_times, --已到期借款的按时还清笔数
t2.ontime_times_last_3months, --到期日在最近3个月内的借款的按时还清笔数
t2.ontime_times_last_6months, --到期日在最近6个月内的借款的按时还清笔数
t2.ontime_times_last_9months, --到期日在最近9个月内的借款的按时还清笔数
t2.ontime_times_last_12months, --到期日在最近12个月内的借款的按时还清笔数
t2.overdue_times, --已到期借款的逾期笔数
t2.overdue_times_last_3months, --到期日在最近3个月内的借款的逾期笔数
t2.overdue_times_last_6months, --到期日在最近6个月内的借款的逾期笔数
t2.overdue_times_last_9months, --到期日在最近9个月内的借款的逾期笔数
t2.overdue_times_last_12months, --到期日在最近12个月内的借款的逾期笔数
t2.prepay_times, --已到期借款的提前还清笔数
t2.prepay_times_last_3months, --到期日在最近3个月内的借款的提前还清笔数
t2.prepay_times_last_6months, --到期日在最近6个月内的借款的提前还清笔数
t2.prepay_times_last_9months, --到期日在最近9个月内的借款的提前还清笔数
t2.prepay_times_last_12months, --到期日在最近12个月内的借款的提前还清笔数
t2.expire_total_amount, --已到期借款的总放款金额
t2.expire_total_amount_last_3months, --到期日在最近3个月内的借款的总放款金额
t2.expire_total_amount_last_6months, --到期日在最近6个月内的借款的总放款金额
t2.expire_total_amount_last_9months, --到期日在最近9个月内的借款的总放款金额
t2.expire_total_amount_last_12months, --到期日在最近12个月内的借款的总放款金额
t2.expire_max_amount, --已到期借款的单笔最大放款金额
t2.expire_max_amount_last_3months, --到期日在最近3个月内的借款的单笔最大放款金额
t2.expire_max_amount_last_6months, --到期日在最近6个月内的借款的单笔最大放款金额
t2.expire_max_amount_last_9months, --到期日在最近9个月内的借款的单笔最大放款金额
t2.expire_max_amount_last_12months, --到期日在最近12个月内的借款的单笔最大放款金额
t2.ontime_total_amount, --已到期借款的按时还清金额（该笔合同全部按时还清才算）
t2.ontime_total_amount_last_3months, --到期日在最近3个月内的借款的按时还清金额
t2.ontime_total_amount_last_6months, --到期日在最近6个月内的借款的按时还清金额
t2.ontime_total_amount_last_9months, --到期日在最近9个月内的借款的按时还清金额
t2.ontime_total_amount_last_12months, --到期日在最近12个月内的借款的按时还清金额
t2.ontime_max_amount, --已到期借款的单笔最大按时还清金额
t2.ontime_max_amount_last_3months, --到期日在最近3个月内的借款的单笔最大按时还清金额
t2.ontime_max_amount_last_6months, --到期日在最近6个月内的借款的单笔最大按时还清金额
t2.ontime_max_amount_last_9months, --到期日在最近9个月内的借款的单笔最大按时还清金额
t2.ontime_max_amount_last_12months, --到期日在最近12个月内的借款的单笔最大按时还清金额
t2.overdue_total_amount, --已到期借款中逾期借款的总放款金额
t2.overdue_total_amount_last_3months, --到期日在最近3个月内的逾期借款的总放款金额
t2.overdue_total_amount_last_6months, --到期日在最近6个月内的逾期借款的总放款金额
t2.overdue_total_amount_last_9months, --到期日在最近9个月内的逾期借款的总放款金额
t2.overdue_total_amount_last_12months, --到期日在最近12个月内的逾期借款的总放款金额
t2.overdue_max_amount, --已到期借款中逾期借款的单笔最大放款金额
t2.overdue_max_amount_last_3months, --到期日在最近3个月内的逾期借款的单笔最大放款金额
t2.overdue_max_amount_last_6months, --到期日在最近6个月内的逾期借款的单笔最大放款金额
t2.overdue_max_amount_last_9months, --到期日在最近9个月内的逾期借款的单笔最大放款金额
t2.overdue_max_amount_last_12months, --到期日在最近12个月内的逾期借款的单笔最大放款金额
t2.prepay_total_amount, --已到期借款中提前还清金额
t2.prepay_total_amount_last_3months, --到期日在最近3个月内的借款的提前还清金额
t2.prepay_total_amount_last_6months, --到期日在最近6个月内的借款的提前还清金额
t2.prepay_total_amount_last_9months, --到期日在最近9个月内的借款的提前还清金额
t2.prepay_total_amount_last_12months, --到期日在最近12个月内的借款的提前还清金额
t2.prepay_max_amount, --已到期借款中单笔最大提前还清金额
t2.prepay_max_amount_last_3months, --到期日在最近3个月内的借款的单笔最大提前还清金额
t2.prepay_max_amount_last_6months, --到期日在最近6个月内的借款的单笔最大提前还清金额
t2.prepay_max_amount_last_9months, --到期日在最近9个月内的借款的单笔最大提前还清金额
t2.prepay_max_amount_last_12months, --到期日在最近12个月内的借款的单笔最大提前还清金额
t2.ontime_amount, --已到期借款中按时还款金额
t2.ontime_amount_last_3months, --到期日在最近3个月内的借款的按时还款金额
t2.ontime_amount_last_6months, --到期日在最近6个月内的借款的按时还款金额
t2.ontime_amount_last_9months, --到期日在最近9个月内的借款的按时还款金额
t2.ontime_amount_last_12months, --到期日在最近12个月内的借款的按时还款金额
t2.overdue_amount, --已到期借款中逾期金额
t2.overdue_amount_last_3months, --到期日在最近3个月内的借款的逾期未还金额
t2.overdue_amount_last_6months, --到期日在最近6个月内的借款的逾期未还金额
t2.overdue_amount_last_9months, --到期日在最近9个月内的借款的逾期未还金额
t2.overdue_amount_last_12months, --到期日在最近12个月内的借款的逾期未还金额
t2.prepay_amount, --已到期借款中提前还款金额
t2.prepay_amount_last_3months, --到期日在最近3个月内的借款的提前还款金额
t2.prepay_amount_last_6months, --到期日在最近6个月内的借款的提前还款金额
t2.prepay_amount_last_9months, --到期日在最近9个月内的借款的提前还款金额
t2.prepay_amount_last_12months, --到期日在最近12个月内的借款的提前还款金额
t2.observation_overdue1day_times, --可观测逾期1天的借款笔数
t2.observation_overdue5day_times, --可观测逾期5天的借款笔数
t2.observation_overdue10day_times, --可观测逾期10天的借款笔数
t2.observation_overdue30day_times, --可观测逾期30天的借款笔数
t2.observation_overdue60day_times, --可观测逾期60天的借款笔数
t2.observation_overdue90day_times, --可观测逾期90天的借款笔数
t2.overdue_1day_times, --可观测逾期1天的借款中逾期1天笔数
t2.overdue_5day_times, --可观测逾期5天的借款中逾期5天笔数
t2.overdue_10day_times, --可观测逾期10天的借款中逾期10天笔数
t2.overdue_30day_times, --可观测逾期30天的借款中逾期30天笔数
t2.overdue_60day_times, --可观测逾期60天的借款中逾期60天笔数
t2.overdue_90day_times, --可观测逾期90天的借款中逾期90天笔数
t2.observation_overdue1day_amount, --可观测逾期1天的借款金额
t2.observation_overdue5day_amount, --可观测逾期5天的借款金额
t2.observation_overdue10day_amount, --可观测逾期10天的借款金额
t2.observation_overdue30day_amount, --可观测逾期30天的借款金额
t2.observation_overdue60day_amount, --可观测逾期60天的借款金额
t2.observation_overdue90day_amount, --可观测逾期90天的借款金额
t2.overdue1day_amount, --可观测逾期1天的借款中逾期1天未还金额
t2.overdue5day_amount, --可观测逾期5天的借款中逾期5天未还金额
t2.overdue10day_amount, --可观测逾期10天的借款中逾期10天未还金额
t2.overdue30day_amount, --可观测逾期30天的借款中逾期30天未还金额
t2.overdue60day_amount, --可观测逾期60天的借款中逾期60天未还金额
t2.overdue90day_amount, --可观测逾期90天的借款中逾期90天未还金额
t2.overdue1day_total_amount, --可观测逾期1天的借款中逾期1天的借款对应的放款金额加总
t2.overdue5day_total_amount, --可观测逾期5天的借款中逾期5天的借款对应的放款金额加总
t2.overdue10day_total_amount, --可观测逾期10天的借款中逾期10天的借款对应的放款金额加总
t2.overdue30day_total_amount, --可观测逾期30天的借款中逾期30天的借款对应的放款金额加总
t2.overdue60day_total_amount, --可观测逾期60天的借款中逾期60天的借款对应的放款金额加总
t2.overdue90day_total_amount, --可观测逾期90天的借款中逾期90天的借款对应的放款金额加总
t2.observation_overdue30day_time_last_12months, --到期日在最近12个月内的借款中，能观测到逾期30天的笔数
t2.overdue_30day_times_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的笔数
t2.overdue30day_total_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同总金额
t2.overdue30day_max_total_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同的最大金额
t2.overdue30day_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同其逾期了30天未还的金额
t2.overdue30day_max_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同其逾期了30天未还的最大金额
t2.observation_overdue60day_time_last_12months, --到期日在最近12个月内的借款中，能观测到逾期30天的笔数
t2.overdue_60day_times_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的笔数
t2.overdue60day_total_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同总金额
t2.overdue60day_max_total_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同的最大金额
t2.overdue60day_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同其逾期了30天未还的金额
t2.overdue60day_max_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同其逾期了30天未还的最大金额
t2.observation_overdue90day_time_last_12months, --到期日在最近12个月内的借款中，能观测到逾期30天的笔数
t2.overdue_90day_times_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的笔数
t2.overdue90day_total_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同总金额
t2.overdue90day_max_total_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同的最大金额
t2.overdue90day_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同其逾期了30天未还的金额
t2.overdue90day_max_amount_last_12months, --到期日在最近12个月内的借款中，逾期超过30天的合同其逾期了30天未还的最大金额
t2.onway_overdue_days_max, --客户目前未还借款中，逾期的最大天数
t2.onway_loan_times, --客户目前未还借款笔数
t2.onway_overdue_times, --客户目前未还借款中处于逾期状态的笔数
t2.onway_overdue1day_times, --客户目前未还借款中处于逾期1天状态的笔数
t2.onway_overdue5day_times, --客户目前未还借款中处于逾期5天状态的笔数
t2.onway_overdue10day_times, --客户目前未还借款中处于逾期10天状态的笔数
t2.onway_overdue30day_times, --客户目前未还借款中处于逾期30天状态的笔数
t2.onway_overdue60day_times, --客户目前未还借款中处于逾期60天状态的笔数
t2.onway_overdue90day_times, --客户目前未还借款中处于逾期90天状态的笔数
t2.onway_overdue180day_times, --客户目前未还借款中处于逾期180天状态的笔数
t2.onway_amount, --客户目前未还金额
t2.onway_30day_toenddate_amount, --借款有多少在30天内即将到期
t2.onway_overdue_amount, --客户目前未还金额中处于逾期状态的金额
t2.onway_overdue1day_amount, --客户目前未还金额中处于逾期1天状态的金额
t2.onway_overdue5day_amount, --客户目前未还金额中处于逾期5天状态的金额
t2.onway_overdue10day_amount, --客户目前未还金额中处于逾期10天状态的金额
t2.onway_overdue30day_amount, --客户目前未还金额中处于逾期30天状态的金额
t2.onway_overdue60day_amount, --客户目前未还金额中处于逾期60天状态的金额
t2.onway_overdue90day_amount, --客户目前未还金额中处于逾期90天状态的金额
t2.onway_overdue180day_amount, --客户目前未还金额中处于逾期180天状态的金额
t2.last_overdue1days_enddate_before_present, --最近一次的1天或以上逾期，应还日离现在多少天
t2.last_overdue5days_enddate_before_present, --最近一次的5天或以上逾期，应还日离现在多少天
t2.last_overdue10days_enddate_before_present, --最近一次的10天或以上逾期，应还日离现在多少天
t2.last_overdue30days_enddate_before_present, --最近一次的30天或以上逾期，应还日离现在多少天
t2.last_overdue60days_enddate_before_present, --最近一次的60天或以上逾期，应还日离现在多少天
t2.last_overdue90days_enddate_before_present, --最近一次的90天或以上逾期，应还日离现在多少天
t3.first_loandate, --到期借款中第一笔借款发生时间
t3.first_loan_amount, --到期借款中第一笔借款总金额
t3.first_is_overdue, --到期借款中第一笔借款是否逾期
t3.first_overdue_days, --到期借款中第一笔借款逾期天数
t3.first_overdue_amount, --到期借款中第一笔借款逾期金额
t3.first_is_prepay, --到期借款中第一笔借款是否提前还款
t3.first_prepay_days, --到期借款中第一笔借款提前还款天数
t3.first_prepay_amount, --到期借款中第一笔借款提前还款金额
t4.last_loandate, --到期日在最近12个月内的借款中，最近一笔借款时间
t4.last_loan_amount, --到期日在最近12个月内的借款中，最近一笔借款总金额
t4.last_is_overdue, --到期日在最近12个月内的借款中，最近一笔借款是否逾期
t4.last_overdue_days, --到期日在最近12个月内的借款中，最近一笔借款逾期天数
t4.last_overdue_amount, --到期日在最近12个月内的借款中，最近一笔借款逾期金额
t4.last_is_prepay, --到期日在最近12个月内的借款中，最近一笔借款是否提前还款
t4.last_prepay_days, --到期日在最近12个月内的借款中，最近一笔借款提前还款天数
t4.last_prepay_amount, --到期日在最近12个月内的借款中，最近一笔借款提前还款金额
t5.first_overdue_loandate, --客户最早一笔逾期借款发生时间
datediff(t5.first_overdue_loandate,t1.credit_audit_time) AS time_between_first_overdue_and_audit, --客户最早一笔逾期借款发生时间距离基础授信通过时时间间隔天数
t5.first_overdue_loan_amount, --客户最早一笔逾期借款逾期金额
t5.first_overdue_total_amount, --客户最早一笔逾期借款借款金额
round(t5.first_overdue_total_amount/t1.credit_approval_amount,2) AS first_overde_total_amount_rate, --客户最早一笔逾期借款金额占基础授信时的额度比率 
t6.last_overdue_loandate, --客户最近一笔逾期借款发生时间
t6.last_overdue_loan_amount, --客户最近一笔逾期借款逾期金额
t6.last_overdue_total_amount, --客户最近一笔逾期借款借款金额
t7.first_1term_amount, --首次借款第一期应还金额
t7.first_1term_observation_overdue, --首次借款第一期是否可观测逾期
t7.first_1term_is_overdue, --首次借款第一期是否逾期
t7.first_1term_overdue_days, --首次借款第一期逾期天数
t7.first_1term_overdue_amount, --首次借款第一期逾期金额
t8.last_1term_amount, --最近一次第一期逾期可观测的借款，第一期应还金额
t8.last_1term_is_overdue, --最近一次第一期逾期可观测的借款，第一期是否逾期
t8.last_1term_overdue_days, --最近一次第一期逾期可观测的借款，第一期逾期天数
t8.last_1term_overdue_amount, --最近一次第一期逾期可观测的借款，第一期逾期金额
t9.expire_1st_terms, --借款第一期已到期笔数
t9.expire_1st_terms_last_3months, --借款第一期到期日在最近3个月的笔数
t9.expire_1st_terms_last_6months, --借款第一期到期日在最近6个月的笔数
t9.expire_1st_terms_last_9months, --借款第一期到期日在最近9个月的笔数
t9.expire_1st_terms_last_12months, --借款第一期到期日在最近12个月的笔数
t9.overdue_1st_terms, --借款第一期已到期中逾期笔数
t9.overdue_1st_terms_last_3months, --借款第一期到期日在最近3个月的逾期笔数
t9.overdue_1st_terms_last_6months, --借款第一期到期日在最近6个月的逾期笔数
t9.overdue_1st_terms_last_9months, --借款第一期到期日在最近9个月的逾期笔数
t9.overdue_1st_terms_last_12months, --借款第一期到期日在最近12个月的逾期笔数
t9.expire_1st_term_amount, --借款第一期已到期的应还款金额
t9.expire_1st_term_amount_last_3months, --借款第一期到期日在最近3个月的应还款金额
t9.expire_1st_term_amount_last_6months, --借款第一期到期日在最近6个月的应还款金额
t9.expire_1st_term_amount_last_9months, --借款第一期到期日在最近9个月的应还款金额
t9.expire_1st_term_amount_last_12months, --借款第一期到期日在最近12个月的应还款金额
t9.overdue_1st_term_amount, --借款第一期已到期的逾期金额
t9.overdue_1st_term_amount_last_3months, --借款第一期到期日在最近3个月的逾期金额
t9.overdue_1st_term_amount_last_6months, --借款第一期到期日在最近6个月的逾期金额
t9.overdue_1st_term_amount_last_9months, --借款第一期到期日在最近9个月的逾期金额
t9.overdue_1st_term_amount_last_12months, --借款第一期到期日在最近12个月的逾期金额
t9.expire_terms, --已到期的期数（本金及利息）
t9.expire_terms_last_3months, --借款第N期到期日在最近3个月的期数
t9.expire_terms_last_6months, --借款第N期到期日在最近6个月的期数
t9.expire_terms_last_9months, --借款第N期到期日在最近9个月的期数
t9.expire_terms_last_12months, --借款第N期到期日在最近12个月的期数
t9.overdue_terms, --借款第N期已到期中逾期期数
t9.onway_overdue_terms, --客户目前未还借款中处于逾期状态的期数
t9.overdue_terms_last_3months, --借款第N期到期日在最近3个月的逾期期数
t9.overdue_terms_last_6months, --借款第N期到期日在最近6个月的逾期期数
t9.overdue_terms_last_9months, --借款第N期到期日在最近9个月的逾期期数
t9.overdue_terms_last_12months, --借款第N期到期日在最近12个月的逾期期数
t9.expire_term_amount, --借款第N期已到期的应还款金额
t9.expire_term_amount_last_3months, --借款第N期到期日在最近3个月的应还款金额
t9.expire_term_amount_last_6months, --借款第N期到期日在最近6个月的应还款金额
t9.expire_term_amount_last_9months, --借款第N期到期日在最近9个月的应还款金额
t9.expire_term_amount_last_12months, --借款第N期到期日在最近12个月的应还款金额
t9.overdue_term_amount, --借款第N期已到期的逾期金额
t9.overdue_term_amount_last_3months, --借款第N期到期日在最近3个月的逾期金额
t9.overdue_term_amount_last_6months, --借款第N期到期日在最近6个月的逾期金额
t9.overdue_term_amount_last_9months, --借款第N期到期日在最近9个月的逾期金额
t9.overdue_term_amount_last_12months, --借款第N期到期日在最近12个月的逾期金额
t10.spouse_is_client, --配偶是否属于达飞客户
t10.spouse_loan_times, --配偶借款次数
t10.spouse_loan_total_amount, --配偶借款总金额
t10.spouse_overdue_times, --配偶逾期次数
t10.spouse_onway_amount, --配偶第一次借款第一期是否逾期
t10.spouse_first_1term_is_overdue, --配偶目前在库金额
t11.contact_is_client, --联系人是否属于达飞客户
t11.contact_loan_times, --联系人借款次数
t11.contact_loan_total_amount, --联系人借款总金额
t11.contact_overdue_times, --联系人逾期次数
t11.contact_onway_amount, --联系人第一次借款第一期是否逾期
t11.contact_first_1term_is_overdue --联系人目前在库金额
FROM risk_features.model3_freeze_monitor_audit t1
LEFT JOIN risk_features.model3_freeze_monitor_repayment t2 ON t1.luserid=t2.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_first_repayment t3 ON t1.luserid=t3.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_last_repayment t4 ON t1.luserid=t4.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_first_overdue_repayment t5 ON t1.luserid=t5.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_last_overdue_repayment t6 ON t1.luserid=t6.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_first_1term_repayment t7 ON t1.luserid=t7.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_last_1term_repayment t8 ON t1.luserid=t8.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_nterms_repayment t9 ON t1.luserid=t9.lborrowerid
LEFT JOIN risk_features.model3_freeze_monitor_spouse t10 ON t1.luserid=t10.luserid
LEFT JOIN risk_features.model3_freeze_monitor_contact t11 ON t1.luserid=t11.user_id;