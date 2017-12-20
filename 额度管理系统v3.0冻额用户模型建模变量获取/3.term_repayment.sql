-- 合同中间表
set hive.auto.convert.join = false;
set hive.limit.optimize.enable=true;
set hive.exec.reducers.max=100;
set hive.exec.reducers.bytes.per.reducer=500000000;
-- SET sep_date= '2017-04-01';

DROP TABLE IF EXISTS risk_features.zzr_model3_intermediate_bill_clean;
CREATE TABLE risk_features.zzr_model3_intermediate_bill_clean
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
		LEFT JOIN risk_features.zzr_model3_time_table_0220 m ON a.lborrowerid=m.luserid
		WHERE nstate <> 8
		GROUP BY 
		lborrowintentid,
		ntermindex
	)t2 ON t1.lid=t2.lborrowintentid
	LEFT JOIN risk_features.zzr_model3_time_table_0220 m ON t1.lborrowerid=m.luserid
	WHERE t1.nstate IN (4,5,7,9)
;

DROP TABLE IF EXISTS risk_features.zzr_model3_nterms_repayment;
CREATE TABLE risk_features.zzr_model3_nterms_repayment
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
	FROM risk_features.zzr_model3_intermediate_bill_clean
	GROUP BY lborrowerid
;

DROP TABLE IF EXISTS risk_features.zzr_model3_first_1term_repayment;
CREATE TABLE risk_features.zzr_model3_first_1term_repayment
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
	risk_features.zzr_model3_intermediate_bill_clean t1
	JOIN 
		(
		SELECT
		lborrowerid,
		MIN(CASE WHEN model_term_begindate_group=1 THEN lborrowintentid ELSE NULL END) AS first_1st_id
		FROM
		risk_features.zzr_model3_intermediate_bill_clean
		GROUP BY lborrowerid
		)t2 ON t1.lborrowerid=t2.lborrowerid AND t1.lborrowintentid=t2.first_1st_id
	WHERE t1.ntermindex=1
;

DROP TABLE IF EXISTS risk_features.zzr_model3_last_1term_repayment;
CREATE TABLE risk_features.zzr_model3_last_1term_repayment
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
	risk_features.zzr_model3_intermediate_bill_clean t1
	JOIN 
		(
		SELECT
		lborrowerid,
		MAX(CASE WHEN model_term_enddate_group=1 THEN lborrowintentid ELSE NULL END) AS last_1st_id
		FROM
		risk_features.zzr_model3_intermediate_bill_clean
		GROUP BY lborrowerid
		)t2 ON t1.lborrowerid=t2.lborrowerid AND t1.lborrowintentid=t2.last_1st_id
	WHERE t1.ntermindex=1
;


