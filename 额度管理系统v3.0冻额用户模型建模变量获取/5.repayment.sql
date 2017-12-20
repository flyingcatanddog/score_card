-- 还款表
set hive.auto.convert.join = false;
set hive.limit.optimize.enable=true;
set hive.exec.reducers.max=100;
set hive.exec.reducers.bytes.per.reducer=500000000;
-- SET sep_date= '2017-04-01';
DROP TABLE IF EXISTS risk_features.zzr_model3_repayment;
CREATE TABLE risk_features.zzr_model3_repayment
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
FROM risk_features.zzr_model3_intermediate_intent_clean t1
LEFT JOIN risk_features.zzr_model3_time_table_0220 m ON t1.lborrowerid=m.luserid
GROUP BY lborrowerid
;

DROP TABLE IF EXISTS risk_features.zzr_model3_first_repayment;
CREATE TABLE risk_features.zzr_model3_first_repayment
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
FROM risk_features.zzr_model3_repayment t1
LEFT JOIN risk_features.zzr_model3_intermediate_intent_clean t2 ON t1.lborrowerid=t2.lborrowerid AND t1.first_intentid=t2.lid
LEFT JOIN risk_features.zzr_model3_time_table_0220 m ON t1.lborrowerid=m.luserid
;


DROP TABLE IF EXISTS risk_features.zzr_model3_last_repayment;
CREATE TABLE risk_features.zzr_model3_last_repayment
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
FROM risk_features.zzr_model3_repayment t1
LEFT JOIN risk_features.zzr_model3_intermediate_intent_clean t2 ON t1.lborrowerid=t2.lborrowerid AND t1.last_intentid=t2.lid
LEFT JOIN risk_features.zzr_model3_time_table_0220 m ON t1.lborrowerid=m.luserid
;

DROP TABLE IF EXISTS risk_features.zzr_model3_last_overdue_repayment;
CREATE TABLE risk_features.zzr_model3_last_overdue_repayment
AS
	SELECT
	t1.lborrowerid,
	t2.strloandate AS last_overdue_loandate,
	t2.lamount-t2.ontime_amount AS last_overdue_loan_amount,
	t2.lamount AS last_overdue_total_amount
FROM
risk_features.zzr_model3_repayment t1
LEFT JOIN risk_features.zzr_model3_intermediate_intent_clean t2 
ON t1.lborrowerid=t2.lborrowerid AND t1.last_overdue_intentid=t2.lid;

DROP TABLE IF EXISTS risk_features.zzr_model3_first_overdue_repayment;
CREATE TABLE risk_features.zzr_model3_first_overdue_repayment
AS 
	SELECT
	t1.lborrowerid,
	t2.strloandate AS first_overdue_loandate,
	t2.lamount-t2.ontime_amount AS first_overdue_loan_amount,
	t2.lamount AS first_overdue_total_amount
FROM
risk_features.zzr_model3_repayment t1
LEFT JOIN risk_features.zzr_model3_intermediate_intent_clean t2 
ON t1.lborrowerid=t2.lborrowerid AND t1.first_overdue_intentid=t2.lid;



DROP TABLE IF EXISTS risk_features.zzr_model3_spouse;
CREATE TABLE risk_features.zzr_model3_spouse
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
risk_features.zzr_model3_audit t1
LEFT JOIN risk_features.zzr_model3_audit t2 ON t1.spouse_identity=t2.identity
LEFT JOIN risk_features.zzr_model3_repayment t3 ON t2.luserid = t3.lborrowerid
LEFT JOIN risk_features.zzr_model3_first_1term_repayment t4 ON t2.luserid = t4.lborrowerid
;

DROP TABLE IF EXISTS risk_features.zzr_model3_contact;
CREATE TABLE risk_features.zzr_model3_contact
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
LEFT JOIN risk_features.zzr_model3_audit t3 ON t2.tel=t3.credit_mobile
LEFT JOIN risk_features.zzr_model3_repayment t4 ON t3.luserid = t4.lborrowerid
LEFT JOIN risk_features.zzr_model3_first_1term_repayment t5 ON t3.luserid = t5.lborrowerid
WHERE t2.relation_name NOT LIKE "%本人%" AND t1.mobile <> t2.tel
GROUP BY t.user_id;

DROP TABLE IF EXISTS risk_features.zzr_model3_y; 
CREATE TABLE risk_features.zzr_model3_y
AS
SELECT
lborrowerid,
CASE WHEN SUM(CASE WHEN y1_group=1 AND y1_observation_overdue60day_group=1 THEN lamount-enddate_plus60_amount ELSE NULL END)>=100000 THEN 1
	 WHEN SUM(CASE WHEN y1_group=1 AND is_ontime=1 THEN 0
	 			   WHEN y1_group=1 AND is_ontime=0 THEN 1 ELSE NULL END)=0 THEN 0 ELSE NULL END AS y1,
CASE WHEN SUM(CASE WHEN y2_group=1 AND y2_observation_overdue60day_group=1 THEN lamount-enddate_plus60_amount ELSE NULL END)>=5000 THEN 1
	 WHEN SUM(CASE WHEN y2_group=1 AND is_ontime=1 THEN 0
	 			   WHEN y2_group=1 AND is_ontime=0 THEN 1 ELSE NULL END)=0 THEN 0 ELSE NULL END AS y2,
CASE WHEN SUM(CASE WHEN y2_group=1 AND y2_observation_overdue30day_group=1 THEN lamount-enddate_plus30_amount ELSE NULL END)>=100000 THEN 1
	 WHEN SUM(CASE WHEN y2_group=1 AND is_ontime=1 THEN 0
	 			   WHEN y2_group=1 AND is_ontime=0 THEN 1 ELSE NULL END)=0 THEN 0 ELSE NULL END AS y3,
CASE WHEN SUM(CASE WHEN y2_group=1 AND y2_observation_overdue30day_group=1 THEN lamount-enddate_plus30_amount ELSE NULL END)>0 THEN 1
	 WHEN SUM(CASE WHEN y4_group=1 AND is_ontime=1 THEN 0
	 			   WHEN y4_group=1 AND is_ontime=0 THEN 1 ELSE NULL END)=0 THEN 0 ELSE NULL END AS y4,
CASE WHEN SUM(CASE WHEN y1_group=1 AND is_ontime=0 THEN 1 ELSE NULL END)>0 THEN 1
	 WHEN SUM(CASE WHEN y1_group=1 AND is_ontime=1 THEN 0 ELSE NULL END)=0 THEN 0 ELSE NULL END AS y5
FROM
risk_features.zzr_model3_intermediate_intent_clean
GROUP BY lborrowerid
;