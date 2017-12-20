-- 授信表
-- SET sep_date= '2017-04-01';
set hive.auto.convert.join = false;
set hive.limit.optimize.enable=true;
set hive.exec.reducers.max=100;
set hive.exec.reducers.bytes.per.reducer=500000000;

DROP TABLE IF EXISTS risk_features.zzr_model3_audit;

CREATE TABLE risk_features.zzr_model3_audit
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
	LEFT JOIN risk_features.zzr_model3_time_table_0220 m ON a.luserid=m.luserid
	WHERE datediff(tsrefreshtime,m.sep_date)<0
	)t6 ON t.luserid=t6.luserid
LEFT JOIN 
	(
	SELECT 
	lsalesmanid,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.zzr_model3_intermediate_intent_clean 
	GROUP BY lsalesmanid
	)t7 ON t3.salesman_id=t7.lsalesmanid
LEFT JOIN 
	(
	SELECT 
	lsalesmanid,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.zzr_model3_intermediate_intent_clean 
	GROUP BY lsalesmanid
	)t8 ON t.lsalesmanid=t8.lsalesmanid
LEFT JOIN 
	(
	SELECT 
	strdeptcode,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.zzr_model3_intermediate_intent_clean 
	GROUP BY strdeptcode
	)t9 ON t3.dept_code=t9.strdeptcode
LEFT JOIN 
	(
	SELECT 
	strdeptcode,
	SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END) AS overdue_amount,
	round(SUM(CASE WHEN model_enddate_group=1 THEN lamount-ontime_amount ELSE NULL END)/SUM(CASE WHEN model_enddate_group=1 THEN lamount ELSE NULL END),2) AS overdue_ratio
	FROM risk_features.zzr_model3_intermediate_intent_clean 
	GROUP BY strdeptcode
	)t10 ON t.strdeptcode=t10.strdeptcode
LEFT JOIN risk_features.zzr_model3_time_table_0220 m ON t.luserid=m.luserid
WHERE t.ngrantstate IN (2,3);