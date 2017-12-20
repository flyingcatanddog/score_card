set sep_date='2017-04-01';
set y_begin_date=${hiveconf:sep_date};
set x_end_date=DATE_SUB(${hiveconf:y_begin_date},1);
set y_end_date= add_months(${hiveconf:x_end_date},6);
set x_begin_date = add_months(${hiveconf:sep_date},-12);

DROP TABLE IF EXISTS risk_features.model3_time_table;
CREATE TABLE risk_features.model3_time_table
AS
	SELECT
		luserid,
		${hiveconf:sep_date} AS sep_date,
		${hiveconf:y_begin_date} AS y_begin_date,
		${hiveconf:x_end_date} AS x_end_date,
		${hiveconf:y_end_date} AS y_end_date,
		${hiveconf:x_begin_date} AS x_begin_date
	FROM ods_yundai.tbborrower;


set sep_date='2017-04-20';
set y_begin_date=${hiveconf:sep_date};
set x_end_date=DATE_SUB(${hiveconf:y_begin_date},1);
set y_end_date= add_months(${hiveconf:x_end_date},6);
set x_begin_date = add_months(${hiveconf:sep_date},-12);
DROP TABLE IF EXISTS risk_features.zzr_model3_time_table_0420;
CREATE TABLE risk_features.zzr_model3_time_table_0420
AS
	SELECT
		luserid,
		${hiveconf:sep_date} AS sep_date,
		${hiveconf:y_begin_date} AS y_begin_date,
		${hiveconf:x_end_date} AS x_end_date,
		${hiveconf:y_end_date} AS y_end_date,
		${hiveconf:x_begin_date} AS x_begin_date
	FROM ods_yundai.tbborrower;


set sep_date='2017-03-10';
set y_begin_date=${hiveconf:sep_date};
set x_end_date=DATE_SUB(${hiveconf:y_begin_date},1);
set y_end_date= add_months(${hiveconf:x_end_date},6);
set x_begin_date = add_months(${hiveconf:sep_date},-12);
DROP TABLE IF EXISTS risk_features.zzr_model3_time_table_0310;
CREATE TABLE risk_features.zzr_model3_time_table_0310
AS
	SELECT
		luserid,
		${hiveconf:sep_date} AS sep_date,
		${hiveconf:y_begin_date} AS y_begin_date,
		${hiveconf:x_end_date} AS x_end_date,
		${hiveconf:y_end_date} AS y_end_date,
		${hiveconf:x_begin_date} AS x_begin_date
	FROM ods_yundai.tbborrower;


set sep_date='2017-02-20';
set y_begin_date=${hiveconf:sep_date};
set x_end_date=DATE_SUB(${hiveconf:y_begin_date},1);
set y_end_date= add_months(${hiveconf:x_end_date},6);
set x_begin_date = add_months(${hiveconf:sep_date},-12);
DROP TABLE IF EXISTS risk_features.zzr_model3_time_table_0220;
CREATE TABLE risk_features.zzr_model3_time_table_0220
AS
	SELECT
		luserid,
		${hiveconf:sep_date} AS sep_date,
		${hiveconf:y_begin_date} AS y_begin_date,
		${hiveconf:x_end_date} AS x_end_date,
		${hiveconf:y_end_date} AS y_end_date,
		${hiveconf:x_begin_date} AS x_begin_date
	FROM ods_yundai.tbborrower;
	

	

DROP TABLE IF EXISTS risk_features.zzr_model3_time_table;
CREATE TABLE risk_features.zzr_model3_time_table
AS
SELECT
luserid,
sep_date,
sep_date AS y_begin_date,
date_sub(sep_date,1) AS x_end_date,
add_months(date_sub(sep_date,1),6) AS y_end_date,
add_months(sep_date,-12) AS x_begin_date
FROM
	(
	SELECT
	a.luserid,
	MAX(CASE WHEN b.should_pay_amount-b.ontime_paid_amount>0 THEN b.strborrowenddate ELSE NULL END) AS sep_date
	FROM
	ods_yundai.tbborrower a
	LEFT JOIN
		(
		SELECT
		t2.lborrowerid,
		t2.lid,
		t2.strborrowenddate,
		SUM(CASE WHEN datediff(t1.strrealrepaydate,t2.strborrowenddate)<=0 THEN t1.lprincipal ELSE 0 END) AS ontime_paid_amount,
		SUM(t1.lprincipal) AS should_pay_amount
		FROM
		ods_yundai.tbborrowerbill t1
		JOIN ods_yundai.tbborrowintent t2
		ON t1.lborrowintentid=t2.lid
		WHERE t1.nstate <> 8 AND t2.nstate IN (4,5,7,8) AND datediff(t2.strborrowenddate,${hiveconf:sep_date})<=0 AND -60<=datediff(t2.strborrowenddate,${hiveconf:sep_date})
		GROUP BY 
		t2.lborrowerid,
		t2.lid,
		t2.strborrowenddate
		)b on a.luserid=b.lborrowerid
	GROUP BY a.luserid
	)t;