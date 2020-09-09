
-- 表的元数据信息
SELECT
	utc.TABLE_NAME,--相当于excel中的read_table
	coltab.fileds,--相当于excel中的read_columns
	pktab.pk_columns,--相当于excel中的pk_columns
	pktab.pk_data_type,--相当于excel中的pk_data_type
	ut.NUM_ROWS --相当于excel中的num_rows
	
FROM
	USER_TAB_COLS utc
	JOIN USER_TABLES ut ON ut.TABLE_NAME = utc.TABLE_NAME
	JOIN (
SELECT
	utc.TABLE_NAME,
	to_char(
	wm_concat ( utc.COLUMN_NAME )) fileds 
FROM
	USER_TAB_COLS utc
	JOIN USER_TABLES ut ON ut.TABLE_NAME = utc.TABLE_NAME
WHERE
	utc.TABLE_NAME IN ( 'thispendsettle','thisspecstock','thisbusinesssummary','tfullstockinfo' )	 
GROUP BY
	utc.TABLE_NAME 
	) coltab ON coltab.TABLE_NAME = utc.TABLE_NAME
	JOIN (
SELECT
	ucc.table_name,
	to_char(
	wm_concat ( ucc.column_name )) pk_columns,
	to_char(
	wm_concat ( a.DATA_TYPE )) pk_data_type 
FROM
	user_constraints uc,
	user_cons_columns ucc
	JOIN USER_TAB_COLS a ON a.TABLE_NAME = ucc.TABLE_NAME 
	AND a.column_name = ucc.column_name 
WHERE
	uc.constraint_name = ucc.constraint_name 
	AND uc.constraint_type = 'P' AND
	ucc.TABLE_NAME IN ( 'thispendsettle','thisspecstock','thisbusinesssummary','tfullstockinfo' )
GROUP BY
	ucc.table_name 
	) pktab ON pktab.table_name = utc.table_name 
WHERE
	utc.TABLE_NAME IN ( 'thispendsettle','thisspecstock','thisbusinesssummary','tfullstockinfo' ) 
GROUP BY
	utc.TABLE_NAME,
	coltab.fileds,
	pktab.pk_columns,
	pktab.pk_data_type,
	ut.NUM_ROWS



-- 常用系统表：USER_TAB_COLS,USER_TABLES,USER_TAB_COMMENTS,USER_COL_COMMENTS,




