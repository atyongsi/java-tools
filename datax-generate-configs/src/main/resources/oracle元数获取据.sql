-- DataX做为数据同步的工具，在抽取数据时通过配置json文件来定义作业，如果抽取的表很多，配置json文件是个体力活。我们通过java程序来自动生成json文件

--倒推思路：1、java程序读取excel文件，生成json文件 2、定义excel文件的内容，即配置json文件时所需要的信息 3、从数据库中获取表的元数据信息(通过sql获取)

-- excel文件格式如下：
-- 上游数据库ip 端口号 用户名 密码 上游数据库名 上游表名 输入的列 主键 主键的数据类型 表数据量 是否增量(增量为true/false) 增量字段 下游数据库ip 端口号 用户名 密码 下游数据库名 下游表名 输出的列
-- read_ip read_port read_username read_password read_db read_table	read_columns pk_columns pk_data_type num_rows incr_whether incr_column write_ip write_port write_username write_password write_db write_table write_columns

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
	utc.TABLE_NAME IN ( 'thispendsettle','thisspecstock','thisbusinesssummary','tfullstockinfo' )	
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




