import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by atyongsi@163.com on 2020/8/24
 * Description:Have Not Yet
 */


//ip|port|username|password|schema|table|target_table|splitPk|pk_column|columns|add_column
public class PostgresqlToJson {

    public static Map PostgresqlReaderMap(String[] info, String flag) {

        String read_ip = info[0];
        String read_port = info[1];
        String read_username = info[2];
        String read_password = info[3];
        String read_schema = info[4];
        String read_table = info[5];
        String read_allColumn = info[6];
        String splitPk = info[7];
        if ("null".equals(splitPk.toLowerCase())) {
            splitPk = null;
        }
        String pkColumn = info[8];
        if ("null".equals(pkColumn.toLowerCase())) {
            pkColumn = null;
        }
        String addColumn = null;//增量字段
        if (info.length > 10) {
            addColumn = info[10];
        }

        //******** reader部分 ********
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:postgresql://" + read_ip + ":" + read_port + "/"+read_schema);
        sb.append("?autoReconnect=true");//开启自动重连，防止连接时间短超时
        Map<String, Object> jdbcUrlTableMap = new HashMap<>();
        List<String> jdbcList = new ArrayList<>();
        jdbcList.add(sb.toString());//考虑到分库的场景,读取的数据源可能有多个
        jdbcUrlTableMap.put("jdbcUrl", jdbcList);//jdbcUrl信息添加到Map,相当于DataX json文件里的jdbcUrl信息
        List<String> tableList = new ArrayList<>();
        tableList.add(read_table);//考虑到水平分表的场景,可能在一个库里读取多张表
        jdbcUrlTableMap.put("table", tableList);//table信息添加到Map,相当于DataX json文件里的table信息
        List<Object> connList = new ArrayList<>();
        connList.add(jdbcUrlTableMap);//相当于DataX json文件里的connection信息
        Map<String, Object> parameterMap = new HashMap<>();
        parameterMap.put("column", read_allColumn.split(","));//相当于DataX json文件里的column信息
        parameterMap.put("connection", connList);//把添加connection的信息添加到Map
        parameterMap.put("username", read_username);//相当于DataX json文件里的username信息
        parameterMap.put("password", read_password);//相当于DataX json文件里的password信息
        parameterMap.put("where", "1=1");// where条件不配置或者为空，视作全表同步数据。
        if (StringUtils.isNoneEmpty(splitPk)) {
            parameterMap.put("splitPk", splitPk);//相当于DataX json文件里的splitPk信息
        }
        if ("add".equals(flag)) {// 如果是增量同步数据,需要修改where条件
            if (StringUtils.isNoneEmpty(addColumn)) {
                String[] addCol = addColumn.split(";");
                String add1 = addCol[0];
                String add2 = addCol[1];
                parameterMap.put("where", String.format("%s>=now() - interval '2 day' or %s>=now() - interval '2 day'", add1, add2));
            }
        }
        Map<String, Object> readerMap = new HashMap<>();
        readerMap.put("name", "postgresqlreader");//相当于DataX json文件里的name信息
        readerMap.put("parameter", parameterMap);//相当于DataX json文件里的parameter信息
        Map<String, Object> map = new HashMap<>();
        map.put("reader", readerMap);

        return map;
    }

    //******** writer部分 ********
    public static Map PostgresqlWriterMap(String[] writeInfo,String flag) {

        String writeIp = writeInfo[0];
        String write_port = writeInfo[1];
        String write_username = writeInfo[2];
        String write_password = writeInfo[3];
        String write_schema = writeInfo[4];
        String write_table = writeInfo[5];


        Map<String, Object> m2 = new HashMap<>();
        m2.put("jdbcUrl", "jdbc:postgresql://192.168.66.94:5434/dw");//目标jdbc信息
        List<String> wtableList = new ArrayList<>();
        String ods_table = target_table;//全量表
        List<Object> wconnList = new ArrayList<>();
        wconnList.add(m2);
        Map<String, Object> mm2 = new HashMap<>();
        mm2.put("column", allColumns.split(","));//目标各个字段
        mm2.put("connection", wconnList);//目标连接信息
        mm2.put("username", "aaaaa");//目标用户名
        mm2.put("password", "123456");//目标密码
        String stg_table = target_table + "_stg";//增量临时表
        if ("add".equals(flag)) {
            wtableList.add(stg_table);
            mm2.put("preSql", new String[]{String.format("truncate table %s;", stg_table)});//执行语句之前操作
            if (StringUtils.isNoneEmpty(pkColumn) && StringUtils.isNoneEmpty(addColumn)) {//执行语句之后操作
                mm2.put("postSql", new String[]{String.format("delete from %s a where exists (select 1 from %s b where a.%s=b.%s);insert into %s select * from %s;", ods_table, stg_table, pkColumn, pkColumn, ods_table, stg_table)});
            } else {
                mm2.put("postSql", new String[]{String.format("delete from %s;insert into %s select * from %s;", ods_table, ods_table, stg_table)});
            }
        } else {
            wtableList.add(ods_table);
            mm2.put("preSql", new String[]{String.format("truncate table %s;", ods_table)});//执行语句之前操作
        }
        m2.put("table", wtableList);//目标表名
        Map<String, Object> mmm2 = new HashMap<>();
        mmm2.put("name", "postgresqlwriter");//目标数据源
        mmm2.put("parameter", mm2);//目标参数
        mmmm1.put("writer", mmm2);

        List<Object> contentList = new ArrayList<>();
        contentList.add(mmmm1);
        Map<String, Object> m3 = new HashMap<>();
        m3.put("content", contentList);

        Map<String, Object> m4 = new HashMap<>();
        m4.put("channel", "10");
        Map<String, Object> mm4 = new HashMap<>();
        mm4.put("speed", m4);
        m3.put("setting", mm4);

        Map<String, Object> m5 = new HashMap<>();
        m5.put("job", m3);

        return null;
    }

}
