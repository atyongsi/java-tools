package org.example.postgresql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by atyongsi@163.com on 2020/8/24
 * Description:Have Not Yet
 */

public class PostgresqlToJson {

    public static Map postgreSqlReaderMap(String[] readInfo, Boolean flag) {

        String read_ip = readInfo[0];
        String read_port = readInfo[1];
        String read_username = readInfo[2];
        String read_password = readInfo[3];
        String read_schema = readInfo[4];
        String read_table = readInfo[5];
        String read_columns = readInfo[6];
        String add_column = readInfo[7];
        String splitPk = readInfo[8];

        //******** reader部分 ********
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:postgresql://" + read_ip + ":" + read_port + "/" + read_schema);
//        sb.append("?autoReconnect=true");//开启自动重连，防止连接时间短超时
        Map<String, Object> jdbcUrlTableMap = new HashMap<>();
        List<String> jdbcList = new ArrayList<>();
        jdbcList.add(sb.toString());
        jdbcUrlTableMap.put("jdbcUrl", jdbcList);//jdbcUrl信息添加到Map,相当于DataX json文件里的jdbcUrl信息
        List<String> tableList = new ArrayList<>();
        tableList.add(read_table);
        jdbcUrlTableMap.put("table", tableList);//table信息添加到Map,相当于DataX json文件里的table信息
        List<Object> connList = new ArrayList<>();
        connList.add(jdbcUrlTableMap);//相当于DataX json文件里的connection信息
        Map<String, Object> parameterMap = new HashMap<>();
        parameterMap.put("column", read_columns.split(","));//相当于DataX json文件里的column信息
        parameterMap.put("connection", connList);//把添加connection的信息添加到Map
        parameterMap.put("username", read_username);//相当于DataX json文件里的username信息
        parameterMap.put("password", read_password);//相当于DataX json文件里的password信息
        parameterMap.put("where", "1=1");// where条件不配置或者为空，视作全表同步数据。
        if (flag) {   // 如果是增量同步数据,需要修改where条件
            if (StringUtils.isNoneEmpty(add_column)) {
                parameterMap.put("where", String.format("%s>=now() - interval '2 day'", add_column));
            } else {
                throw new IllegalArgumentException("增量抽取数据,需要添加增量字段!");
            }
        }

        if (StringUtils.isNoneEmpty(splitPk)) {
            parameterMap.put("splitPk", splitPk);//相当于DataX json文件里的splitPk信息
        }

        Map<String, Object> readerMap = new HashMap<>();
        readerMap.put("name", "postgresqlreader");//相当于DataX json文件里的name信息
        readerMap.put("parameter", parameterMap);//相当于DataX json文件里的parameter信息

        return readerMap;
    }

    //******** writer部分 ********
    public static Map postgreSqlWriterMap(String[] writeInfo, Boolean flag) {

        String write_ip = writeInfo[0];
        String write_port = writeInfo[1];
        String write_username = writeInfo[2];
        String write_password = writeInfo[3];
        String write_schema = writeInfo[4];
        String write_table = writeInfo[5];// 已经存在的全量表
        String write_columns = writeInfo[6];
        String add_column = writeInfo[7];// 增量字段
        String pk_columns = writeInfo[8];// 主键

        Map<String, Object> jdbcUrTableMap = new HashMap<>();
        jdbcUrTableMap.put("jdbcUrl", "jdbc:postgresql://" + write_ip + ":" + write_port + "/" + write_schema);//目标jdbc信息

        Map<String, Object> parameterMap = new HashMap<>();//用来构建 DataX json文件里的parameter
        ArrayList<Object> tableList = new ArrayList<>();
        String stg_table = write_table + "_stg";// 增量临时表
        if (flag) {
            tableList.add(stg_table);// 如果抽取增量数据,先抽到增量临时表里
            parameterMap.put("preSql", new String[]{String.format("truncate table %s;", stg_table)});//执行语句之前,先清空增量临时表
            if (StringUtils.isNoneEmpty(pk_columns) && StringUtils.isNoneEmpty(add_column)) {//执行语句之后操作
                parameterMap.put("postSql", new String[]{String.format("delete from %s a where exists (select 1 from %s b where a.%s=b.%s);insert into %s select * from %s;", write_table, stg_table, pk_columns, pk_columns, write_table, stg_table)});
            } else {
                parameterMap.put("postSql", new String[]{String.format("delete from %s;insert into %s select * from %s;", write_table, write_table, stg_table)});
            }
        } else {
            tableList.add(write_table);
            parameterMap.put("preSql", new String[]{String.format("truncate table %s;", write_table)});//执行语句之前操作
        }

        jdbcUrTableMap.put("table", tableList);

        List<Object> writeConnList = new ArrayList<>();//这里注意connection是个List
        writeConnList.add(jdbcUrTableMap);

        parameterMap.put("column", write_columns.split(","));//目标各个字段
        parameterMap.put("connection", writeConnList);//相当于DataX json文件里connection信息,注意这里是个List
        parameterMap.put("username", write_username);//目标用户名
        parameterMap.put("password", write_password);//目标密码

        Map<String, Object> writeMap = new HashMap<>();
        writeMap.put("name", "postgresqlwriter");//相当于DataX json文件里的name信息
        writeMap.put("parameter", parameterMap);//相当于DataX json文件里的parameter信息

        return writeMap;
    }

    public static String postgreSqlToJson(String[] readInfo, String[] writeInfo, Boolean flag, int channel) {
        Map postgreSqlReaderMap = postgreSqlReaderMap(readInfo, flag);
        Map postgreSqlWriterMap = postgreSqlWriterMap(writeInfo, flag);

        Map<String, Object> contentMap = new HashMap<>();
        contentMap.put("reader", postgreSqlReaderMap);
        contentMap.put("writer", postgreSqlWriterMap);

        List<Object> contentList = new ArrayList<>();//用来构建 DataX json文件里的content
        contentList.add(contentMap);

        Map<String, Object> channelMap = new HashMap<>();
        channelMap.put("channel", channel);

        Map<String, Object> speedMap = new HashMap<>();
        speedMap.put("speed", channelMap);

        Map<String, Object> jobMap = new HashMap<>();
        jobMap.put("setting", speedMap);
        jobMap.put("content", contentList);

        HashMap<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("job", jobMap);

        String str = JSON.toJSONString(jsonMap);// 构造json的map生成json字符串
        JSONObject jsonObject = JSONObject.parseObject(str);// json字符串格式化
        String jsonStr = JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);

        return jsonStr;
    }

}
