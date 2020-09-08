package org.example.oracle;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by atyongsi@163.com on 2020/9/4
 * Description:读取excel表格,转成json并输出,这里输出.conf结尾的文件
 * 因为 dolphinScheduler需要.conf结尾的文件.
 */
public class OracleToJson {

    // 表数据量临界值
    static long criticalValue = 5000000L;

    // 3M
    static long threeMB = 3145728L;

    // 1M
    static long oneMB = 1048576L;

    public static void main(String[] args) throws IOException, InvalidFormatException {
        readExcelToConfigs("datax-generate-configs/src/main/resource/test.xlsx");
    }

    public static void readExcelToConfigs(String path) throws IOException, InvalidFormatException {
        //读取excel工作簿
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(new File(path));

        //获取工作簿下工作表的数量
        int numberOfSheets = xssfWorkbook.getNumberOfSheets();

        for (int i = 0; i < numberOfSheets; i++) {
            //读取工作簿下的工作表,第一个工作表的index为0
            XSSFSheet sheet = xssfWorkbook.getSheetAt(i);
            String sheetName = xssfWorkbook.getSheetName(i);

            int lastRowNum = sheet.getLastRowNum();//工作表最后一行
            for (int j = 1; j <= lastRowNum; j++) {//从第一行开始读
                XSSFRow row = sheet.getRow(j);//读取一行数据

                //获取输出表的名字,配置文件在此基础上加 .conf后缀
                String writeTableName = getCell(row, 17);

                String jsonConfig = oracleToJson(row);
                System.out.println(jsonConfig);

                File dir = new File("datax-generate-configs/src/main/resource/" + sheetName);
                if (!dir.exists()) {
                    dir.mkdirs();
                }
                FileWriter fileWriter = new FileWriter(new File("datax-generate-configs/src/main/resource/" + sheetName + "/" + writeTableName + ".conf"));
                fileWriter.write(jsonConfig);
                fileWriter.close();
            }

        }
    }

    private static String getCell(XSSFRow row, int num) {
        return row.getCell(num) == null ? null : row.getCell(num).toString();
    }


    // TODO  speed/bytes需要由表数据量来定
    public static String oracleToJson(XSSFRow row) {

        Map<String, Object> speedMap = new HashMap<>();
        speedMap.put("channel", 3);

        //channel默认配置3,传输速度byte根据数据量来定
        long dataTotal = getCell(row, 9) == null ? 0L : (long) Double.parseDouble(Objects.requireNonNull(getCell(row, 9)));

        if (dataTotal > criticalValue) {
            speedMap.put("byte", threeMB);
        } else {
            speedMap.put("byte", oneMB);
        }

        HashMap<String, Object> errorLimitMap = new HashMap<>();
        errorLimitMap.put("record", 0);
        errorLimitMap.put("percentage", 0.01);

        HashMap<String, Object> settingMap = new HashMap<>();
        settingMap.put("speed", speedMap);
        settingMap.put("errorLimit", errorLimitMap);

        Map readerMap = oracleReaderMap(row);
        Map writerMap = oracleWriterMap(row);

        Map<String, Object> contentMap = new HashMap<>();
        contentMap.put("reader", readerMap);
        contentMap.put("writer", writerMap);

        List<Object> contentList = new ArrayList<>();//用来构建 DataX json文件里的content
        contentList.add(contentMap);

        Map<String, Object> jobMap = new HashMap<>();
        jobMap.put("setting", settingMap);
        jobMap.put("content", contentList);

        HashMap<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("job", jobMap);

        String str = JSON.toJSONString(jsonMap);// 构造json的map生成json字符串
        JSONObject jsonObject = JSONObject.parseObject(str);// json字符串格式化
        String jsonStr = JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);

        return jsonStr;
    }


    // TODO 切分主键splitPk,需要由主键 /主键类型来定. where条件需要由增全量来定
    public static Map oracleReaderMap(XSSFRow row) {

        String readIp = getCell(row, 0);
        int readPort = getCell(row, 1) == null ? 1521 : (int) Double.parseDouble(getCell(row, 1));
        String readUsername = getCell(row, 2);
        String readPassword = getCell(row, 3);
        String readDb = getCell(row, 4);
        String readTable = getCell(row, 5);
        String readColumns = getCell(row, 6);
        String[] pkColumns = getCell(row, 7).split(",");
        String[] pkDataTypes = getCell(row, 8).split(",");

        //是否增量,true为增量
        Boolean flag = getCell(row, 10) != null && Boolean.getBoolean(getCell(row, 10));

        String incrColumn = getCell(row, 11);

        // *** reader部分 ***
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:oracle:thin:@" + readIp + ":" + readPort + "/" + readDb);
        Map<String, Object> jdbcUrlTableMap = new HashMap<>();
        List<String> jdbcList = new ArrayList<>();
        jdbcList.add(sb.toString());
        jdbcUrlTableMap.put("jdbcUrl", jdbcList);//jdbcUrl信息添加到Map,相当于DataX json文件里的jdbcUrl信息
        List<String> tableList = new ArrayList<>();
        tableList.add(readTable);
        jdbcUrlTableMap.put("table", tableList);//table信息添加到Map,相当于DataX json文件里的table信息
        List<Object> connList = new ArrayList<>();
        connList.add(jdbcUrlTableMap);//相当于DataX json文件里的connection信息
        Map<String, Object> parameterMap = new HashMap<>();
        parameterMap.put("column", readColumns.split(","));//相当于DataX json文件里的column信息
        parameterMap.put("connection", connList);//把添加connection的信息添加到Map
        parameterMap.put("username", readUsername);//相当于DataX json文件里的username信息
        parameterMap.put("password", readPassword);//相当于DataX json文件里的password信息

        // where条件不配置或者为空，视作全表同步数据。
        if (flag) {
            if (StringUtils.isNoneEmpty(incrColumn)) {
                parameterMap.put("where", String.format("%s>now() - interval '30 day'", incrColumn));
            } else {
                throw new IllegalArgumentException("增量抽取数据,需要添加增量字段!");
            }
        }

        // 数据分片
        if (pkColumns != null && pkDataTypes != null) {
            String pkColumn = null;
            for (int i = 0; i < pkDataTypes.length; i++) {
                String pkDataType = pkDataTypes[i].toUpperCase();
                if (pkDataType.contains("NUMBER") || pkDataType.contains("VARCHAR")) {
                    pkColumn = pkColumns[i];
                    break;
                }
            }
            parameterMap.put("splitPk", pkColumn);
        }

        Map<String, Object> readerMap = new HashMap<>();
        readerMap.put("name", "oraclereader");//相当于DataX json文件里的name信息
        readerMap.put("parameter", parameterMap);//相当于DataX json文件里的parameter信息

        return readerMap;
    }


    // TODO 前置sql和后置sql需要由增全量
    public static Map oracleWriterMap(XSSFRow row) {

        String[] pkColumns = getCell(row, 7).split(",");
        boolean flag = getCell(row, 10) != null && Boolean.getBoolean(getCell(row, 10));
        String writeIp = getCell(row, 12);
        int writePort = getCell(row, 13) == null ? 1521 : (int) Double.parseDouble(getCell(row, 1));
        String writeUsername = getCell(row, 14);
        String writePassword = getCell(row, 15);
        String writeDb = getCell(row, 16);
        String writeTable = getCell(row, 17);
        String writeColumns = getCell(row, 18);

        // *** writer部分 ***
        Map<String, Object> jdbcUrTableMap = new HashMap<>();
        jdbcUrTableMap.put("jdbcUrl", "jdbc:oracle:thin:@" + writeIp + ":" + writePort + "/" + writeDb);//目标jdbc信息

        Map<String, Object> parameterMap = new HashMap<>();//用来构建 DataX json文件里的parameter
        ArrayList<Object> tableList = new ArrayList<>();
        String stgTable = writeTable + "_incr_temp";// 增量临时表

        if (flag) {
            // 如果抽取增量数据,先抽到增量临时表里
            tableList.add(stgTable);
            //增量抽取先清空增量临时表
            parameterMap.put("preSql", new String[]{String.format("truncate table %s", stgTable)});
            //执行语句之后操作,更新数据做逻辑删除,新数据插入
            if (pkColumns != null && StringUtils.isNoneEmpty(pkColumns[0])) {
                List<String> params = new ArrayList<>();
                Collections.addAll(params, writeTable, stgTable);
                StringBuilder sb = new StringBuilder("delete from %s a where exists (select 1 from %s b where 1 = 1");
                for (int i = 0; i < pkColumns.length; i++) {
                    sb.append(" and a.%s = b.%s");
                    params.add(pkColumns[i]);
                    params.add(pkColumns[i]);
                }
                sb.append("),insert into %s select * from %s,");
                Collections.addAll(params, writeTable, stgTable);
                parameterMap.put("postSql", new String[]{String.format(sb.toString(), params.toArray())});
            } else {
                try {
                    throw new IllegalArgumentException("表没有主键,增量抽取数据时,把新增数据插入下游数据库,下游数据库不做删除");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    parameterMap.put("postSql", new String[]{String.format("insert into %s select * from %s", writeTable, stgTable)});
                }
            }
        } else {
            tableList.add(writeTable);
            //全量抽取的话,先清空表再抽取
            parameterMap.put("preSql", new String[]{String.format("truncate table %s", writeTable)});
        }

        jdbcUrTableMap.put("table", tableList);

        List<Object> writeConnList = new ArrayList<>();//这里注意connection是个List
        writeConnList.add(jdbcUrTableMap);

        parameterMap.put("column", writeColumns.split(","));//目标各个字段
        parameterMap.put("connection", writeConnList);//相当于DataX json文件里connection信息,注意这里是个List
        parameterMap.put("username", writeUsername);//目标用户名
        parameterMap.put("password", writePassword);//目标密码

        Map<String, Object> writeMap = new HashMap<>();
        writeMap.put("name", "oraclewriter");//相当于DataX json文件里的name信息
        writeMap.put("parameter", parameterMap);//相当于DataX json文件里的parameter信息

        return writeMap;
    }

}
