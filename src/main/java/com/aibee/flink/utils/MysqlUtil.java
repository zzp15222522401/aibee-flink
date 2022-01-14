package com.aibee.flink.utils;

/**
 * Created by Administrator on 2017/12/24.
 */

import com.aibee.flink.cdc.FlinkCDC;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;


public class MysqlUtil {
    //    private static Connection conn = null;
//    private static PreparedStatement preparedStatement = null;
//    public static ResultSet resultSet = null;
//    public static String driver = "com.mysql.jdbc.Driver";
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDC.class);


    public static HashMap<String, String> getTablesPri(ParameterTool parameterTool) {
        LOG.info("==============================GetKafkaPartition==============================");
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        HashMap<String, String> resultMap = new HashMap<>();
        //这里我的数据库是cgjr
        String host = parameterTool.get("hostname");
        String port = parameterTool.get("port");
        String db = parameterTool.get("databases");
        String user = parameterTool.get("username");
        String password = parameterTool.get("password");
        String tableNames = parameterTool.get("tables");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + db
                + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = (Connection) DriverManager.getConnection(
                    url,
                    user,
                    password);//第三个'/'代表 'localhost:3306/'
            if(tableNames.contains(",")){
                for (String table : tableNames.split(",")) {
                    String sql = "desc "+table;
                    preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
                    resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                       if(resultSet.getString("Key").trim().toUpperCase().equals("PRI")){
                           resultMap.put(table,resultSet.getString("Field"));
                       }
                    }
                }
            }else{
                String sql = "desc "+tableNames;
                preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    if(resultSet.getString("Key").trim().toUpperCase().equals("PRI")){
                        resultMap.put(tableNames,resultSet.getString("Field"));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                //依次关闭连接
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (conn != null) {
                    conn.close();

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return resultMap;
    }

    /*
     * 主要用于提供查询返回 sink的分区数
     * */
    public static HashMap<String, String> getKafkaPartition(ParameterTool parameterTool) {
        LOG.info("==============================GetKafkaPartition==============================");
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        HashMap<String, String> resultMap = new HashMap<>();
        //这里我的数据库是cgjr
        String host = parameterTool.get("hostname");
        String port = parameterTool.get("port");
        String db = parameterTool.get("databases");
        String user = parameterTool.get("username");
        String password = parameterTool.get("password");
        String topicName = parameterTool.get("tables");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + db
                + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = (Connection) DriverManager.getConnection(
                    url,
                    user,
                    password);//第三个'/'代表 'localhost:3306/'
            String sql = "select db_table_name,kafka_partition from flink_cdc_kafka_partition_conf where  kafka_topic=?";
            preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
            preparedStatement.setString(1, topicName);

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String DbTableName = resultSet.getString("db_table_name");
                String KafkaPartition = resultSet.getString("kafka_partition");
                resultMap.put(DbTableName, KafkaPartition);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                //依次关闭连接
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (conn != null) {
                    conn.close();

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return resultMap;
    }

    /*
     * 向数据库添加
     * */
    public static HashMap insertPartitionInfo(
            ParameterTool parameterTool,
            String dbTableName,
            int partitionNum) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        HashMap<String, String> resultMap = new HashMap<>();
        //这里我的数据库是cgjr
        String host = parameterTool.get("conf_host");
        String port = parameterTool.get("conf_port");
        String db = parameterTool.get("conf_db");
        String user = parameterTool.get("conf_user");
        String password = parameterTool.get("conf_password");
        String topicName = parameterTool.get("topicid");
        String sinkDbTable = dbTableName;
        String url = "jdbc:mysql://" + host + ":" + port + "/" + db
                + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = (Connection) DriverManager.getConnection(
                    url,
                    user,
                    password);//第三个'/'代表 'localhost:3306/'\
            if (getKafkaPartition(parameterTool).get(sinkDbTable) == null) {
                String sql = "INSERT INTO " + db
                        + ".flink_cdc_kafka_partition_conf( db_table_name, kafka_partition,kafka_topic) values(?,?,?)";
                preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
                preparedStatement.setString(1, sinkDbTable);
                if (partitionNum > 1) {
                    preparedStatement.setString(
                            2,
                            getKafkaNumberPartition(getKafkaPartition(parameterTool)
                                    .values()
                                    .toArray(), partitionNum));
                } else {
                    preparedStatement.setString(2, "0");
                }
                preparedStatement.setString(3, topicName);
                preparedStatement.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                //依次关闭连接
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return resultMap;
    }

    /*
     * 根据分区数获取追加kafka分区号
     * listOne:获取到表中已经存在分区
     * */
    public static String getKafkaNumberPartition(Object[] listOne, int partitionNum) {
        if (listOne.length >= partitionNum) {
            HashMap<String, Integer> map = new HashMap<>();

            //wordcount 分区0 2,分区1次 排序取出次数最小一个
            for (Object key : listOne) {
                if (map.containsKey(key)) {
                    map.put(key.toString(), map.get(key) + 1);
                } else {
                    map.put(key.toString(), 1);
                }
            }
            Object[] obj = map.values().toArray();
            Arrays.sort(obj);
            String resultPartition = null;
            for (Object key : map.keySet()) {
                if (map.get(key).equals(obj[0])) {
                    resultPartition = key.toString();
                    break;
                }
            }
            return resultPartition;
        } else {
            ArrayList<Integer> kafkaPartitionNum = new ArrayList<>();
            for (int i = 0; i < partitionNum; i++) {
                kafkaPartitionNum.add(i);
            }
            ArrayList<Integer> mysqlPartition = new ArrayList<>();
            for (Object o : listOne) {
                mysqlPartition.add(new Integer(o.toString()));
            }
            kafkaPartitionNum.removeAll(mysqlPartition);
            Collections.sort(kafkaPartitionNum);
            if (mysqlPartition.size() == 0) {
                return "0";
            } else {
                return kafkaPartitionNum.get(0).toString();

            }
        }
    }

    /*
     * 调用入口
     * 根据传入的cdc表list动态匹配kafka分区号
     * */
    public static HashMap<String, Integer> getKafkaNumberPartition(ParameterTool parameterTool) {
        String tables = parameterTool.get("tables");
        int kafkaParitionNum = parameterTool.getInt("kafka_partition_num");
        if (tables.split(",").length > 0) {
            for (String dbTableName : tables.split(",")) {
                insertPartitionInfo(parameterTool, dbTableName.trim(), kafkaParitionNum);
            }
        } else {
            insertPartitionInfo(parameterTool, tables.trim(), kafkaParitionNum);
        }
        HashMap<String, Integer> stringIntegerHashMap = new HashMap<String, Integer>();
        for (String s : getKafkaPartition(parameterTool).keySet()) {
            if (getKafkaPartition(parameterTool).get(s) != null && s != null) {
                stringIntegerHashMap.put(
                        s,
                        Integer.valueOf(getKafkaPartition(parameterTool).get(s)));
            }
        }
        return stringIntegerHashMap;
    }

    public static void main(String[] args) {
//--conf_host
//it-m-db02.aibee.cn
//--conf_port
//3306
//--conf_db
//bi_bigdata_conf_prod
//--conf_user
//bi_bigdata_rw
//--conf_password
//K2QyYE7M#hBj987B
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        for (int i = 0; i <= 26; i++) {
//            insertPartitionInfo(parameterTool, "mall_bi_test.table" + i, 6);
//
//        }
//        HashMap<String, String> kafkaNumberPartition = getKafkaNumberPartition(parameterTool);
//        for (String s : kafkaNumberPartition.keySet()) {
//            System.out.println(s+"  "+ kafkaNumberPartition.get(s) );
//        }
//        getKafkaNumberPartition(parameterTool);

        HashMap<String, String> tablesPri = getTablesPri(parameterTool);
        for (String s : tablesPri.keySet()) {
            System.out.println(s + "   "+ tablesPri.get(s));
        }
    }
}
