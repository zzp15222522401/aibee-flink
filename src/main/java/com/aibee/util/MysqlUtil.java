package com.aibee.util;

/**
 * Created by Administrator on 2017/12/24.
 */

import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


public class MysqlUtil {
//    private static Connection conn = null;
//    private static PreparedStatement preparedStatement = null;
//    public static ResultSet resultSet = null;
//    public static String driver = "com.mysql.jdbc.Driver";

    /*
     * 主要用于提供查询返回 sink的分区数
     * */
    public static HashMap getKafkaPartition(ParameterTool parameterTool) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String driver = "com.mysql.jdbc.Driver";
        HashMap<String, String> resultMap = new HashMap<>();
        //这里我的数据库是cgjr
        String host = parameterTool.get("conf_host");
        String port = parameterTool.get("conf_port");
        String db = parameterTool.get("conf_db");
        String user = parameterTool.get("conf_user");
        String password = parameterTool.get("conf_password");
        String topicName = parameterTool.get("topic_name");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = (Connection) DriverManager.getConnection(url, user, password);//第三个'/'代表 'localhost:3306/'
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
    public static HashMap insertPartitionInfo(ParameterTool parameterTool, String dbTableName, int partitionNum) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String driver = "com.mysql.jdbc.Driver";
        HashMap<String, String> resultMap = new HashMap<>();
        //这里我的数据库是cgjr
        String host = parameterTool.get("conf_host");
        String port = parameterTool.get("conf_port");
        String db = parameterTool.get("conf_db");
        String user = parameterTool.get("conf_user");
        String password = parameterTool.get("conf_password");
        String topicName = parameterTool.get("topic_name");
        String sinkDbTable = db.trim() + "." + dbTableName.trim();
        String url = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = (Connection) DriverManager.getConnection(url, user, password);//第三个'/'代表 'localhost:3306/'\
            if (getKafkaPartition(parameterTool).get(sinkDbTable) == null) {
                String sql = "INSERT INTO bi_bigdata_conf_prod.flink_cdc_kafka_partition_conf( db_table_name, kafka_partition,kafka_topic) values(?,?,?) ";
                preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
                preparedStatement.setString(1, sinkDbTable);
                if (partitionNum > 1) {
                    preparedStatement.setString(2, getKafkaNumberPartition(getKafkaPartition(parameterTool).values().toArray(), partitionNum));
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
     * */
    public static String getKafkaNumberPartition(Object[] listOne, int partitionNum) {
        if (listOne.length >= partitionNum) {
            HashMap<String, Integer> map = new HashMap<>();

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
                    System.out.println(resultPartition);
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
        for (int i = 0; i <= 26; i++) {
            insertPartitionInfo(parameterTool, "mall_bi_test.table" + i, 6);

        }

    }
}
