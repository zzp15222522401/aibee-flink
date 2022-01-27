package com.aibee.flink.sql;

import com.aibee.flink.sql.utils.FileOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSql {
    public static void main(String[] args) {
        //创建EnvironmentSettings，默认为BlinkPlanner,isStreamingMode
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().build();
        // 基于setting创建tableEnv

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        // 获取用户传递的参数
        ParameterTool params = ParameterTool.fromArgs(args);
        String sqlPath = params.getRequired("sql-path");
        String sqlDelimiter = params.get("sql-delimiter",";");//默认多个SQL间以；分割
        // 读取SQL文件中的内容
        String strSQLs = FileOperator.readTextFile(sqlPath);
        //根据分号分割，区别多个SQL
        String[] sqlStrs = strSQLs.split(sqlDelimiter);
        for(String exeSQL:sqlStrs){
            String trimSQL = exeSQL.trim();//去除首尾空格
            if(trimSQL.length()>0&&!trimSQL.startsWith("--")){
                tableEnv.executeSql(exeSQL);
            }
        }

    }
}
