package com.aibee.flink.sql;

import com.aibee.flink.sql.utils.FileOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author ZhangChangjun
 * 外部SQL执行器，用于批量执行外部传入的SQL脚本。
 */
public class FlinkSQLExecutor {
    public static void main(String[] args) {
        //创建EnvironmentSettings，默认为BlinkPlanner,isStreamingMode
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().build();
        // 基于setting创建tableEnv
        TableEnvironment tableEnv = TableEnvironment.create(bsSettings);

        // 获取用户传递的参数
        ParameterTool params = ParameterTool.fromArgs(args);
        String sqlPath = params.getRequired("sql-path");
        String sqlDelimiter = params.get("sql-delimiter",";");//默认多个SQL间以；分割

        // 读取SQL文件中的内容
        String strSQLs = FileOperator.readTextFile(sqlPath);
        //TODO 增加SQL中参数替换功能

        //根据分号分割，区别多个SQL
        String[] sqlStrs = strSQLs.split(sqlDelimiter);
        //创建 StatementSet 用于执行 DML 语句
        StatementSet statementSet = tableEnv.createStatementSet();
        for(String exeSQL:sqlStrs){
            String trimSQL = exeSQL.trim();//去除首尾空格
            if(trimSQL.length()>0&&!trimSQL.startsWith("--")){
                if(trimSQL.toLowerCase().startsWith("insert into")){//如果是insert开头，执行addInsert否则，执行executeSql
                    statementSet.addInsertSql(trimSQL);
                }else{
                    tableEnv.executeSql(exeSQL);
                }
            }
        }
        //执行sinks
        statementSet.execute();

    }



}
