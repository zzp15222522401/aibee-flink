package com.aibee.flink.sql;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySQLSourceTest {
    public static void main(String[] args) throws Exception {

        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("data-db01.aibee.cn")
                .port(3306)
                .databaseList("cjzhang_test") // monitor all tables under inventory database
                .username("openfaasdb_user")
                .password("5apj5R7s$y75S8z3")
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
