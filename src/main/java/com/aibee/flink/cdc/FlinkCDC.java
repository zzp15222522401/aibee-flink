package com.aibee.flink.cdc;
//import com.ververica.cdc.connectors.mysql.MySqlSource;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;


public class FlinkCDC {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //mysql链接
        String hostname = parameterTool.get("hostname");
        String user =  parameterTool.get("user");
        String password =  parameterTool.get("password");
        String databases =  parameterTool.get("databases");
        String tables =  parameterTool.get("tables","mall_bi_test.arrival_info_20210101,mall_bi_test.arrival_info_20210308_bak,mall_bi_test.arrival_info_20210101_bak");
        int port =  parameterTool.getInt("port");

        //kafka链接
        String bootstrap =  parameterTool.get("bootstrap");
        String topicid =  parameterTool.get("topicid");
        //是否添加Schema
        boolean ifschema = parameterTool.getBoolean("ifschema",false) ;
        //启动模式
        StartupOptions startupOptions = parameterTool.get("startupOptions","latest").equals("initial") ?  StartupOptions.initial() : StartupOptions.latest();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //开启Checkpoint,每隔5秒钟做一次CK
        env.enableCheckpointing(5000L);
        //指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //设置状态后端
        String checkpointDir ="file:///tmp/flink/checkpoints";
        env.setStateBackend(new FsStateBackend(checkpointDir));



        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(user)
                .password(password)
                .databaseList(databases)
                //可选配置项,注意：指定的时候需要使用"db.table"的方式
                .tableList(tables)
                .startupOptions(startupOptions)
                .deserializer(new JsonDebeziumDeserializationSchema(ifschema))
                .build();


        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","172.24.6.129:9092,172.24.6.130:9092,172.24.6.131:9092");
        mysqlDS.addSink(new FlinkKafkaProducer<String>("flink-metric-log",new SimpleStringSchema(), properties,java.util.Optional.of(new MyPartitioner()))).name("flinkinsertkafka");


        mysqlDS.print();

        //6.执行任务
        env.execute();

    }
}
