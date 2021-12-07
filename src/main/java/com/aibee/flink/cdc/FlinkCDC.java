package com.aibee.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;


import java.util.Properties;


public class FlinkCDC {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //mysql链接
        String hostname = parameterTool.get("hostname");
        String user =  parameterTool.get("user");
        String password =  parameterTool.get("password");
        String databases =  parameterTool.get("databases");
        String tables =  parameterTool.get("tables");
        int port =  parameterTool.getInt("port");

        //kafka链接
        String bootstrap =  parameterTool.get("bootstrap");
        String topicid =  parameterTool.get("topicid");
        //是否添加Schema
        boolean ifschema = parameterTool.getBoolean("hiveschema",false) ;
        //启动模式
        StartupOptions startupOptions = parameterTool.get("startupOptions","latest").equals("initial") ?  StartupOptions.initial() : StartupOptions.latest();
        //保存检查点个数
        Configuration configuration = new Configuration();
        configuration.setInteger("state.checkpoints.num-retained",3);
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        //开启Checkpoint,每隔5分钟做一次CK
        env.enableCheckpointing(300000L);
        //指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));



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
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        mysqlDS.addSink(new FlinkKafkaProducer<String>(topicid,new SimpleStringSchema(), props,java.util.Optional.of(new MyPartitioner()))).name("flinkinsertkafka");

        env.execute();

    }
}
