package com.aibee.flink.cdc;
//import com.ververica.cdc.connectors.mysql.MySqlSource;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;


public class FlinkCDC {

    public static void main(String[] args) throws Exception {


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

        //my
        String hostname = "172.16.244.60";
        String user = "root";
        String password = "123456";
        String databases = "store_bi_system";
        String tables = "store_bi_system.score,store_bi_system.common_domain";


        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(3306)
                .username(user)
                .password(password)
                .databaseList(databases)
                //可选配置项,注意：指定的时候需要使用"db.table"的方式
                .tableList()
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","172.24.6.129:9092,172.24.6.130:9092,172.24.6.131:9092");
//        mysqlDS.addSink(new FlinkKafkaProducer<String>("flink-metric-log",new SimpleStringSchema(), properties,java.util.Optional.of(new MyPartitioner()))).name("flinkinsertkafka");


        mysqlDS.print();

        //6.执行任务
        env.execute();

    }
}
