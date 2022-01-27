package com.aibee.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Properties;

import static com.aibee.util.MysqlUtil.getKafkaNumberPartition;


public class FlinkCDC {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCDC.class);
//    public static HashMap<String, String> kafkaNumberPartitionMap;

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //获取kafka分区与对应表分区号写入配置库

        HashMap<String, Integer>  kafkaNumberPartitionMap = getKafkaNumberPartition(parameterTool);
        Properties properties = new Properties();
        properties.setProperty("bigint.unsigned.handling.mode", "long");
        //mysql链接
        String hostname = parameterTool.get("hostname");
        String user = parameterTool.get("user");
        String password = parameterTool.get("password");
        String databases = parameterTool.get("databases");
        String tables = parameterTool.get("tables");
        String serverId = parameterTool.get("serverid");
        int parallelism = parameterTool.getInt("parallelism_env", 1);
        int port = parameterTool.getInt("port");

        //kafka链接
        String bootstrap = parameterTool.get("bootstrap");
        String topicid = parameterTool.get("topicid");
        //是否添加Schema
        boolean ifschema = parameterTool.getBoolean("hiveschema", false);
        //启动模式
        StartupOptions startupOptions = parameterTool.get("startupOptions", "latest").equals("initial") ? StartupOptions.initial() : StartupOptions.latest();
        //保存检查点个数
        Configuration configuration = new Configuration();
        configuration.setInteger("state.checkpoints.num-retained", 3);
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        System.out.println("serverid: " + serverId);
        env.setParallelism(parallelism);
        LOG.info("parallelism_env:  " + parallelism);
        LOG.info("serverid:  " + serverId);
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
                .debeziumProperties(properties)
                //可选配置项,注意：指定的时候需要使用"db.table"的方式
                .tableList(tables)
                .startupOptions(startupOptions)
//                .deserializer(new JsonDebeziumDeserializationSchema(ifschema))
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer((DebeziumDeserializationSchema) new StructDebeziumDeserializationSchema("UTC"))
                .build();


        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        mysqlDS.setParallelism(parallelism)
                .addSink(new FlinkKafkaProducer<String>(topicid, new SimpleStringSchema(), props, java.util.Optional.of(new MyPartitioner(kafkaNumberPartitionMap)))).name("flinkinsertkafka");
//
        env.execute();

    }
}
//提交命令
//$FLINK_HOME/bin/flink run -m yarn-cluster -yqu root.data-platform -yd -yjm 6144m -ytm 8192m -ynm flink-mysqlcdc-stream_sink_kafka_test  -ys 6  -c com.aibee.flink.cdc.FlinkCDC /opt/bigdata/scpg/aibee-flink-1.0-SNAPSHOT-v1.jar --hostname manage16.aibee.cn --user bi_app_w --password 8g6Ftv5OP2qm --port 3307 --databases mall_bi_test --tables mall_bi_test.arrival_info_20210308_bak,mall_bi_test.arrival_info_20210101,mall_bi_test.arrival_info_20210101_bak,mall_bi_test.arrival_info_20210308,mall_bi_test.arrival_info_20210505,mall_bi_test.arrival_info_20210508,mall_bi_test.arrival_info_20210203,mall_bi_test.arrival_info_20210807,mall_bi_test.arrival_info_20210711 --bootstrap 172.24.6.129:9092,172.24.6.130:9092,172.24.6.131:9092 --topicid flink-metric-log --serverid 5405-5409 --startupOptions initial --parallelism_env 6 --conf_host it-m-db02.aibee.cn --conf_port 3306 --conf_db bi_bigdata_conf_prod --conf_user bi_bigdata_rw --conf_password K2QyYE7M#hBj987B --kafka_partition_num 6