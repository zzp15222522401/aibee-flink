package com.aibee.flink.transfer;

import com.aibee.flink.transfer.StructDebeziumDeserializationSchema;
import com.aibee.flink.cdc.TableIdPartitioner;
import com.aibee.flink.utils.MysqlUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
//com.aibee.flink.transfer.MySQL2KafkaTask

/**
 * MySQL 写Kafka 工具类
 * --local-run
 * true
 * --hostname
 * manage16.aibee.cn
 * --username
 * bi_app_w
 * --password
 * 8g6Ftv5OP2qm
 * --port
 * 3307
 * --serverid
 * 5500-6400
 * --databases
 * mall_bi_test
 * --tables
 * mall_bi_test.mall_heat_map,mall_bi_test.mall_customer_event,mall_bi_test.mall_customer_preference,mall_bi_test.mall_daily_action_index,mall_bi_test.mall_customer_info,mall_bi_test.mall_rt_heatmap_action_index,mall_bi_test.mall_stay_time,mall_bi_test.mall_customer_daily_record,mall_bi_test.mall_cross_daily_time_slice_action_index,mall_bi_test.mall_cross_statistic
 * --startupOptions
 * initial
 * --kafka.bootstrap.servers 172.24.6.129:9092,172.24.6.130:9092,172.24.6.131:9092
 * --kafka.topic store-bi-flink-log
 */
public class MySQL2KafkaTask {
    private static final Logger LOG = LoggerFactory.getLogger(MySQL2KafkaTask.class);

    public static void main(String[] args) throws Exception {
        /**
         *  debezium相关参数，使用debezium.xxxx
         *  kafka 相关参数，使用 kafka.xxx
         */
        ParameterTool params = ParameterTool.fromArgs(args);
        /*
         * 动态获取tables主键
         * */
        HashMap<String, String> tablesPriMap = MysqlUtil.getTablesPri(params);

        /**
         * 数据库连接信息
         */
        String hostname = params.getRequired("hostname");
        String username = params.getRequired("username");
        String password = params.getRequired("password");
        String databases = params.getRequired("databases");
        String tables = params.getRequired("tables");
        String serverIdRange = params.getRequired("serverid");
        String kafkaTopic = params.getRequired("kafka.topic");
        String serverTimeZone = params.get("serverTimeZone", "Asia/Shanghai");
        Boolean isDebug = params.getBoolean("is_debug", false);
        long checkpointInterval = params.getLong("checkpoint_interval", 60000L);

        int port = params.getInt("port");
        boolean includeSchema = params.getBoolean("includeSchema", false);
        StartupOptions startupOptions = params
                .get("startupOptions", "latest")
                .equals("initial") ? StartupOptions.initial() : StartupOptions.latest();

        //判断是否为本地模式
        boolean localModule = params.getBoolean("local-run", false);

        Map<String, String> paramMap = params.toMap();
        Properties debeziumProperties = new Properties();
        Properties kafkaProperties = new Properties();
        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
        for (String key : paramMap.keySet()) {
            if (key.startsWith("debezium.")) {
                String tmpKey = key.replaceFirst("debezium.", "");
                debeziumProperties.setProperty(tmpKey, paramMap.get(key));
            }
            if (key.startsWith("kafka.")) {
                String tmpKey = key.replaceFirst("kafka.", "");
                kafkaProperties.setProperty(tmpKey, paramMap.get(key));
            }
        }


        /**
         * 根据不同环境创建不同tableEnv
         */
        StreamExecutionEnvironment env;
        TableEnvironment tableEnv;
        Configuration configuration = new Configuration();
        configuration.setInteger("state.checkpoints.num-retained", 3);
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);//ck 间隔（ms ）
        env.getCheckpointConfig().setCheckpointTimeout(params.getLong("checkpoint_timeout", 10 * 60000L));
//            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5 * 60000L);
//            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

//            env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(params.getInt("max_checkpoint", 1));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);  //尾和头

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));

        //创建EnvironmentSettings，默认为BlinkPlanner,isStreamingMode
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().build();
        // 基于setting创建tableEnv
        tableEnv = TableEnvironment.create(bsSettings);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .databaseList(databases) // monitor all tables under inventory database
                .username(username)
                .password(password)
                .debeziumProperties(debeziumProperties)
                .tableList(tables)
                .serverId(serverIdRange)
                .startupOptions(startupOptions)
                .serverTimeZone(serverTimeZone)
                .includeSchemaChanges(includeSchema)
                .deserializer((DebeziumDeserializationSchema) new StructDebeziumDeserializationSchema("UTC"))
                .build();
        DataStreamSource<String> mySQLSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "CDCSource");
        if (isDebug) {
            mySQLSource.setParallelism(1).print();
        }else {
            int sinkParallelism = params.getInt("sink_parallelism", env.getParallelism());
            mySQLSource.setParallelism(sinkParallelism)
                    .addSink(new FlinkKafkaProducer<String>(
                            kafkaTopic,
                            new SimpleStringSchema(),
                            kafkaProperties,
                            Optional.of(new TableIdPartitioner(tablesPriMap)))).
                    name("KafkaSink")
                    .uid(kafkaTopic);
        }
//



//        RowType physicalDataType =
//                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
//        MetadataConverter[] metadataConverters = getMetadataConverters();
//        final TypeInformation<RowData> typeInfo =
//                scanContext.createTypeInformation(producedDataType);
//
//        DebeziumDeserializationSchema<RowData> deserializer =
//                RowDataDebeziumDeserializeSchema.newBuilder()
//                        .setPhysicalRowType(physicalDataType)
//                        .setMetadataConverters(metadataConverters)
//                        .setResultTypeInfo(typeInfo)
////                        .setServerTimeZone(serverTimeZone)
//                        .setUserDefinedConverterFactory(
//                                MySqlDeserializationConverterFactory.instance())
//                        .build();


        /**
         * 通过侧输出 分流
         */
//        SingleOutputStreamOperator<String> sideOutStream = mySQLSource.process(new ProcessFunction<String, String>() {
//            @Override
//            public void processElement(String inputStr, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
//                /**
//                 * 解析Json 获取dbName与tableName
//                 */
//                JSONObject jsonObject = JSONObject.parseObject(inputStr);
//                JSONObject sourceObj = jsonObject.getJSONObject("source");
//                String dbName = sourceObj.getString("db");
//                String table = sourceObj.getString("table");
//
//                //根据库名称+表名称获取tag，并按照tag下下游分发
//                context.output(tableTagMap.get(String.format("%s.%s", dbName, table)), inputStr);
//            }
//        });
//
//        for (String fullTbName: tableList) {
//            String tbName = fullTbName.split("\\.")[1];
//            DataStream<String> tableStream = sideOutStream.getSideOutput(tableTagMap.get(fullTbName));
////            tableStream.process(new ProcessFunction<String, RowData>() {
////
////                @Override
////                public void processElement(String inputStr, ProcessFunction<String, RowData>.Context context, Collector<RowData> collector) throws Exception {
////                    byte[] bytes = inputStr.getBytes(StandardCharsets.UTF_8);
////                    DebeziumJsonDeserializationSchema debeziumJsonDeserializationSchema = new DebeziumJsonDeserializationSchema();
////                }0
////            });
//            Table tmpTable = streamTableEnv.fromDataStream(tableStream);
//            streamTableEnv.createTemporaryView(tbName, tmpTable);
//            Table resultTable = streamTableEnv.sqlQuery(String.format("SELECT * FROM %s",tbName));
//
//            // interpret the insert-only Table as a DataStream again
//            DataStream<Row> resultStream = streamTableEnv.toDataStream(resultTable);
//
//            // add a printing sink and execute in DataStream API
//            resultStream.print();
//        }

        env.execute(MySQL2KafkaTask.class.getSimpleName());
    }
}
