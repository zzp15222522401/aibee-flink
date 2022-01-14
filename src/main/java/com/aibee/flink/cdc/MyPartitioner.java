package com.aibee.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class MyPartitioner extends FlinkKafkaPartitioner {
    private static final Logger LOG = LoggerFactory.getLogger(MyPartitioner.class);
    private HashMap<String, Integer> kafkaSinkNumberPartitionMap = new HashMap<>();

    public MyPartitioner(HashMap<String, Integer> cdckafkaSinkNumberPartitionMap) {
        kafkaSinkNumberPartitionMap = cdckafkaSinkNumberPartitionMap;
    }

    @Override
    public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        //这里接收到的key是上面MySchema()中序列化后的key，需要转成string，然后取key的hash值取模kafka分区数量
        JSONObject jsonObject = JSONObject.parseObject(new String(value));
        JSONObject source;
        if (jsonObject.containsKey("payload")) {
            JSONObject payload = jsonObject.getJSONObject("payload");
            source = payload.getJSONObject("source");
        } else {
            source = jsonObject.getJSONObject("source");
        }
        String db = source.getString("db");
        String table = source.getString("table");
//        int partition = Math.abs((table + db).hashCode() % partitions.length);
//        LOG.info(new String(key));
//        int partition = 0;
        int par = kafkaSinkNumberPartitionMap.get(db.trim() + "." + table.trim());

//        System.out.println("SinkTable:" + db + "." + table + "  SinkKafkaPartition:" + par);
        return par;
    }
}
