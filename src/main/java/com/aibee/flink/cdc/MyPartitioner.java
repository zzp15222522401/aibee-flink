package com.aibee.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

public class MyPartitioner extends FlinkKafkaPartitioner {
    @Override
    public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        //这里接收到的key是上面MySchema()中序列化后的key，需要转成string，然后取key的hash值取模kafka分区数量
        JSONObject jsonObject = JSONObject.parseObject(new String(value));
        JSONObject source;
        if(jsonObject.containsKey("payload")){
            JSONObject payload = jsonObject.getJSONObject("payload");
             source = payload.getJSONObject("source");
        }else {
             source = jsonObject.getJSONObject("source");
        }
        String db = source.getString("db");
        String table = source.getString("table");
        int part = Math.abs((table + db ).hashCode() % partitions.length);
//        System.out.println("part:"+part+"+size:"+partitions.length);
        return part;
    }
}
