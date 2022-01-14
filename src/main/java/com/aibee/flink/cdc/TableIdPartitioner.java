package com.aibee.flink.cdc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * 根据库名+表名称+主键进行分区
 * 如果无主键，使用库名+表名分区
 */
public class TableIdPartitioner extends FlinkKafkaPartitioner {
    private static final Logger LOG = LoggerFactory.getLogger(TableIdPartitioner.class);
    private HashMap<String,String > tablePriMap = new HashMap<>();

    public TableIdPartitioner(HashMap<String, String> tablePriMaps) {
        tablePriMap = tablePriMaps;
    }

    @Override
    public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        /**
         * 获取消息体
         */
        JSONObject jsonObject = JSONObject.parseObject(new String(value));
        JSONObject payload  = jsonObject;
        //兼容包含Schema的情况
        if(jsonObject.containsKey("payload")){
            payload = jsonObject.getJSONObject("payload");
        }
        //获取消息体内容
        JSONObject source = payload.getJSONObject("source");
        String op = payload.getString("op");// c,u,r,d
        JSONObject before = payload.getJSONObject("before");
        JSONObject after = payload.getJSONObject("after");
        String id= "";
        /**
         * 根据不同操作类型，判断消息中是否包含ID，如果包含，则将ID列放到待Hash的key中
         * r,c,u都包含after的消息体，只有d类型需要从before中取
         */
        String db = source.getString("db");
        String table = source.getString("table");
        String dbTable=db.trim()+"."+table.trim();
        String tablePri = tablePriMap.get(dbTable);
        System.out.println("tablePri:"+tablePri);
        if("d".equals(op)){
            id = before.containsKey(tablePri)?before.getString(tablePri):"";
        }else {
            id = after.containsKey(tablePri)?after.getString(tablePri):"";
        }
    
        int part = Math.abs((table + db + id).hashCode() % partitions.length);
        //debug
        LOG.debug("db={},table={},id={},part={}",db,table,id,part);
        return part;
    }
}
