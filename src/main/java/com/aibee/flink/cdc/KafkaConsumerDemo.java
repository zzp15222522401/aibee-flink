package com.aibee.flink.cdc;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

public class KafkaConsumerDemo {

    public static void main(String args[]) {
        //设置JAAS配置文件的路径。
        JavaKafkaConfigurer.configureSasl();

        //加载kafka.properties。
        Properties kafkaProperties =  JavaKafkaConfigurer.getKafkaProperties();

        Properties props = new Properties();
        //设置接入点，请通过控制台获取对应Topic的接入点。
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        //设置SSL根证书的路径，请记得将XXX修改为自己的路径。
        //与SASL路径类似，该文件也不能被打包到jar中。
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaProperties.getProperty("ssl.truststore.location"));
        //根证书存储的密码，保持不变。
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
        //接入协议，目前支持使用SASL_SSL协议接入。
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        //SASL鉴权方式，保持不变。
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        //两次Poll之间的最大允许间隔。
        //消费者超过该值没有返回心跳，服务端判断消费者处于非存活状态，服务端将消费者从Consumer Group移除并触发Rebalance，默认30s。
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        //设置单次拉取的量，走公网访问时，该参数会有较大影响。
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 32000);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 32000);
        //每次Poll的最大数量。
        //注意该值不要改得太大，如果Poll太多数据，而不能在下次Poll之前消费完，则会触发一次负载均衡，产生卡顿。
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        //消息的反序列化方式。
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //当前消费实例所属的消费组，请在控制台申请之后填写。
        //属于同一个组的消费实例，会负载消费消息。
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty("group.id"));
        //Hostname校验改成空。
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        //构造消息对象，也即生成一个消费实例。
        KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);
        //设置消费组订阅的Topic，可以订阅多个。
        //如果GROUP_ID_CONFIG是一样，则订阅的Topic也建议设置成一样。
        List<String> subscribedTopics =  new ArrayList<String>();
        //如果需要订阅多个Topic，则在这里加进去即可。
        //每个Topic需要先在控制台进行创建。
        subscribedTopics.add(kafkaProperties.getProperty("topic"));
        consumer.subscribe(subscribedTopics);
        // 获取topic的partition信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(kafkaProperties.getProperty("topic"));
        List<TopicPartition> topicPartitions = new ArrayList<>();

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        long nowTime = now.getTime();
        System.out.println("当前时间: " + df.format(now));
        long fetchDataTime = nowTime - 1000 * 60 * 60;  // 计算30分钟之前的时间戳

        for(PartitionInfo partitionInfo : partitionInfos) {
            topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), fetchDataTime);
        }
        // 获取每个partition一个小时之前的偏移量
        Map<TopicPartition, OffsetAndTimestamp> map = consumer.offsetsForTimes(timestampsToSearch);

        OffsetAndTimestamp offsetTimestamp = null;
        System.out.println("开始设置各分区初始偏移量...");
        for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
            // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
            offsetTimestamp = entry.getValue();
            if(offsetTimestamp != null) {
                int partition = entry.getKey().partition();
                long timestamp = offsetTimestamp.timestamp();
                long offset = offsetTimestamp.offset();
                System.out.println("partition = " + partition +
                        ", time = " + df.format(new Date(timestamp))+
                        ", offset = " + offset);
                // 设置读取消息的偏移量
                consumer.seek(entry.getKey(), offset);
            }
        }
        System.out.println("设置各分区初始偏移量结束...");

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("partition = " + record.partition() + ", offset = " + record.offset() + "time" + df.format(record.timestamp()));
            }
        }
//        //循环消费消息。
//        while (true){
//            try {
//                ConsumerRecords<String, String> records = consumer.poll(1000);
//                //必须在下次Poll之前消费完这些数据, 且总耗时不得超过SESSION_TIMEOUT_MS_CONFIG。
//                //建议开一个单独的线程池来消费消息，然后异步返回结果。
//                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println(String.format("Consume partition:%d offset:%d value:%s", record.partition(), record.offset(),record.value()));
//                }
//            } catch (Exception e) {
//                try {
//                    Thread.sleep(1000);
//                } catch (Throwable ignore) {
//
//                }
//                e.printStackTrace();
//            }
//        }
    }
}