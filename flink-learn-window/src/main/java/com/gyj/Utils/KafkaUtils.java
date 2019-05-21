package com.gyj.Utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author gyj
 * @title: KafkaUtils
 * @description: TODO
 * @date 2019/5/17 15:38
 */
public class KafkaUtils {

    public static final String bootstrapServers = "10.182.83.222:21005,10.182.83.223:21005,10.182.83.224:21005,10.182.83.225:21005,10.182.83.226:21005";
    public static final String zookeeperConnect = "10.182.83.227:24002,10.182.83.228:24002,10.182.83.229:24002";

    /**
     * 初始化kafka配置
     *
     * @param groupId
     * @return
     */
    public static Properties initKafkaProps(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers",bootstrapServers);
        props.put("zookeeper.connect",zookeeperConnect);
        //props.put("bootstrap.servers", "localhost:9092");
        //props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

    public static void KafkaProducer(String msg) {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.required.acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);
        producer.send(new ProducerRecord("gyj_test1", msg));
        producer.flush();
    }

    public static void main(String[] args){
        for (int i=0;i<20;i++){
            KafkaProducer("001," + System.currentTimeMillis() + ",test"+String.valueOf(i));
        }
    }


}
