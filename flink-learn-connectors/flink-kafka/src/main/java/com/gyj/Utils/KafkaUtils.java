package com.gyj.Utils;

import java.util.Properties;

/**
 * @author gyj
 * @title: KafkaUtils
 * @description: TODO
 * @date 2019/5/17 15:38
 */
public class KafkaUtils {

    public static final String bootstrapServers = "";
    public static final String zookeeperConnect = "";

    /**
     * 初始化kafka配置
     * @param groupId
     * @return
     */
    public static Properties initKafkaProps(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers",bootstrapServers);
        props.put("zookeeper.connect",zookeeperConnect);
        props.put("group.id",groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }


}
