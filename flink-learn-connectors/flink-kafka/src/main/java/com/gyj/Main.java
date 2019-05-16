package com.gyj;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author gyj
 * @title: Main
 * @description: TODO
 * @date 2019/5/1615:51
 */
public class Main {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String bootstrapServers = "10.182.83.222:21005,10.182.83.223:21005,10.182.83.224:21005,10.182.83.225:21005,10.182.83.226:21005";
        final String zookeeperConnect = "10.182.83.227:24002,10.182.83.228:24002,10.182.83.229:24002";
        final String groupId = "gyj_flink_test";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("zookeeper.connect", zookeeperConnect);
        properties.setProperty("group.id", groupId);

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("gyj_test1", new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest();
        DataStream<String> stream = env.addSource(myConsumer);
        //逻辑处理
        stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) {
                return Tuple2.of(s,1);
            }
        }).keyBy(0).timeWindow(Time.seconds(5))
                .sum(1).print();
        //stream.print();
        //FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>("gyj_test2", new SimpleStringSchema(), properties);
        //stream.addSink(myProducer);
        env.execute("flink kafka consumer start...");

    }
}
