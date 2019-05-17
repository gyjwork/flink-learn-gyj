package com.gyj;

import com.gyj.Utils.KafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;


/**
 * @author gyj
 * @title: Main
 * @description: TODO
 * @date 2019/5/1615:51
 */
public class Main {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("gyj_test1", new SimpleStringSchema(), KafkaUtils.initKafkaProps("gyj_flink_test"));
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
