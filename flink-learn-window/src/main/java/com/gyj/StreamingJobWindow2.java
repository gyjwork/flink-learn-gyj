
package com.gyj;


import com.gyj.Entites.MyEvent;
import com.gyj.Utils.KafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import javax.annotation.Nullable;

/**
 * 时间戳分配器/水印生成器
 */
public class StreamingJobWindow2 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("gyj_test1", new SimpleStringSchema(), KafkaUtils.initKafkaProps("flink_windows"));

        DataStream<String> stream = env.addSource(myConsumer);

        //解析输入的数据
        DataStream<MyEvent> stream1 = stream.map(new MapFunction<String, MyEvent>() {
            @Override
            public MyEvent map(String s) throws Exception {
                String[] split = s.split(",");
                MyEvent myEvent = new MyEvent(split[0], Long.valueOf(split[1]), split[2]);
                return myEvent;
            }
        });

        //抽取timestamp和生成watermark
        //重新AssignerWithPeriodicWatermarks方法
        DataStream<MyEvent> waterMarkStream =stream1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<MyEvent>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return null;
            }

            @Override
            public long extractTimestamp(MyEvent myEvent, long l) {
                return 0;
            }
        });
    }
}
