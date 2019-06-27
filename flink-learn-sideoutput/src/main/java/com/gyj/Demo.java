package com.gyj;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * 除了DataStream 算子操作产生的主流之外，您还可以生成任意数量的附加旁路输出结果流。
 * 结果流中的数据类型不必与主流中的数据类型匹配，并且不同旁路输出的类型也可以不同。
 * 当您希望拆分通常必须复制流的数据流，然后从每个流中过滤掉您不希望拥有的数据时，此算子操作非常有用。
 * 使用旁路输出时，首先需要定义一个OutputTag用于标识旁路输出流的方法：
 */

/**
 * @author gyj
 * @title: Demo
 * @description: flink-sideoutput
 * @date 2019/6/24 15:28
 */
public class Demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // 获取输入数据
        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        final OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };

        SingleOutputStreamOperator<Tuple2<String, Integer>> mainDataStream = text
                .process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // normalize and split the line
                String[] tokens = value.toLowerCase().split("\\W+");
                for (String token : tokens) {
                    if (token.length() > 5) {
                        context.output(outputTag, "sideout-" + token);
                    } else {
                        collector.collect(Tuple2.of(token, 1));
                    }
                }
            }
        });

        DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag);

        DataStream<Tuple2<String, Integer>> counts = mainDataStream
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1);

        // wordcount结果输出
        counts.print();
        // 侧输出结果输出
        sideOutputStream.print();

        env.execute("flink-sideoutput-test");

    }
}
