/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gyj;

import com.gyj.Entites.MyEvent;
import com.gyj.Utils.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

/**
 * 带时间戳和水印的源函数
 */
public class StreamingJobWindow1 {

    /*
    流源可以直接为它们生成的数据元分配时间戳，也可以发出水印。
    完成此 算子操作后，不需要时间戳分配器。请注意，如果使用时间戳分配器，则源将提供的任何时间戳和水印都将被覆盖。
    要直接为源中的数据元分配时间戳，源必须使用该collectWithTimestamp(...) 方法SourceContext。
    要生成水印，源必须调用该emitWatermark(Watermark)函数。
     */
    public static void main(String[] args) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //要处理事件时间，流式传输程序需要相应地设置时间特性。
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("gyj_test1", new SimpleStringSchema(), KafkaUtils.initKafkaProps("flink_windows"));

        DataStreamSource<MyEvent> dataStreamSource = env.addSource(new SourceFunction<MyEvent>() {

            @Override
            public void run(SourceContext<MyEvent> ctx) throws Exception {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(KafkaUtils.initKafkaProps("flink_windows"));
                consumer.subscribe(Collections.singleton("gyj_test1"));
                try {
                    for (; ; ) {
                        ConsumerRecords<String, String> records = consumer.poll(100);
                        // 处理kafka数据逻辑
                        records.forEach(record -> {
                            String[] split = record.value().split(",");
                            MyEvent myEvent = new MyEvent(split[0], System.currentTimeMillis(), split[2]);
                            //生成timestamp
                            System.out.println(sdf.format(new Date(myEvent.getEventTime())));
                            ctx.collectWithTimestamp(myEvent, myEvent.getEventTime());
                            // 生成watermarks ，实际中应该是隔一段时间生成水印，而不是生成时间戳就生成一个水印
                            ctx.emitWatermark(new Watermark(myEvent.getEventTime()));

                        });
                        consumer.commitAsync();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        consumer.commitSync();
                    } finally {
                        consumer.close();
                    }
                }
            }

            @Override
            public void cancel() {
                System.out.println("end...");
            }
        });

        SingleOutputStreamOperator<MyEvent> max = dataStreamSource.keyBy("id")
                .timeWindow(Time.seconds(5))
                .max("eventTime");

        max.print();
        env.execute("Event Time");

        /*
         一个翻滚窗口分配器的每个数据元分配给指定的窗口的窗口大小。
         翻滚窗具有固定的尺寸，不重叠。
         例如，如果指定大小为5分钟的翻滚窗口，则将评估当前窗口，并且每五分钟将启动一个新窗口.
         */
//        input.map(i -> Tuple2.of(i, 1))
//                .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .sum(1)
//                .print();

        //env.execute("Flink Streaming Java API window");
    }
}
