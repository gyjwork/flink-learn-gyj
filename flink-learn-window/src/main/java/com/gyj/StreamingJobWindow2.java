
package com.gyj;


import com.gyj.Entites.MyEvent;
import com.gyj.Utils.KafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 时间戳分配器/水印生成器
 */

/**
 * window的设定无关数据本身，而是系统定义好了的。
 *
 * 输入的数据中，根据自身的Event Time，将数据划分到不同的window中，如果window中有数据，则当watermark时间>=Event Time时，就符合了window触发的条件了，最终决定window触发，还是由数据本身的Event Time所属的window中的window_end_time决定。
 *
 * 下面的测试中，最后一条数据到达后，其水位线已经升至10:11:24秒，正好是最早的一条记录所在window的window_end_time，所以window就被触发了。
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

            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             * 定义生成watermark的逻辑
             * 默认100ms被调用一次
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp
            @Override
            public long extractTimestamp(MyEvent myEvent, long l) {
                long timestamp = myEvent.getEventTime();
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("---------------------------------------------------------------------");
                System.out.println("key:"+myEvent.getId()+",eventtime:["+timestamp+"|"+sdf.format(timestamp)+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+
                        sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]");
                System.out.println("---------------------------------------------------------------------");
                return timestamp;
            }
        });

        //分组，聚合
        DataStream<String> window = waterMarkStream
                .keyBy(myEvent -> myEvent.getId())
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<MyEvent, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<MyEvent> input, Collector<String> out) throws Exception {
                        System.out.println("---------------------------------------------------------------------");
                        System.out.println("-----------------------------【apply】----- --------------------------");
                        System.out.println("输出key，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间");
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<MyEvent> it = input.iterator();
                        while (it.hasNext()) {
                            MyEvent next = it.next();
                            arrarList.add(next.getEventTime());
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                        out.collect(result);
                        System.out.println("---------------------------------------------------------------------");
                    }
                });

        //测试-把结果打印到控制台即可
        window.print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("gyj-eventtime-watermark");

    }
}
