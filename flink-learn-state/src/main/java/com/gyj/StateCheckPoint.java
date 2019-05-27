package com.gyj;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author gyj
 * @title: StateCheckPoint
 * @description: 代码分析：
 *               代码每隔2s发送10条记录，所有数据key=1,会发送到统一窗口进行计数，发送超过100条是，抛出异常，模拟异常
 *               窗口中统计收到的消息记录数，当异常发生时，查看windows是否会从state中获取历史数据，即checkpoint是否生效
 *               注释已经添加进代码中，有个问题有时，state.value()在open（）方法中调用的时候，会抛出null异常，而在apply中使用就不会抛出异常。                  使用就不会抛出异常。
 * Console日志输出分析：
 *               四个open调用，因为我本地代码默认并行度为4，所以会有4个windows函数被实例化出来，调用各自open函数
 *               source发送记录到达100抛出异常
 *               source抛出异常之后，count发送统计数丢失，重新从0开始
 *               windows函数，重启后调用open函数，获取state数据，处理记录数从checkpoint中获取恢复，所以从100开始
 * @date 2019/5/27 15:14
 */
public class StateCheckPoint {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //打开并设置checkpoint
        // 1.设置checkpoint目录，这里我用的是本地路径，记得本地路径要file开头
        // 2.设置checkpoint类型，at lease onece or EXACTLY_ONCE
        // 3.设置间隔时间，同时打开checkpoint功能
        //
        env.setStateBackend(new FsStateBackend("file:\\idea_project\\flink-learn-gyj\\flink-learn-state\\state_checkpoint"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(1000);


        //添加source 每个2s 发送10条数据，key=1，达到100条时候抛出异常
        env.addSource(new SourceFunction<Tuple3<Integer, String, Integer>>() {
            private Boolean isRunning = true;
            private int count = 0;

            @Override
            public void run(SourceContext<Tuple3<Integer, String, Integer>> sourceContext) throws Exception {
                while (isRunning) {

                    for (int i = 0; i < 10; i++) {
                        sourceContext.collect(Tuple3.of(1, "gyj", count));
                        count++;
                    }
                    if (count > 100) {
                        System.out.println("err_________________>100");
                        throw new Exception("err_________________>100");
                    }
                    System.out.println("source:" + count);
                    Thread.sleep(2000);
                }

            }

            @Override
            public void cancel() {
                System.out.println("concel_______________________");
            }
        }).keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                //窗口函数，比如是richwindowsfunction 否侧无法使用manage state
                .apply(new RichWindowFunction<Tuple3<Integer, String, Integer>, Integer, Tuple, TimeWindow>() {
                    private transient ValueState<Integer> state;
                    private int count = 0;

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Integer, String, Integer>> iterable, Collector<Integer> collector) throws Exception {
                        //从state中获取值
                        count = state.value();
                        for (Tuple3<Integer, String, Integer> item : iterable) {
                            count++;
                        }
                        //更新state值
                        state.update(count);
                        System.out.println("windows:" + tuple.toString() + "  " + count + "   state count:" + state.value());
                        collector.collect(count);
                    }

                    //获取state
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("##open");
                        ValueStateDescriptor<Integer> descriptor =new ValueStateDescriptor<Integer>(
                                        "average", // the state name
                                        TypeInformation.of(new TypeHint<Integer>() {
                                        }), // type information
                                        0);
                        state = getRuntimeContext().getState(descriptor);
                    }
                }).print();
        env.execute();
    }
}
