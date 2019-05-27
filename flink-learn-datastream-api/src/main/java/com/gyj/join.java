package com.gyj;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * @author gyj
 * @title: join
 * @description: TODO
 * @date 2019/5/27 16:41
 */
public class join {

    private static class NameKeySelector implements KeySelector<Integer, Integer> {
        @Override
        public Integer getKey(Integer integer) {
            return integer;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,String>> stream9000 = env.socketTextStream("localhost", 9000)
                .map(x -> x.split(",")).map(y -> Tuple2.of(y[0],y[1])).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataStream<Tuple2<String,String>> stream9001 = env.socketTextStream("localhost", 9001)
                .map(x -> x.split(",")).map(y -> Tuple2.of(y[0],y[1])).returns(Types.TUPLE(Types.STRING, Types.STRING));

        stream9000.join(stream9001)
                .where(x -> x.f0)
                .equalTo(y -> y.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .apply(new JoinFunction<Tuple2<String,String>, Tuple2<String,String>, String>() {
                    @Override
                    public String join(Tuple2 tuple2, Tuple2 tuple22) {
                        return "【一】："+tuple2.f0+"|"+tuple2.f1+"【二】："+tuple22.f0+"|"+tuple22.f1;
                    }
                }).print();

        env.execute("flink join test....");


    }
}
