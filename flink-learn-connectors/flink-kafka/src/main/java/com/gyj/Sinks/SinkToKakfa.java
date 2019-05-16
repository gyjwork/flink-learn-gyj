package com.gyj.Sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author gyj
 * @title: SinkToKakfa
 * @description: TODO
 * @date 2019/5/1616:23
 */
public class SinkToKakfa extends RichSinkFunction<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(String value, Context context) {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
