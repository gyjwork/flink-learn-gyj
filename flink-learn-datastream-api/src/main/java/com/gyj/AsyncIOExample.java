package com.gyj;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * @author gyj
 * @title: AsyncIOExample
 * @description: TODO
 * @date 2019/5/28 14:01
 */
public class AsyncIOExample {

    class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

        private transient DatabaseClient client;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void asyncInvoke(String s, ResultFuture<Tuple2<String, String>> resultFuture) {

        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }


    }
