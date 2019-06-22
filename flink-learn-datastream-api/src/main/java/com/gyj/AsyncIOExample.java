package com.gyj;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.Future;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author gyj
 * @title: AsyncIOExample
 * @description: TODO
 * @date 2019/5/28 14:01
 */
public class AsyncIOExample {

    static class AsyncDatabaseRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

        //https://blog.csdn.net/rlnlo2pnefx9c/article/details/83829461
        private transient SQLClient mySQLClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            JsonObject mySQLClientConfig = new JsonObject();
            mySQLClientConfig.put("url", "jdbc:mysql://localhost:3306/test")
                    .put("driver_class", "com.mysql.jdbc.Driver")
                    .put("max_pool_size", 20)//连接池中最大连接数量，默认是15
                    .put("user", "root")
                    .put("password", "password");
            /*
            https://mp.weixin.qq.com/s?__biz=MzA4MjUwNzQ0NQ==&mid=402228882&idx=1&sn=d9eebb0bbc306de4516a018dbc67af5e#rd
             */
            VertxOptions vo = new VertxOptions();
            //设置Vert.x实例中使用的Event Loop线程的数量
            //默认值为：2 * Runtime.getRuntime().availableProcessors()（可用的处理器个数）
            vo.setEventLoopPoolSize(10);
            //设置Vert.x实例中支持的Worker线程的最大数量，默认值为20
            vo.setWorkerPoolSize(20);
            Vertx vertx = Vertx.vertx(vo);

            mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);

        }

        @Override
        public void asyncInvoke(String key, ResultFuture<Tuple2<String, String>> resultFuture) {
            mySQLClient.getConnection(conn -> {
                if (conn.failed()) {
                    return;
                }

                final SQLConnection connection = conn.result();
                connection.query("SELECT * FROM user", res2 -> {
                    ResultSet rs = new ResultSet();
                    if (res2.succeeded()) {
                        rs = res2.result();
                    }
                    List<JsonObject> rows = rs.getRows();
                    for (JsonObject json : rows) {
                        resultFuture.complete(Collections.singleton(new Tuple2<>(key, json.toString())));
                    }
                });
            });
        }

        @Override
        public void close() throws Exception {
            //关闭链接
            mySQLClient.close();
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.socketTextStream("localhost", 9000);

        DataStream<Tuple2<String, String>> resultStream = AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 10000, TimeUnit.MILLISECONDS, 100);

        stream.print();
        env.execute("async io api test...");
    }
}
