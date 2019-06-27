package com.gyj;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.*;

/**
 * @author gyj
 * @title: Demo
 * @description: flink-elasticsearch
 * @date 2019/6/10 15:51
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("xxx", 9000, "\n");

        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "es_test");
        config.put("bulk.flush.max.actions", "1");
        //1、用来表示是否开启重试机制
        //config.put("bulk.flush.backoff.enable", "true");
        //2、重试策略，又可以分为以下两种类型
        //a、指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8 ...
        //config.put("bulk.flush.backoff.type", "EXPONENTIAL");
        //b、常数型，表示多次重试之间的时间间隔为固定常数。eg:2 -> 2 -> 2 ...
        //config.put("bulk.flush.backoff.type", "CONSTANT");
        //3、进行重试的时间间隔。对于指数型则表示起始的基数
        //config.put("bulk.flush.backoff.delay", "2");
        //4、失败重试的次数
        //config.put("bulk.flush.backoff.retries", "3");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("xxx"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("xxx"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("xxx"), 9300));

        text.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {

            public IndexRequest createIndexRequest(String element) {
                Map<String, String> json = new HashMap<>();
                //将需要写入ES的字段依次添加到Map当中
                json.put("data", element);
                return Requests.indexRequest()
                        .index("flink-els-index")
                        .type("flink-els-type")
                        .source(json);
            }

            @Override
            public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(createIndexRequest(s));
            }
        }, new ActionRequestFailureHandler() {
            //Flink Elasticsearch Sink允许用户通过简单地实现ActionRequestFailureHandler并将其提供给构造函数来指定请求失败的处理方式
            @Override
            public void onFailure(ActionRequest action,
                                  Throwable failure,
                                  int restStatusCode,
                                  RequestIndexer indexer) throws Throwable {
                if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                    indexer.add(new ActionRequest[]{action});
                } else {
                    if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                        // 忽略写入超时，因为ElasticSearchSink 内部会重试请求，不需要抛出来去重启 flink job
                        return;
                    } else {
                        Optional<IOException> exp = ExceptionUtils.findThrowable(failure, IOException.class);
                        if (exp.isPresent()) {
                            IOException ioExp = exp.get();
                            if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                                // 经过多次不同的节点重试，还是写入失败的，则忽略这个错误，丢失数据。
                                System.out.println(ioExp.getMessage());
                                return;
                            }
                        }
                    }
                    throw failure;
                }
            }
        }));
        env.execute("flink-elasticsearch-test...");
    }
}
