package com.gyj;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gyj
 * @title: Checkpoint
 * @description: flink 检查点相关说明
 * @date 2019/5/27 16:09
 *
 *
 */
public class Checkpoint {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //每1000毫秒启动一个检查点
        env.enableCheckpointing(1000);
        // 高级选项：
        // 将模式设置为完全一次（这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //检查点之间的最短时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //检查点必须在一分钟内完成，否则将被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //同时只允许一个检查点正在进行中
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //启用取消作业后保留的外部化检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

}
