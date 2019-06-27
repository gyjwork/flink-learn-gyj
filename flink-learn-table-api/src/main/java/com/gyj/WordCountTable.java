package com.gyj;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @author gyj
 * @title: WordCountTable
 * @description: flink官方example
 * @date 2019/6/25 14:17
 */
public class WordCountTable {

    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WC> input = env.fromElements(
                new WC("hello", 1),
                new WC("ok", 1),
                new WC("hello", 1));

        Table table = tEnv.fromDataSet(input);
        Table filtered = table
                .groupBy("word")
                .select("word,frequency.sum as frequency")
                .filter("frequency = 2");

//        String explanation = tEnv.explain(filtered);
//        System.out.println(explanation);

        DataSet<WC> result = tEnv.toDataSet(filtered,WC.class);
        result.print();

    }

    public static class WC {
        public String word;
        public long frequency;

        public String getWord() {
            return word;
        }

        public long getFrequency() {
            return frequency;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public void setFrequency(long frequency) {
            this.frequency = frequency;
        }

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        public WC() {
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
}
