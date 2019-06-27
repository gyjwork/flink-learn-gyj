package com.gyj;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author gyj
 * @title: Demo
 * @description: flink-table-api
 * @date 2019/6/24 16:54
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        // for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a TableEnvironment
        // for batch programs use BatchTableEnvironment instead of StreamTableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // register a Table
        tableEnv.registerTable("table1", null);                         // or
        tableEnv.registerTableSource("table2", null);              // or
        tableEnv.registerExternalCatalog("extCat", null);
        // register an output Table
        tableEnv.registerTableSink("outputTable", null);

        // create a Table from a Table API query
        Table tapiResult = tableEnv.scan("table1").select("...");
        // create a Table from a SQL query
        Table sqlResult = tableEnv.sqlQuery("SELECT ... FROM table2 ... ");

        // emit a Table API result Table to a TableSink, same for SQL result
        tapiResult.insertInto("outputTable");

        // execute
        env.execute();
    }

}
