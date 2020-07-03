package com.flinklearn.expdata.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public  class Utils {

    public static void printHeader(String msg) {

        System.out.println("\n**************************************************************");
        System.out.println(msg);
        System.out.println("---------------------------------------------------------------");
    }


    public static void printBatchTable(BatchTableEnvironment batchTableEnv,
                                        Table table, String message) throws Exception {

        System.out.println("\n*----------" + message + "------------");

        batchTableEnv
                .toDataSet(table, Row.class)
                .first(5)
                .print();

    }

    public static void printStreamTable(StreamTableEnvironment streamTableEnv,
                                        Table table, String message) throws Exception {

        streamTableEnv
                .toAppendStream(table, Row.class)
                .map(new MapFunction<Row, Object>() {
                    @Override
                    public Object map(Row row) {
                        System.out.println(message + " Stream : " + row);
                        return row;
                    }
                });

    }
}
