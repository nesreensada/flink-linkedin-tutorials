package com.flinklearn.batch.chapter3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlatMapExtractCustomerTags implements FlatMapFunction
        <Tuple7<Integer,String, String, //Input Tuple
                String, Integer, Double, String>,
                Tuple2<String, String >> //Output Tuple

{
    @Override
    public void flatMap(
            Tuple7<Integer, String, String, String,
                    Integer, Double, String> order,
            Collector<Tuple2<String, String>> collector)  {

        //Extract relevant columns from the master tuple
        String customer = order.f1;
        String tags = order.f6;

        //Split the tags string. For each tag, collect the customer
        //and the tag as separate records
        for (String tag : tags.split(":")) {
            collector.collect(new Tuple2(customer, tag));
        }

    }
}
