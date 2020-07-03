package com.flinklearn.batch.chapter3;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceProductwiseSummary implements ReduceFunction
    <Tuple3<String, Integer, Double>>
{
    @Override
    public Tuple3<String, Integer, Double> reduce  //Reduce Function
            (Tuple3<String, Integer, Double> rec1, //first or Summary record
             Tuple3<String, Integer, Double> rec2) //Next record
    {

        return new Tuple3(rec1.f0, //The Product name group
                            rec1.f1 + rec2.f1, //Total Orders
                            rec1.f2 + rec2.f2); //Total Order Value
    }
}
