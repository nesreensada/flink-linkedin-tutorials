package com.flinklearn.batch.chapter4;


import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

public class OrderVendorJoinSelector
        implements JoinFunction

        <Tuple7<Integer,String, String, String,  //Input 1
                Integer, Double, String>,
                ProductVendorPojo,                          //Input 2
                Tuple2<String, Integer>>                 //Output
{

    @Override
    public Tuple2<String, Integer>   //Output : Vendor, Item Count
    join(
            Tuple7<Integer, String, String, String,  //Input 1
                    Integer, Double, String> order,
            ProductVendorPojo product)  //Input 2
    {
        //Return Vendor and Item Count
        return new Tuple2(product.getVendor(), order.f4);
    }
}
