package com.flinklearn.batch.chapter3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FilterOrdersByDate implements FilterFunction
        <Tuple8<Integer,String, String, //Output Tuple
                String, Integer, Double, String, Double>>
{
    @Override
    public boolean filter(
            Tuple8<Integer, String, String, String,
                    Integer, Double, String, Double> order)
    {
        try {
            String dateStr = order.f3;

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

            //Convert String to Date
            Date orderDate =sdf.parse(dateStr);

            //Allowed Date Range
            Date firstDate = sdf.parse("2019/11/01");
            Date lastDate = sdf.parse("2019/11/11");

            //Check if order data is within range
            if ( orderDate.compareTo(firstDate) >= 0 &&
                    orderDate.compareTo(lastDate) < 0) {
                //return true if within range.
                return true;
            }
            else {
                //return false if outside range.
                return false;
            }

        } catch (ParseException e) {
            return false;
        }

    }
}
