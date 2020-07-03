package com.flinklearn.batch.chapter3;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/*
A simple Apache Flink program that counts the number of elements in a collection
Running this program inside the IDE will create an embedded Apache Flink environment.
 */
public class BasicTransformations {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Basic Transformation program....");

            //Get execution environment
            final ExecutionEnvironment env
                    = ExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                  Read CSV file into a DataSet
             ****************************************************************************/

            /*Make sure that the tuple with the correct number of elements is chosen
            to match the number of columns in the CSV file.
             */
            DataSet<Tuple7<Integer,String, String, String,
                                        Integer, Double, String>> rawOrders

                    = env.readCsvFile("src/main/resources/sales_orders.csv")
                            .ignoreFirstLine()
                            .parseQuotedStrings('\"')
                            .types(Integer.class, String.class, String.class, String.class,
                                        Integer.class, Double.class,String.class);

            Utils.printHeader("Raw orders read from file");
            rawOrders.first(5).print();

            /****************************************************************************
             *                  Compute Total Order Value for each record
             ****************************************************************************/

            DataSet<Tuple8<Integer,String, String, String,
                                Integer, Double, String, Double>> computedOrders

                    = rawOrders.map( new MapComputeTotalOrderValue() );

            Utils.printHeader("Orders with Order Value computed");
            computedOrders.first(5).print();

            /****************************************************************************
             *                 Extracts Tags by Customer into a separate dataset
             ****************************************************************************/

            DataSet<Tuple2<String, String>> customerTags

                    = rawOrders.flatMap(new FlatMapExtractCustomerTags());

            Utils.printHeader("Customer and Tags extracted as separate dataset");
            customerTags.first(10).print();

            /****************************************************************************
             *                 Filter Orders for First 10 days of November
             ****************************************************************************/

            DataSet<Tuple8<Integer,String, String, String,
                    Integer, Double, String, Double>> filteredOrders

                        = computedOrders.filter( new FilterOrdersByDate() );


            Utils.printHeader("Orders filtered for first 10 days of November");
            filteredOrders.first(5).print();

            System.out.println("\nTotal orders in first 10 days = "
                        + filteredOrders.count());



            /****************************************************************************
             *                Aggregate across all orders
             ****************************************************************************/

            //Use Projections to filter subset of columns
            DataSet<Tuple2<Integer,Double>> orderColumns
                    = computedOrders.project(4,7);

            //Find Average Order Size and Total Order Value
            DataSet totalOrders =
                       orderColumns
                             .aggregate(SUM,0) //Total Order Item Count
                             .and(SUM,1); //Total Order Value

            //Extract the Summary row tuple, by converting the DataSet to a List and
            //fetching the first record
            Tuple2<Integer, Double> sumRow
                    = (Tuple2<Integer,Double>) totalOrders.collect().get(0);

            Utils.printHeader("Aggregated Order Data ");
            System.out.println(" Total Order Value = "
                                + sumRow.f1
                                +"  Average Order Items = "
                                + sumRow.f0 * 1.0 / computedOrders.count() );

            /****************************************************************************
             *               Group and Aggregate -  by Product Type
             ****************************************************************************/

            DataSet<Tuple3<String,Integer,Double>> productOrderSummary

                    = computedOrders
                            .map(i -> Tuple3.of(i.f2, 1, i.f7)) //Subset of columns
                            .returns(Types.TUPLE(Types.STRING ,
                                    Types.INT, Types.DOUBLE )) //Set return types

                            .groupBy(0)  //Group by Product
                            .reduce(new ReduceProductwiseSummary());  //Summarize by Product

            Utils.printHeader("Product wise Order Summary ");
            productOrderSummary.print();

            //Find average for each product using an inline map function
            System.out.println("\n Average Order Value by Product :");

            //Compute average Order value by product using anonymous function
            productOrderSummary
                    .map(new MapFunction<Tuple3<String,Integer,Double>,
                                        Tuple2<String, Double>>()
                        {
                            public Tuple2<String, Double>
                                map(Tuple3<String, Integer, Double> summary) {
                                    return new Tuple2(
                                            summary.f0, //Get product
                                            summary.f2 * 1.0 / summary.f1); //Get Average
                                }
                        })
                    .print();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

