package com.flinklearn.expdata.chapter3;

import com.flinklearn.expdata.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class WorkingWithDatasets {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Batch Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Working with DataSets program....");

            //Create a Flink Batch Execution Environment
            ExecutionEnvironment batchEnv
                    = ExecutionEnvironment.getExecutionEnvironment();

            //Create a Table environment within the Batch Environment
            BatchTableEnvironment batchTableEnv
                    = BatchTableEnvironment.create(batchEnv);

            /****************************************************************************
             *                      Convert Dataset to a Table
             ****************************************************************************/

            /* Load the Orders into a Tuple Dataset
             */
            DataSet<Tuple7<Integer,String, String, String,
                    Integer, Double, String>> rawOrders

                    = batchEnv.readCsvFile("data/batch/sales_orders.csv")
                        .ignoreFirstLine()
                        .parseQuotedStrings('\"')
                        .types(Integer.class, String.class,
                                String.class, String.class,
                                Integer.class, Double.class,String.class);

            System.out.println("Loaded into DataSet :") ;
            rawOrders.first(5).print();

            //Register the DataSet as Table with Schema
            batchTableEnv
                    .registerDataSet(
                            "sales_orders",     //Name of the Table
                            rawOrders,             //Source DataSet
                            //Column Names (otherwise f0,f1..)
                            "ID, Product, Vendor, SalesDate,"
                                    + "Quantity, Rate, Tags");

            //Get and Print the registered Table.
            Table salesOrders = batchTableEnv.scan("sales_orders");

            System.out.println("\n Converted to Table with Schema :");
            salesOrders.printSchema();

            Utils.printBatchTable(batchTableEnv, salesOrders,
                    "DataSet converted to Table");

            /****************************************************************************
             *                      Convert Table to a DataSet
             ****************************************************************************/

            //Summarize Table by Product
            Table productSummary
                    = salesOrders
                        .select("Product, Quantity, Rate")
                        .groupBy("Product")
                        .select("Product, "
                                + "sum(Quantity) as TotalQuantity, "
                                + "avg(Rate) as AverageRate" );

            //Convert to DataSet of Type Row.
            DataSet<Row> dsProductSummaryRow
                    = batchTableEnv.toDataSet(productSummary,Row.class);

            //Convert to desired data format - in this case, a Tuple
            DataSet<Tuple3<String, Integer, Double>> dsProductSummaryTuple
                    = dsProductSummaryRow
                        .map(new MapFunction<Row,
                                Tuple3<String, Integer, Double>>() {
                            @Override
                            public Tuple3<String, Integer, Double> map(Row row)
                                    throws Exception {

                                //Extract Row elements
                                String product = (String)row.getField(0);
                                Integer quantity = (Integer) row.getField(1);
                                Double rate = (Double)row.getField(2);

                                //Return tuple
                                return new Tuple3<String,Integer,Double>
                                        (product,quantity,rate);
                            }
                        });

            System.out.println("\n Table converted to DataSet of Tuple :" +
                    "\n--------------------------------------");

            dsProductSummaryTuple.print();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
