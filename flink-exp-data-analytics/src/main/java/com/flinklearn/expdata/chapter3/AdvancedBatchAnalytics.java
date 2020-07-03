package com.flinklearn.expdata.chapter3;

import com.flinklearn.expdata.common.Utils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class AdvancedBatchAnalytics {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Batch Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Advanced Batch SQL Analytics program....");

            //Create a Flink Batch Execution Environment
            ExecutionEnvironment batchEnv
                    = ExecutionEnvironment.getExecutionEnvironment();

            //Create a Table environment within the Batch Environment
            BatchTableEnvironment batchTableEnv
                    = BatchTableEnvironment.create(batchEnv);

            /*************************************************************************
             *                 Reading a File Batch Data Source
             *************************************************************************/

            //Create a Table Source with the Source CSV File & Schema

            TableSource salesSource
                    = new CsvTableSource.Builder()  //Type = CSV
                    .path("data/batch/sales_orders.csv")  //Path for the Source
                    .ignoreFirstLine()              //Ignore Header line
                    .field("ID", Types.INT)
                    .field("Customer", Types.STRING)
                    .field("Product", Types.STRING)
                    .field("OrderDate", Types.STRING)
                    .field("Quantity", Types.INT)
                    .field("Rate", Types.DOUBLE)
                    .field("Tags", Types.STRING)
                    .build();   //Build the source definition

            //Register the Source as a Table
            batchTableEnv.registerTableSource("product_sales", salesSource);

            System.out.println("\n Table Created with Schema : \n");

            //Create a Table Object with the product_sales table.
            Table salesTable
                    = batchTableEnv
                    .scan("product_sales");

            salesTable.printSchema();

            /*************************************************************************
             *                 Aggregating Data
             *************************************************************************/

            //Aggregate using Table API
            Table aggregateAPI
                    = salesTable.select("Customer, Product, Quantity, Rate")
                        .groupBy("Customer, Product")
                        .select("Customer, Product, "
                                    + " Quantity.sum as TotalQuantity, "
                                    + " Rate.avg as AvgRate, "
                                    + " Rate.min as MinRate, "
                                    + " Rate.max as MaxRate ");

            aggregateAPI.printSchema();

            Utils.printBatchTable(batchTableEnv,aggregateAPI,
                    "GROUP BY using Table API");

            //Aggregate using SQL
            Table aggregateSQL
                    = batchTableEnv.sqlQuery(
                    "SELECT Customer, Product, SUM(Quantity), "
                            + " AVG(Rate), MIN(Rate), MAX(Rate) "
                            + " FROM product_sales "
                            + " GROUP BY Customer, Product "
                                );

            Utils.printBatchTable(batchTableEnv,aggregateSQL,
                    "GROUP BY using SQL");


            /*************************************************************************
             *                   Order By, Fetch
             *************************************************************************/

            //Use the base table for sorting
            Table orderByAPI
                    = salesTable
                        .select("Customer, Product, Quantity")
                        .orderBy("Customer.asc, Product.asc, Quantity.desc")
                        .fetch(5);

            Utils.printBatchTable(batchTableEnv,orderByAPI,
                    "ORDER BY using Table API");

            //Use an aggregated table for sorting
            Table orderBySQL
                    = batchTableEnv.sqlQuery(
                    "SELECT Customer, Product, SUM(Quantity), "
                            + " AVG(Rate), MIN(Rate), MAX(Rate) "
                            + " FROM product_sales "
                            + " GROUP BY Customer, Product "
                            + " ORDER BY Customer asc, 3 desc"
                            + " LIMIT 5"
                                );

            Utils.printBatchTable(batchTableEnv,orderBySQL,
                    "ORDER BY using SQL ");

            /*************************************************************************
             *                   Updating Data
             *************************************************************************/

            Table enhancedSalesTableAPI
                    = salesTable
                        .addColumns("(Rate * Quantity) as TotalPrice");

            enhancedSalesTableAPI.printSchema();

            Utils.printBatchTable(batchTableEnv,enhancedSalesTableAPI,
                    "Table with new Added Column API");

            Table enhancedSalesTableSQL
                    = batchTableEnv
                        .sqlQuery(" SELECT ID,Customer,Product, "
                                + "OrderDate,Quantity,Rate,Tags,"
                                + "(Rate * Quantity) as TotalPrice "
                                + "FROM product_sales");

            Utils.printBatchTable(batchTableEnv,enhancedSalesTableAPI,
                    "Table with new Added Column SQL");

            //If you want to use SQL against this table.
            batchTableEnv
                    .registerTable(
                            "enhanced_sales",
                            enhancedSalesTableSQL);


            /*************************************************************************
             *                   Print Execution Plan & Execute
             *************************************************************************/

            //Print execution Plan
            System.out.println("\n Execution Plan :\n**********************\n "
                    + batchTableEnv.explain(salesTable));

            //Needed for proper ordering or records
            batchEnv.execute("Advanced Batch Analytics");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
