package com.flinklearn.expdata.chapter2;

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

public class BasicBatchAnalytics {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Batch Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Basic Batch SQL Analytics program....");

            //Create a Flink Batch Execution Environment
            ExecutionEnvironment batchEnv
                    = ExecutionEnvironment.getExecutionEnvironment();

            //Create a Table environment within the Batch Environment
            BatchTableEnvironment batchTableEnv
                    = BatchTableEnvironment.create(batchEnv);

            /****************************************************************************
             *                 Reading a File Batch Data Source
             ****************************************************************************/

            //Create a Table Source with the Source CSV File & Schema

            TableSource salesSource
                    = new CsvTableSource.Builder()  //Type = CSV
                    .path("data/batch/sales_orders.csv")  //Path for the Source
                    .ignoreFirstLine()              //Ignore Header line
                    .field("ID", Types.INT)
                    .field("Customer", Types.STRING)
                    .field("Product", Types.STRING)
                    .field("Date", Types.STRING)
                    .field("Quantity", Types.INT)
                    .field("Rate", Types.DOUBLE)
                    .field("Tags", Types.STRING)
                    .build();   //Build the source definition

            //Register the Source as a Table
            batchTableEnv.registerTableSource("product_sales", salesSource);


            //Create a Table Object with the product_sales table.
            Table salesTable
                        = batchTableEnv
                            .scan("product_sales");

            System.out.println("\n Table Created with Schema : \n");

            salesTable.printSchema();

            /****************************************************************************
             *                 Selecting Data from Table
             ****************************************************************************/

            //Using Table API for Select
            Table selectAPI
                    = salesTable
                        .select("Customer, Product, Quantity, Rate");

            Utils.printBatchTable(batchTableEnv,selectAPI,"SELECT using Table API");

            //Using SQL for Select
            Table selectSQL
                    = batchTableEnv
                        .sqlQuery("SELECT Customer, Product, Quantity, Rate"
                                    + " FROM product_sales");

            Utils.printBatchTable(batchTableEnv,selectSQL,"SELECT using SQL");

            /****************************************************************************
             *                 Using Filters with Tables
             ****************************************************************************/

            //Using Table API for Select
            Table filterAPI
                    = salesTable
                    .select("Customer, Product, Quantity, Rate")
                    .filter("Product === 'Mouse' && Quantity > 3");

            Utils.printBatchTable(batchTableEnv,filterAPI,"FILTER using Table API");

            //Using SQL for Select
            Table filterSQL
                    = batchTableEnv
                    .sqlQuery("SELECT Customer, Product, Quantity, Rate"
                            + " FROM product_sales"
                            + " WHERE Product = 'Mouse' AND Quantity > 3");

            Utils.printBatchTable(batchTableEnv,filterSQL,"FILTER using SQL");

            /****************************************************************************
             *                 Write to a File Sink
             ****************************************************************************/

            //Define a CSV Table Sink
            TableSink<Row> summarySink

                    = new CsvTableSink(
                        "data/file_sink/MouseSales.csv",  //Destination File
                        ",",                //Field Delimiter
                        1,                  //Number of files to create
                        FileSystem.WriteMode.OVERWRITE  //Overwrite or Append
                     )
                    .configure(     //Configure Schema for CSV File
                            new String[]{
                                    "Customer",
                                    "Product",
                                    "Quantity",
                                    "Rate"},
                            new TypeInformation[]{
                                    Types.STRING,
                                    Types.STRING,
                                    Types.INT,
                                    Types.DOUBLE});

            //Register the Sink Table
            batchTableEnv.registerTableSink(
                                "customer_summary",  //Table Name
                                    summarySink);       //Sink Definition

            //Insert contents for filterAPI to the Sink Table
            filterAPI.insertInto("customer_summary");

            //Force the output to the sink table.
            batchEnv.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
