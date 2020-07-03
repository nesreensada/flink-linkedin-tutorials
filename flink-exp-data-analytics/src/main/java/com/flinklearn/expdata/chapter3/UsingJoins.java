package com.flinklearn.expdata.chapter3;

import com.flinklearn.expdata.common.Utils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class UsingJoins {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Batch Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Join program....");

            //Create a Flink Batch Execution Environment
            ExecutionEnvironment batchEnv
                    = ExecutionEnvironment.getExecutionEnvironment();

            //Create a Table environment within the Batch Environment
            BatchTableEnvironment batchTableEnv
                    = BatchTableEnvironment.create(batchEnv);

            /****************************************************************************
             *                 Reading the Tables from the Sources
             ****************************************************************************/

            //Read the Sales Orders Source

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

            System.out.println("\n Sales Table Created with Schema : \n");

            //Create a Table Object with the product_sales table.
            Table salesTable
                    = batchTableEnv
                    .scan("product_sales");

            salesTable.printSchema();

            //Read the Product Vendor Source

            TableSource vendorSource
                    = new CsvTableSource.Builder()
                    .path("data/batch/product_vendor.csv")
                    .ignoreFirstLine()
                    .field("Product", Types.STRING)
                    .field("Vendor", Types.STRING)
                    .build();


            batchTableEnv.registerTableSource("product_vendor", vendorSource);

            //Scan the table. There cannot be duplicate column names in
            //Joined tables, so alias them.
            Table vendorTable
                    = batchTableEnv
                        .scan("product_vendor")
                        .select("Product as Product2, Vendor");

            /****************************************************************************
             *                 Join the Tables
             ****************************************************************************/

            //Join using Table API
            Table joinedTableAPI
                    = salesTable
                        .join(vendorTable)      //Join the tables
                        .where("Product = Product2")    //Join Condition
                        .select("Product,Vendor, Quantity") //Post-join processing
                        .groupBy("Product,Vendor")
                        .select("Product, Vendor, Quantity.sum");

            Utils.printBatchTable(batchTableEnv,joinedTableAPI,
                    "JOIN using Table API");

            //Join Using SQL
            Table joinedTableSQL
                    = batchTableEnv.sqlQuery(
                        "SELECT p1.Product, v1.Vendor, "
                            + " sum(Quantity) as TotalQuantity"
                            + " FROM product_sales p1, product_vendor v1 "
                            + " WHERE p1.Product = v1.Product "
                            + " GROUP BY p1.Product, v1.Vendor"
            );

            Utils.printBatchTable(batchTableEnv,joinedTableSQL,
                    "JOIN using SQL");


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
