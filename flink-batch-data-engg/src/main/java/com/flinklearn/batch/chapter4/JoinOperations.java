package com.flinklearn.batch.chapter4;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

public class JoinOperations {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Dataset Join Program....");

            //Get execution environment
            final ExecutionEnvironment env
                    = ExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                      Load the datasets
             ****************************************************************************/

            /* Load the Orders into a Tuple Dataset
             */
            DataSet<Tuple7<Integer,String, String, String,
                    Integer, Double, String>> rawOrders

                    = env.readCsvFile("src/main/resources/sales_orders.csv")
                    .ignoreFirstLine()
                    .parseQuotedStrings('\"')
                    .types(Integer.class, String.class, String.class, String.class,
                    Integer.class, Double.class,String.class);


            /* Load the Product vendor information into a POJO Dataset.
                CSVs can be loaded as and processed as classes.
            */
            DataSet<ProductVendorPojo> productVendor
                    = env.readCsvFile("src/main/resources/product_vendor.csv")
                        .ignoreFirstLine()
                        .pojoType(ProductVendorPojo.class, "product", "vendor");


            /****************************************************************************
             *                      Join the datasets
             ****************************************************************************/

            DataSet<Tuple2<String,Integer>> vendorOrders
                    = rawOrders.join(productVendor)     //Second DataSet
                            .where(2)           //Join Field from first Dataset
                            .equalTo("product") //Join Field from second Dataset
                            .with(new OrderVendorJoinSelector()); //Returned Data

            System.out.println("\n Summarized Item Count by Vendor : ");

            //Print total item count by vendor
            //Use a map function to do a pretty print.
            vendorOrders
                    .groupBy(0)
                    .sum(1)
                    .print();



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
