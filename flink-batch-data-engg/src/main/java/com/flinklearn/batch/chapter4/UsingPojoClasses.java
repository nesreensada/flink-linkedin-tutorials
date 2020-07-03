package com.flinklearn.batch.chapter4;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;


public class UsingPojoClasses {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Pojo Classes program....");

            //Get execution environment
            final ExecutionEnvironment env
                    = ExecutionEnvironment.getExecutionEnvironment();

            /* Load the Product vendor information into a POJO Dataset.
            CSVs can be loaded as and processed as classes.
            */
            DataSet<ProductVendorPojo> productVendor
                    = env.readCsvFile("src/main/resources/product_vendor.csv")
                    .ignoreFirstLine()
                    .pojoType(ProductVendorPojo.class, "product", "vendor");

            //Print the contents
            System.out.println("Product Vendor Details loaded : ");
            productVendor.print();

            //Compute Vendorwise Product Counts and print them

            DataSet<Tuple2<String,Integer>> vendorSummary
                = productVendor.map(i -> Tuple2.of(i.vendor, 1)) // Vendor and 1 count per record
                        .returns(Types.TUPLE(Types.STRING ,Types.INT))
                        .groupBy(0) //Group by Vendor
                        .reduce( (sumRow,nextRow) ->   //Reduce operation
                                Tuple2.of(sumRow.f0, sumRow.f1+ nextRow.f1));


            //Convert a dataset to a list and pretty print
            System.out.printf("\n%15s  %10s\n\n", "Vendor", "Products");

            List<Tuple2<String,Integer>> vendorList = vendorSummary.collect();
            for( Tuple2<String, Integer> vendorRecord :vendorList) {
                System.out.printf("%15s  %10s\n", vendorRecord.f0, vendorRecord.f1);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



