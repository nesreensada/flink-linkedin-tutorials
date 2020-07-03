package com.flinklearn.batch.chapter2;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;

import java.util.Arrays;
import java.util.List;

/*
A simple Apache Flink program that counts the number of elements in a collection
Running this program inside the IDE will create an embedded Apache Flink environment.
 */
public class FlinkFirstProgram {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Batch Sample program....");

            /*Get the execution enviornment.
            While running inside IDE, it will create an embedded environment
            While running inside a Flink installation, it will acquire the current context.
             */
            final ExecutionEnvironment env
                    = ExecutionEnvironment.getExecutionEnvironment();

            //Create a list of products
            List<String> products
                    = Arrays.asList("Mouse","Keyboard","Webcam");

            //Convert the list into a Flink DataSet
            DataSet<String> dsProducts
                    = env.fromCollection(products);

            /* Count the number of items in the DataSet
            Flink uses lazy execution, so all code is executed only when
            an output is requested.
             */
            System.out.println("Total products = " + dsProducts.count());

            /* write results to a text file */
            dsProducts.writeAsText("output/tempdata.csv");
            //Print execution plan
            System.out.println(env.getExecutionPlan());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
