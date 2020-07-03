package com.flinklearn.expdata.chapter6;

import com.flinklearn.expdata.common.Utils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class CourseUseCase {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Batch Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Course Use Case program....");

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
                    .path("data/batch/student_scores.csv")  //Path for the Source
                    .ignoreFirstLine()              //Ignore Header line
                    .field("Student", Types.STRING)
                    .field("Subject", Types.STRING)
                    .field("ClassScore", Types.DOUBLE)
                    .field("TestScore", Types.DOUBLE)
                    .build();   //Build the source definition

            //Register the Source as a Table
            batchTableEnv.registerTableSource("student_scores", salesSource);

            System.out.println("\n Table Created with Schema : \n");

            //Create a Table Object with the product_sales table.
            Table studentTable
                    = batchTableEnv
                    .scan("student_scores");

            studentTable.printSchema();

            /****************************************************************************
             *                 Extract Subject and Total Score for Bob
             ****************************************************************************/

            Table bobScoresAPI
                    = studentTable
                        .filter("Student === 'Bob'")
                        .select("Subject, ClassScore, TestScore")
                        .addColumns("(ClassScore + TestScore) as TotalScore")
                        .select("Subject,TotalScore");

            Utils.printBatchTable(batchTableEnv, bobScoresAPI,
                    "Bob's Total Scores with Table API");

            Table bobScoresSQL
                    = batchTableEnv
                        .sqlQuery("SELECT Subject, (ClassScore + TestScore) as TotalScore "
                                + " FROM student_scores "
                                + " WHERE Student = 'Bob'");

            Utils.printBatchTable(batchTableEnv, bobScoresSQL,
                    "Bob's Total Scores with SQL");

            /****************************************************************************
             *                 Find Average, Max and Min Total Scores by Subject
             ****************************************************************************/

            Table subjectSummaryAPI
                    = studentTable
                        .select("Subject, ClassScore, TestScore")
                        .addColumns("(ClassScore + TestScore) as TotalScore")
                        .groupBy("Subject")
                        .select("Subject, TotalScore.avg as AvgTotalScore, "
                                + " TotalScore.min as MinTotalScore, "
                                + " TotalScore.max as MaxTotalScore ");

            Utils.printBatchTable(batchTableEnv, subjectSummaryAPI,
                    "Subjectwise Summary with Table API");

            Table subjectSummarySQL
                    = batchTableEnv
                        .sqlQuery("SELECT Subject, "
                                + " AVG(ClassScore + TestScore) as AvgTotalScore, "
                                + " MIN(ClassScore + TestScore) as MinTotalScore, "
                                + " MAX(ClassScore + TestScore) as MaxTotalScore "
                            + " FROM student_scores "
                            + " GROUP BY Subject");

            Utils.printBatchTable(batchTableEnv, subjectSummarySQL,
                    "Subjectwise Summary with SQL");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
