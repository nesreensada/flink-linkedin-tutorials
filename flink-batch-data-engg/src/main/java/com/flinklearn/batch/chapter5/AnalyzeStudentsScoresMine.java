package com.flinklearn.batch.chapter5;

import com.flinklearn.batch.chapter3.MapComputeTotalOrderValue;
import com.flinklearn.batch.chapter3.ReduceProductwiseSummary;
import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;

public class AnalyzeStudentsScoresMine {
    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the analysis of students score ....");
            final ExecutionEnvironment env
                    = ExecutionEnvironment.getExecutionEnvironment();


             /*Make sure that the tuple with the correct number of elements is chosen
            to match the number of columns in the CSV file.
             */
            DataSet<Tuple4<String, String, Double, Double>> rawScores

                    = env.readCsvFile("src/main/resources/student_scores.csv")
                    .ignoreFirstLine()
                    .parseQuotedStrings('\"')
                    .types(String.class, String.class,
                           Double.class,Double.class);

            Utils.printHeader("Raw orders read from file");
            rawScores.first(5).print();

            /****************************************************************************
             *                  Compute Total Scores  for each student
             ****************************************************************************/

            DataSet<Tuple4<String, String, Integer, Double>> computedScores

                    = rawScores.map(new MapFunction<
                        Tuple4<String, String, Double, Double>,
                        Tuple4<String, String,Integer, Double>>() {

                            @Override
                            public Tuple4<String, String, Integer, Double>
                            map(Tuple4<String, String, Double, Double> score)
                            {
                                return new Tuple4(score.f0, //Student
                                        score.f1, //Subject
                                        1, //Return a 1 for counting later
                                        score.f2 + score.f3); //Total
                            }
                        } );
            Utils.printHeader("Orders with Order Value computed");
            computedScores.first(5).print();


            /****************************************************************************
             *              Print total scores for all Students for Physics
             ****************************************************************************/
            Utils.printHeader("Student Scores for Physics");

            computedScores.filter( scores -> (scores.f1.equals("Physics") ? true : false))
                    .project(0,3)
                    .print();

            /****************************************************************************
             *          Compute Average Score by Students across Subjects
             ****************************************************************************/
            DataSet<Tuple2<String, Double>> avgScoresByStudent =
                    computedScores.<Tuple3<String,Integer,Double>>
                                project(0,2,3)
                    .groupBy(0) // groupby student
                            .reduce(new ReduceFunction<Tuple3<String, Integer, Double>>() {
                                @Override
                                public Tuple3<String, Integer, Double>
                                reduce(Tuple3<String, Integer, Double> rec1,
                                       Tuple3<String, Integer, Double> rec2)
                                {
                                    //Return sum of counts and sum of total scores by student
                                    return new Tuple3(rec1.f0, rec1.f1 + rec2.f1,
                                            rec1.f2 + rec2.f2);
                                }
                            })
                            .map( i -> Tuple2.of(i.f0, i.f2 / i.f1))
                             .returns(Types.TUPLE(Types.STRING , Types.DOUBLE ));
            Utils.printHeader("Student Average Scores across Subjects");
            avgScoresByStudent.print();

            DataSet<Tuple3<String, String, Double>> topStudentBySubject

                    =computedScores
                    .<Tuple3<String,String,Double>>
                            project(0,1,3) //Select subset of fields
                    .groupBy(1)  //Group by Subject
                    .reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
                @Override
                public Tuple3<String, String, Double>
                reduce(Tuple3<String, String, Double> rec1,
                       Tuple3<String, String, Double> rec2)  {

                    //Compare records for multiple students. Return
                    //record for student with highest score.
                    if ( rec1.f2 > rec2.f2) {
                        return rec1;
                    }
                    else {
                        return rec2;
                    }
                }
            });
            Utils.printHeader("Top Student by Subject");
            topStudentBySubject
                    .project(1,0,2) //Rearrange Tuple so Subject comes first
                    .print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
