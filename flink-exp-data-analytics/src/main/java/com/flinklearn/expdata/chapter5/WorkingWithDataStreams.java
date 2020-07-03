package com.flinklearn.expdata.chapter5;

import com.flinklearn.expdata.common.Utils;
import com.flinklearn.expdata.datasouce.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Properties;

public class WorkingWithDataStreams {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Stream Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Working with DataStreams program....");

            //Create a Stream Environment in Flink
            StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //Create a Stream Table Environment in the Stream Environment
            StreamTableEnvironment streamTableEnv
                    = StreamTableEnvironment.create(streamEnv);

            /****************************************************************************
             *                  Read Kafka Topic Stream into a DataStream.
             ****************************************************************************/

            //Set connection properties to Kafka Cluster
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "flink.learn.realtime");

            //Setup a Kafka Consumer on Flnk
            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>
                            ("flink.kafka.streaming.source", //topic
                                    new SimpleStringSchema(), //Schema for data
                                    properties); //connection properties

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest();

            //Create the data stream
            DataStream<String> auditTrailStr = streamEnv
                    .addSource(kafkaConsumer);

            //Convert each record to an Object
            DataStream<Tuple3<String,String,Integer>> auditTrailTuple
                    = auditTrailStr
                    .map(new MapFunction<String,Tuple3<String,String,Integer>>() {
                        @Override
                        public Tuple3<String,String,Integer> map(String auditStr) {

                            System.out.println("--- Received Record : " + auditStr);

                            String[] attributes = auditStr
                                    .replace("\"","")
                                    .split(",");

                            return new Tuple3<String,String,Integer>
                                    (   attributes[1], //User
                                        attributes[2],//Entity
                                        Integer.valueOf(attributes[5])//Duration
                                    );
                        }
                    });

            /****************************************************************************
             *                  Convert DataStream to a Flink Table
             ****************************************************************************/

            //Register the DataStream as Table with Schema
            streamTableEnv
                    .registerDataStream(
                            "audit_trail",     //Name of the Table
                            auditTrailTuple,             //Source Datastream
                            //Column Names (otherwise f0,f1..)
                            "User, Entity, Duration, "
                            + " User.proctime as ProcessingTime");

            //Get and Print the registered Table.
            Table auditTrail = streamTableEnv.scan("audit_trail");

            System.out.println("\n Converted to Table with Schema :");
            auditTrail.printSchema();

            Utils.printStreamTable(streamTableEnv,auditTrail,
                    "Audit Trail Table : ");


            /****************************************************************************
             *                  Convert Flink Table to a DataStream
             ****************************************************************************/

            //Create a User Summary Table with 5 second summaries
            Table userSummary
                    = auditTrail
                        .select("User, Duration, ProcessingTime")
                        .window(Tumble.over("5.seconds") //5 Sec window
                                .on("ProcessingTime") //Processing Time based
                                .as("FiveSecWindow"))   //Alias for window field

                        .groupBy("FiveSecWindow, User") //Group By
                        .select("FiveSecWindow.start, " //Window Start
                                + " User,User.count");

            //Convert User Summary to DataStream
            DataStream<Tuple3<Timestamp, String,Long>> dsUserSummary
                    = streamTableEnv
                        .toAppendStream(userSummary,Row.class) //Append to Stream

                        //Map function to Transform Row to Tuple
                        .map(new MapFunction<Row, Tuple3<Timestamp, String, Long>>() {
                            @Override
                            public Tuple3<Timestamp, String, Long> map(Row row) throws Exception {
                                return new Tuple3<Timestamp,String,Long>
                                        ((Timestamp) row.getField(0),
                                                (String)row.getField(1),
                                                (Long) row.getField(2));
                            }
                        });

            //Print DataStream output
            dsUserSummary.map(new MapFunction<Tuple3<Timestamp,String, Long>, Object>() {
                @Override
                public Object map(Tuple3<Timestamp,String, Long> userSummary)
                        throws Exception {
                    System.out.println("DataStream : User Summary :" + userSummary);
                    return null;
                }
            });

            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/

            //Start the Kafka Stream generator on a separate thread
            Utils.printHeader("Starting Kafka Data Generator...");
            Thread genThread = new Thread(new KafkaStreamDataGenerator());
            genThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Analytics Example");


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
