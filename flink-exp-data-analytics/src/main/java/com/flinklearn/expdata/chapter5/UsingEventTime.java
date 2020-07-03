package com.flinklearn.expdata.chapter5;

import com.flinklearn.expdata.common.Utils;
import com.flinklearn.expdata.datasouce.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Properties;

public class UsingEventTime {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Stream Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Event Time Analytics program....");

            //Create a Stream Environment in Flink
            StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //Setup to use Event Time
            streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
            DataStream<Tuple3<String, Integer, Long>> auditTrailTuple
                    = auditTrailStr
                    .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                        @Override
                        public Tuple3<String, Integer, Long> map(String auditStr) {

                            System.out.println("--- Received Record : " + auditStr);

                            String[] attributes = auditStr
                                    .replace("\"", "")
                                    .split(",");

                            return new Tuple3<String, Integer, Long>
                                    (attributes[1], //User
                                            Integer.valueOf(attributes[5]), //Duration
                                            Long.valueOf(attributes[4]) //Timestamp
                                    );
                        }
                    });

            /****************************************************************************
             *                  Setup Event Time and Watermarks
             ****************************************************************************/
            //Create a water marked Data Stream
            DataStream<Tuple3<String, Integer, Long>> auditTrailWithET
                    = auditTrailTuple.assignTimestampsAndWatermarks(
                    (new AssignerWithPunctuatedWatermarks<Tuple3<String, Integer, Long>>() {

                        //Extract Event timestamp value.
                        @Override
                        public long extractTimestamp(
                                Tuple3<String, Integer, Long> auditTrail,
                                long previousTimeStamp) {

                            return auditTrail.f2;
                        }

                        //Extract Watermark
                        transient long currWaterMark = 0L;
                        int delay = 5000;
                        int buffer = 2000;

                        @Nullable
                        @Override
                        public Watermark checkAndGetNextWatermark(
                                Tuple3<String, Integer, Long> auditTrail,
                                long newTimestamp) {

                            long currentTime = System.currentTimeMillis();

                            if (currWaterMark == 0L) {
                                currWaterMark = currentTime;
                            }
                            //update watermark every 10 seconds
                            else if (currentTime - currWaterMark > delay) {
                                currWaterMark = currentTime;
                            }
                            //return watermark adjusted to bufer
                            return new Watermark(
                                    currWaterMark - buffer);

                        }

                    }));

            /****************************************************************************
             *                  Convert DataStream to a Flink Table
             ****************************************************************************/

            //Register the DataStream as Table with Schema
            streamTableEnv
                    .registerDataStream(
                            "audit_trail",     //Name of the Table
                            auditTrailWithET,      //Source DataSet
                            //Column Names (otherwise f0,f1..)
                            "User, Duration, "
                                    + " Timestamp.rowtime"); //Set Event Time

            //Get and Print the registered Table.
            Table auditTrail = streamTableEnv.scan("audit_trail");

            System.out.println("\n Converted to Table with Schema :");
            auditTrail.printSchema();

            Utils.printStreamTable(streamTableEnv, auditTrail,
                    "Audit Trail Table : ");

            //Create a User Summary Table with 5 second summaries
            Table userSummary
                    = auditTrail
                    .select("User, Duration, Timestamp")
                    .window(Tumble.over("5.seconds") //5 Sec window
                            .on("Timestamp") //Event Time based
                            .as("FiveSecWindow"))   //Alias for window field

                    .groupBy("FiveSecWindow, User") //Group By
                    .select("FiveSecWindow.start, FiveSecWindow.end, "
                            + " User, User.count");

            //Print DataStream output
            streamTableEnv
                    .toRetractStream(userSummary, Row.class)
                    .map(new MapFunction<Tuple2<Boolean, Row>, Object>() {
                        @Override
                        public Object map(Tuple2<Boolean, Row> userSummary)
                                throws Exception {

                            String currentTime = (new Date()).toString();
                            System.out.println(" \n** Current Time " + currentTime
                                    + " : User Summary :" + userSummary.f1);
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
            streamEnv.execute("Flink Event Time Analytics Example");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}