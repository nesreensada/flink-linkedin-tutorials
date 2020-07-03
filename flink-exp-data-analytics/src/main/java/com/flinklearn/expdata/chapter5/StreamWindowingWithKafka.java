package com.flinklearn.expdata.chapter5;

import com.flinklearn.expdata.common.Utils;
import com.flinklearn.expdata.datasouce.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.util.Properties;

public class StreamWindowingWithKafka {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Stream Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Stream Window Analytics program....");

            //Create a Stream Environment in Flink
            StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //Create a Stream Table Environment in the Stream Environment
            StreamTableEnvironment streamTableEnv
                    = StreamTableEnvironment.create(streamEnv);

            /****************************************************************************
             *                 Reading a Kafka Streaming Source
             ****************************************************************************/

            //Setup the Kafka Connector Descriptor
            ConnectorDescriptor kafkaConDesc
                    = new Kafka()
                    .version("universal")       //Support latest Kafka 1.0+
                    .topic("flink.kafka.streaming.source") //Topic Name
                    .startFromLatest()              //Start new
                    .property("bootstrap.servers",  //Broker list
                            "localhost:9092");

            //Derive format from the data received.
            FormatDescriptor auditFormat
                    = new Csv()
                    .deriveSchema();

            //Schema for the audit Table
            Schema auditSchema
                    = new Schema()
                    .field("ID", Types.INT)
                    .field("User", Types.STRING)
                    .field("Entity", Types.STRING)
                    .field("Operation", Types.STRING)
                    .field("Timestamp", Types.LONG)
                    .field("Duration", Types.INT)
                    .field("ChangeCount", Types.INT)
                    //Add processing time for windowing
                    .field("ProcessingTime",Types.SQL_TIMESTAMP)
                        .proctime();

            //Register the table
            streamTableEnv
                    .connect(kafkaConDesc)  // Source Connection
                    .withFormat(auditFormat) //Format
                    .withSchema(auditSchema)  //Schema
                    .inAppendMode()         //In append mode
                    .registerTableSource("audit_trail"); //table name

            //Create a table object
            Table auditTable = streamTableEnv
                    .scan("audit_trail");

            //Print output in append mode
            Utils.printStreamTable(streamTableEnv, auditTable, "AuditTable");

            /****************************************************************************
             *                 Windowing with Tumbling Windows
             ****************************************************************************/

            //Compute 5 second window summaries for Each user and Change Count

            Table tumbledWindowTable
                    = auditTable
                    .select("User, ChangeCount, ProcessingTime") //Select

                    .window(Tumble.over("5.seconds") //5 Sec window
                            .on("ProcessingTime") //Processing Time based
                            .as("FiveSecWindow"))   //Alias for window field

                    .groupBy("FiveSecWindow, User") //Group By
                    .select("FiveSecWindow.start,FiveSecWindow.end, " //Window Interval
                                    + " User,ChangeCount.sum");

            //Print the output
            streamTableEnv
                    .toRetractStream(tumbledWindowTable, Row.class)
                    .map(new MapFunction<Tuple2<Boolean,Row>, Object>() {
                        @Override
                        public Object map(Tuple2<Boolean,Row> retractRow) {
                            Row row = retractRow.f1;
                            System.out.println("Tumble Window 5 Sec : "
                                    + "Keep = " + retractRow.f0
                                    + " Data = " + row);
                            return row;
                        }
                    });


            /****************************************************************************
             *                 Windowing with Sliding Windows
             ****************************************************************************/

            //Compute 5 second window summaries for Each user and Change Count

            Table slidingWindowTable
                    = auditTable
                    .select("User, ChangeCount, ProcessingTime") //Select

                    .window(Slide.over("10.seconds") //10 Sec window
                            .every("5.seconds" )  //slide every 5 seconds
                            .on("ProcessingTime") //Processing Time based
                            .as("FiveSecWindow"))   //Alias for window field

                    .groupBy("FiveSecWindow, User") //Group By
                    .select("FiveSecWindow.start,FiveSecWindow.end, " //Window Interval
                            + " User,ChangeCount.sum");

            //Print the output
            streamTableEnv
                    .toRetractStream(slidingWindowTable, Row.class)
                    .map(new MapFunction<Tuple2<Boolean,Row>, Object>() {
                        @Override
                        public Object map(Tuple2<Boolean,Row> retractRow) {
                            Row row = retractRow.f1;
                            System.out.println("Sliding Window 10 Sec : "
                                    + "Keep = " + retractRow.f0
                                    + " Data = " + row);
                            return row;
                        }
                    });

            /****************************************************************************
             *                 Writing Table to Kafka
             ****************************************************************************/

            //Setup Kafka Producer Properties
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");

            //Serializer for the Data Row
            SerializationSchema<Row> kafkaSerializer =
                    new SerializationSchema<Row>() {

                        //Convert Row to Bytes
                        @Override
                        public byte[] serialize(Row row) {

                            String returnStr = row.getField(0) +","
                                    + row.getField(1) + ","
                                    + row.getField(2) + ","
                                    + row.getField(3);
                            return returnStr.getBytes();
                        }
                    };


            //Setup a Kafka Sink
            KafkaTableSink kafkaTableSink
                    = new KafkaTableSink(
                            slidingWindowTable.getSchema(), //Table Schema
                            "flink.kafka.streaming.sink", //Topic name
                            properties,             //Connection properties
                            java.util.Optional.of(  //Kafka Partitioner
                                    new FlinkFixedPartitioner<Row>()),
                            kafkaSerializer         //Serialize data
                            );

            //Register the Kafka sink as a Table
            streamTableEnv
                    .registerTableSink("kafka_trail", //Table name
                                kafkaTableSink);        //mapped sink

            //Insert contents of slidingWindowTable into the Kafka Table
            slidingWindowTable
                    .insertInto("kafka_trail");

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
