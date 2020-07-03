package com.flinklearn.expdata.chapter4;

import com.flinklearn.expdata.common.Utils;
import com.flinklearn.expdata.datasouce.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.* ;
import org.apache.flink.types.Row;


public class BasicStreamingAnalytics {

    public static void main(String[] args) {

        try {

            /****************************************************************************
             *                 Setting up the Stream Environment
             ****************************************************************************/

            Utils.printHeader("Starting the Stream SQL Analytics program....");

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
                    .field("ChangeCount", Types.INT);

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
             *                 Analyzing a Stream with Continuous Query
             ****************************************************************************/

            //Find running summary by User for Entity Type Customer
            Table customerAnalytics
                    = auditTable
                        .filter("Entity === 'Customer'")
                        .groupBy("User")
                        .select("User, User.count, Duration.sum");

            //Print the output, both the retract and add records
            streamTableEnv
                    .toRetractStream(customerAnalytics, Row.class)
                    .map(new MapFunction<Tuple2<Boolean,Row>, Object>() {
                        @Override
                        public Object map(Tuple2<Boolean,Row> retractRow) {
                            Row row = retractRow.f1;
                            System.out.println("Customer Stream : "
                                    + "Keep = " + retractRow.f0
                                    + " Data = " + row);
                            return row;
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
