package com.flinklearn.realtime.chapter2;

import com.flinklearn.realtime.common.MapCountPrinter;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import org.apache.flink.core.fs.Path;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class BasicStreamingOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();

            //Keeps the ordering of records. Else multiple threads can change
            //sequence of printing.
            streamEnv.setParallelism(1);

            /****************************************************************************
             *                  Read CSV File Stream into a DataStream.
             ****************************************************************************/

            //Define the data directory to monitor for new files
            String dataDir = "data/raw_audit_trail";

            //Define the text input format based on the directory
            TextInputFormat auditFormat = new TextInputFormat(
                    new org.apache.flink.core.fs.Path(dataDir));

            //Create a Datastream based on the directory
            DataStream<String> auditTrailStr
                        = streamEnv.readFile(auditFormat,
                            dataDir,    //Director to monitor
                            FileProcessingMode.PROCESS_CONTINUOUSLY,
                            1000); //monitor interval

            //Convert each record to an Object
            DataStream<AuditTrail> auditTrailObj
                    = auditTrailStr
                        .map(new MapFunction<String,AuditTrail>() {
                            @Override
                            public AuditTrail map(String auditStr) {
                                System.out.println("--- Received Record : " + auditStr);
                                return new AuditTrail(auditStr);
                            }
                        });

            /****************************************************************************
             *                  Perform computations and write to output sink.
            ****************************************************************************/

            //Print message for audit trail counts
            MapCountPrinter.printCount(
                    auditTrailObj.map( i -> (Object)i),
                    "Audit Trail : Last 5 secs");

            //Window by 5 seconds, count #of records and save to output
           DataStream<Tuple2<String,Integer>> recCount
                    = auditTrailObj
                    .map( i
                            -> new Tuple2<String,Integer>
                            (String.valueOf(System.currentTimeMillis()),1))
                    .returns(Types.TUPLE(Types.STRING ,Types.INT))
                    .timeWindowAll(Time.seconds(5))
                    .reduce((x,y) ->
                            (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)));


            //Define the output directory to store summary information
            String outputDir = "data/five_sec_summary";
            //Clean out existing files in the directory
            FileUtils.cleanDirectory(new File(outputDir));

            //Setup a streaming file sink to the output directory
            final StreamingFileSink<Tuple2<String,Integer>> countSink
                    = StreamingFileSink
                        .forRowFormat(new Path(outputDir),
                                new SimpleStringEncoder<Tuple2<String,Integer>>
                                        ("UTF-8"))
                        .build();

            //Add the file sink as sink to the DataStream.
            recCount.addSink(countSink);


            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/
            //Start the File Stream generator on a separate thread
            Utils.printHeader("Starting File Data Generator...");
            Thread genThread = new Thread(new FileStreamDataGenerator());
            genThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Audit Trail Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
