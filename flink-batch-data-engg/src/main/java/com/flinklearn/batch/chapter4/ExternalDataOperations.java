package com.flinklearn.batch.chapter4;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

public class ExternalDataOperations {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Data Sources / Sinks Program....");

            //Get execution environment
            final ExecutionEnvironment env
                    = ExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                      Load data from MySQL
             ****************************************************************************/

            //Define data types for data read from the DB
            TypeInformation[] orderFieldTypes = new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
            };

            RowTypeInfo orderRowInfo = new RowTypeInfo(orderFieldTypes);

            //Setup the JDBC Input format
            JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                    .setDrivername("com.mysql.cj.jdbc.Driver")
                    .setDBUrl("jdbc:mysql://localhost:3306/flink?useLegacyDatetimeCode=false&serverTimezone=UTC")
                    .setUsername("root")
                    .setPassword("password")
                    .setQuery("SELECT Customer, Quantity,Rate from sales_orders")
                    .setRowTypeInfo(orderRowInfo)
                    .finish();

            //Retrieve Data
            DataSet<Row> orderRecords = env.createInput(jdbcInputFormat);

            Utils.printHeader("Data retrieved from Database");
            orderRecords.first(5).print();

            /****************************************************************************
             *                      Perform Data Processing
             ****************************************************************************/

            //Compute Order Value ( Count * Rate ) using Map and return tuples
            DataSet<Tuple2<String,Double>> orderValues
                    = orderRecords.map(new MapFunction<Row,
                    Tuple2<String, Double>>()
                    {
                        @Override
                        public Tuple2<String, Double> map(Row row) throws Exception {

                            return new Tuple2(
                                    row.getField(0), //Get Customer
                                    (Integer)row.getField(1)
                                            * (Double)row.getField(2)); //Get Order Value
                        }

                    });

            //Summarize total order value by customer
            DataSet<Tuple2<String,Double>> orderSummary
                    = orderValues
                        .groupBy(0)
                        .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                            @Override
                            public Tuple2<String, Double> reduce(
                                    Tuple2<String, Double> row1, Tuple2<String, Double> row2)
                            {
                                return new Tuple2(row1.f0, row1.f1 + row2.f1);
                            }
                        });

            //Convert Tuple back to Row
            DataSet<Row> summaryResult
                    = orderSummary.map(new MapFunction<Tuple2<String, Double>,
                                Row>()
                        {
                            @Override
                            public Row map(Tuple2<String, Double> rec) throws Exception {

                                return Row.of(rec.f0,rec.f1);
                            }

                        });

            Utils.printHeader("Customer Summary Computed");
            summaryResult.print();

             /****************************************************************************
             *                      Update data back to MySQL
             ****************************************************************************/

            //Define Output Format Builder
            JDBCOutputFormat.JDBCOutputFormatBuilder orderOutputBuilder =
                    JDBCOutputFormat.buildJDBCOutputFormat()
                            .setDrivername("com.mysql.cj.jdbc.Driver")
                            .setDBUrl("jdbc:mysql://localhost:3306/flink?useLegacyDatetimeCode=false&serverTimezone=UTC")
                            .setUsername("root")
                            .setPassword("password")
                            .setQuery("INSERT INTO customer_summary VALUES (?,?) ")
                            .setSqlTypes(new int[] {Types.VARCHAR, Types.DOUBLE});

            //Define DataSink for data
            summaryResult.output(orderOutputBuilder.finish());

            //Triggers the program execution.
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
