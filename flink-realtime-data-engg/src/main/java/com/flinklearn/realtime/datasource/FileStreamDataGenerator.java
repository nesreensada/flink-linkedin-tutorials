package com.flinklearn.realtime.datasource;

import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/****************************************************************************
 * This Generator generates a series of data files in the raw_data folder
 * It is an audit trail data source.
 * This can be used for streaming consumption of data by Flink
 ****************************************************************************/

public class FileStreamDataGenerator implements Runnable {


    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static void main(String[] args) {
        FileStreamDataGenerator fsdg = new FileStreamDataGenerator();
        fsdg.run();
    }

    public void run() {

        try {

            //Define list of users
            List<String> appUser = new ArrayList<String>();
            appUser.add("Tom");
            appUser.add("Harry");
            appUser.add("Bob");

            //Define list of application operations
            List<String> appOperation = new ArrayList<String>();
            appOperation.add("Create");
            appOperation.add("Modify");
            appOperation.add("Query");
            appOperation.add("Delete");

            //Define list of application entities
            List<String> appEntity = new ArrayList<String>();
            appEntity.add("Customer");
            appEntity.add("SalesRep");

            //Define the data directory to output the files
            String dataDir = "data/raw_audit_trail";

            //Clean out existing files in the directory
            FileUtils.cleanDirectory(new File(dataDir));

            //Define a random number generator
            Random random = new Random();

            //Generate 100 sample audit records, one per each file
            for(int i=0; i < 100; i++) {

                //Capture current timestamp
                String currentTime = String.valueOf(System.currentTimeMillis());

                //Generate a random user
                String user = appUser.get(random.nextInt(appUser.size()));
                //Generate a random operation
                String operation = appOperation.get(random.nextInt(appOperation.size()));
                //Generate a random entity
                String entity= appEntity.get(random.nextInt(appEntity.size()));
                //Generate a random duration for the operation
                String duration = String.valueOf(random.nextInt(10) + 1 );
                //Generate a random value for number of changes
                String changeCount = String.valueOf(random.nextInt(4) + 1);

                //Create a CSV Text array
                String[] csvText = { String.valueOf(i), user, entity,
                                        operation, currentTime, duration, changeCount} ;

                //Open a new file for this record
                FileWriter auditFile = new FileWriter(dataDir
                                            + "/audit_trail_" + i + ".csv");
                CSVWriter auditCSV = new CSVWriter(auditFile);

                //Write the audit record and close the file
                auditCSV.writeNext(csvText);

                System.out.println(ANSI_BLUE + "FileStream Generator : Creating File : "
                            + Arrays.toString(csvText) + ANSI_RESET);

                auditCSV.flush();
                auditCSV.close();

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
