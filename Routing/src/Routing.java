import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * -------------------------------------------------------------------------------------------------------
 * 										ROUTING - MAIN CLASS
 * 
 * This is the main class for routing.  Usage:
 * arg 0 : -emr or -pseudo: This decides whether the program runs on the pseudo mode or emr mode
 * arg 1 : path to the training data input
 * arg 2 : Path to the testing data input
 * arg 3 : Output folder
 * arg 4 : Path to the folder containing the model files
 * arg 5 : training or process or evaluation : This decides whether the program is training the model,
 * 			processing the request file to generate itineraries or evaluating the itineraries agains the
 * 			validation file
 * arg 6 : Validation file => 04missed.csv.gz
 * 
 * -------------------------------------------------------------------------------------------------------
 */

public class Routing {

	public static void main(String[] args) {
		
		for(String a : args)
			System.out.println(a);
		
		if(args.length < 6 || args.length > 7){
			System.out.println(args.length + " ");
			displayUsageAndExit();
		}
		
		String runType = args[0];	// -emr or -pseudo
		String predictionMode = args[5]; // training or processRequest or evaluation
		
		if(!(areParamsValid(runType))){
			displayUsageAndExit();
		}		
		
		if(predictionMode.equalsIgnoreCase("processRequest")){
			RequestProcessor.main(args);
			System.exit(0);
		}
		
		if(predictionMode.equalsIgnoreCase("evaluation")){
			CompareResults.main(args);
			System.exit(0);
		}
		
		String inputTrainPath = args[1];
		String inputTestPath = args[2];
		String outputPath = args[3];
		String rfModel = args[4];

		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();
		
		conf.set("rfModelLocation", rfModel);
		conf.set("testLocation", inputTestPath);
		conf.set("outputLocation", outputPath);
		
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("Routing");
			job.setJarByClass(Routing.class);
			
			job.setMapperClass(AirlineMapper.class);
			job.setReducerClass(AirlineRoutingReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(AirlineMapperValue.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);			
			FileInputFormat.addInputPath(job, new Path(inputTrainPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			// Wait for the MapReduce job to complete before exiting application
			if(runType.equals("-pseudo")){
				if(job.waitForCompletion(true)){
					//printEndTime(startTime, runType, System.getenv("HADOOP_HOME")+"/pseudo"+"Time.csv");
					System.out.println("Job completed, check the output");
					System.exit(0);
				}
				System.exit(1);
			}
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private static void printEndTime(long startTime, String runType, String outputDest) {
		long endTime = System.currentTimeMillis();
		long totalTime = (endTime - startTime) / 1000;
		System.out.println("\nRun type: [ROUTING MR JOB 1] " + runType.substring(1) + " took " + totalTime + " secs");
		
		String lineToWrite = runType.substring(1) + "," + totalTime;
		
		FileWriter writer;
		try {
			writer = new FileWriter(outputDest, true);
			writer.append(lineToWrite+"\n");
			writer.close();
		} catch (FileNotFoundException e) {
			System.err.println("Unable to write in timeOutput [file not found] " + runType);
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.err.println("Unable to write in timeOutput [unsupported encoding] " + runType);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public static void displayUsageAndExit() {
		System.err.println("Invalid inputs given.");
		System.err.println("USAGE:");
		
		System.err.println("\narg 0:");
		System.err.println("\t-pseudo\t\t:\trun on a pseudo cluster");
		System.err.println("\t-emr\t\t:\trun on amazon web services");
		
		System.err.println("\narg 1:");
		System.err.println("\t<Path to training input>");
		
		System.err.println("\narg 2:");
		System.err.println("\t<Path to test input>");
		
		System.err.println("\narg 3:");
		System.err.println("\t<Path to output>");
		
		System.err.println("\narg 4:");
		System.err.println("\t<Path to random forest model>");
		
		System.err.println("\narg 5:");
		System.err.println("\t<training or testing or evaluation>");
		
		System.err.println("\narg 6:");
		System.err.println("\t<Path to validation file>");
		
		System.exit(-1);
	}
	
	public static boolean areParamsValid(String runType) {
		return runType.equalsIgnoreCase("-pseudo") || runType.equalsIgnoreCase("-emr");
	}
}
