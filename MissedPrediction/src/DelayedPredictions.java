import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
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


public class DelayedPredictions {

	static class RunConstants {
		static final String TESTING_MODE = "-testing";
		static final String TRAINING_MODE = "-training";
		static final String EVAL_MODE = "-evaluation";
		static final String EMR_TYPE = "-emr";
		static final String PSEUDO_TYPE = "-pseudo";
		
		static final String TIMINGFOLDER = "timingOutput/time.txt";
	}
	
	
	public static void main(String[] args) {
		
		for(String a : args) System.out.println(a);
		
		if(!(args.length == 7)){
			System.out.println(args.length + " ");
			displayUsageAndExit();
		}
		
		String runType = args[0];
		String inputTrainPath = args[1];
		String inputTestPath = args[2];
		String outputPath = args[3];
		String rfModel = args[4];
		String predictionMode = args[5];
		String validationFile = args[6];
		
		if(!(areParamsValid(runType, predictionMode))){
			displayUsageAndExit();
		}		
		
		long startTime = System.currentTimeMillis();
		
		if(predictionMode.equalsIgnoreCase(RunConstants.TESTING_MODE)){
			RFPredictor.main(args);
			printEndTime(runType.equalsIgnoreCase(RunConstants.PSEUDO_TYPE), startTime, "TESTING MODE for A6", RunConstants.TIMINGFOLDER);
			System.exit(0);
		}
		
		if(predictionMode.equalsIgnoreCase(RunConstants.EVAL_MODE)){
			ComparePredictions.main(args);
			printEndTime(runType.equalsIgnoreCase(RunConstants.PSEUDO_TYPE), startTime, "EVALUATION MODE for A6", RunConstants.TIMINGFOLDER);
			System.exit(0);
		}
		
		if(predictionMode.equalsIgnoreCase(RunConstants.TRAINING_MODE)){
			DelayedTrain.main(args);
			printEndTime(runType.equalsIgnoreCase(RunConstants.PSEUDO_TYPE), startTime, "TRAINING MODE for A6", RunConstants.TIMINGFOLDER);
			System.exit(0);
		}
	}
	
	private static void printEndTime(boolean writeToLocalFile, long startTime, String printMessage, String outputDest) {
		long endTime = System.currentTimeMillis();
		long totalTime = (endTime - startTime) / 1000;
		
		System.out.println("\n\n=====================================================================================");
		System.out.println("\t\t\t\t" + "<< END OF JOB TIMING INFORMATION >>");
		System.out.println("\t\t" + "Message: " + printMessage + "\n\t" + "Job took " + totalTime + " secs");
		System.out.println("\t\t" + "Start: " + new Date(startTime).toString() + "\t" + "End: " + new Date(endTime).toString());
		System.out.println("=====================================================================================");
		
		if(!writeToLocalFile){
			return;
		}
		
		String lineToWrite = "Message: " + printMessage + 
				"\t" + "Job took " + totalTime + " secs" + 
				"\t" + "Start: " + new Date(startTime).toString() + 
				"\t" + "End: " + new Date(endTime).toString();
		
		FileWriter writer;
		try {
			writer = new FileWriter(outputDest, true);
			writer.append(lineToWrite+"\n");
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
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
		System.err.println("\tRun ardument one of : " + RunConstants.EVAL_MODE + ", " + RunConstants.TESTING_MODE + ", " + RunConstants.TRAINING_MODE);
		
		System.err.println("\narg 6:");
		System.err.println("\t<Path to validation/evaluation file>");
		
		System.exit(-1);
	}
	
	public static boolean areParamsValid(String runType, String predictionMode) {
		
		boolean runBool = runType.equalsIgnoreCase(RunConstants.EMR_TYPE) || runType.equalsIgnoreCase(RunConstants.PSEUDO_TYPE);
		boolean predBool = predictionMode.equalsIgnoreCase(RunConstants.TESTING_MODE) || predictionMode.equalsIgnoreCase(RunConstants.TRAINING_MODE) 
				|| predictionMode.equalsIgnoreCase(RunConstants.EVAL_MODE);
		
		return runBool && predBool;
	}
}
