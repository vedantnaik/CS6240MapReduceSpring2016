import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVReader;
import weka.classifiers.Classifier;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class RFPredictor {
	public static void main(String[] args) {
		
		String runType = args[0];
		String inputTrainPath = args[1];
		String inputTestPath = args[2];
		String outputPath = args[3];
		String rfModel = args[4];
		String train_or_test = args[5];
		
		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();
		
		conf.set("rfModelLocation", rfModel);
		conf.set("testLocation", inputTestPath);
		conf.set("outputLocation", outputPath);
		
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("RFPredictor");
			job.setJarByClass(RFPredictor.class);
			
			job.setMapperClass(RFPredictorMapper.class);
			job.setReducerClass(RFPredictoReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(AirlineMapperValue.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(inputTestPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			// Wait for the MapReduce job to complete before exiting application
			if(runType.equals("-pseudo")){
				if(job.waitForCompletion(true)){
					printEndTime(startTime, runType, System.getenv("HADOOP_HOME")+"/pseudo"+"Time.csv");
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
	
	private static void displayUsageAndExit() {
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
		
		System.exit(-1);
	}
	
	private static void printEndTime(long startTime, String runType, String outputDest) {
		long endTime = System.currentTimeMillis();
		long totalTime = (endTime - startTime) / 1000;
		System.out.println("\nRun type: " + runType.substring(1) + " [TESTING PHASE] took " + totalTime + " secs");
		
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
	
	private static boolean areParamsValid(String runType) {
		return runType.equalsIgnoreCase("-pseudo") || runType.equalsIgnoreCase("-emr");
	}
	
	
	/*public static void old_main(String[] args) {
		
		
		String modelFolder = args[0];
		String testInputFile = args[1];
		String outputFolder = args[2];
		HashMap<String, Classifier> storedClassifier = null;
		HashMap<String, Boolean> predictionsMap = new HashMap<String, Boolean>();
		
		try {
			storedClassifier = RFModelMaker.getModelsFromFileSystem(modelFolder);
		} catch (Exception e) {
			System.err.println("Unable to read stored models");
			e.printStackTrace();
			System.exit(1);
		}
		
		BufferedReader buffered = null;
		try {
			InputStream fileStream = new FileInputStream(testInputFile);
	    	InputStream gzipStream = new GZIPInputStream(fileStream);
	    	Reader decoder = new InputStreamReader(gzipStream, "ASCII");
	    	buffered = new BufferedReader(decoder);
		} catch (IOException e) {
			System.err.println("Unable to read test input file");
			e.printStackTrace();
			System.exit(1);
		}
		
		
		RFModelMaker rfModel = new RFModelMaker();
		Instances testingInstances = new Instances("Testing", rfModel.getAirlineAttributes(), RFModelMaker.Constants.ATTR_SIZE); 
		testingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
		
		HashMap<String, Boolean> predictionOutput = new HashMap<String, Boolean>();
		String fileEntryString;
		
		try {
			while((fileEntryString = buffered.readLine())!=null){
				
				String fileEntry = fileEntryString.toString();
				fileEntry = fileEntry.replaceAll("\"", "");
				String correctedString = fileEntry.replaceAll(", ", ":");
				String entryNumber = correctedString.substring(0, correctedString.indexOf(","));
				correctedString = correctedString.substring(correctedString.indexOf(",")+1);
				String[] fields = correctedString.split(",");
				
				Instance inst = new DenseInstance(RFModelMaker.Constants.ATTR_SIZE);
				inst.setDataset(testingInstances);
				
				try {
					inst.setValue(0, (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.CRS_ARR_TIME)));
					inst.setValue(1, (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.CRS_DEP_TIME)));
					inst.setValue(2, (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.QUARTER)));
					inst.setValue(3, (double) FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN).hashCode());
					inst.setValue(4, (double) FileRecord.getValueOf(fields, FileRecord.Field.DEST).hashCode());
					inst.setValue(5, (double) FileRecord.getValueOf(fields, FileRecord.Field.CARRIER).hashCode());
					inst.setValue(6, (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.DAY_OF_MONTH)));
					inst.setValue(7, (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.DAY_OF_WEEK)));
					inst.setValue(8, (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.DISTANCE_GROUP)));
					inst.setValue(9, (double) FileRecord.dateIsAroundHoliday(FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE), 2));
				} catch (NumberFormatException e1) {
					continue;
				}
	
				inst.setValue(RFModelMaker.Constants.PRED_CLASS_INDEX, Double.NaN);
				
				
				
//				System.out.println("predict:"
//						+ (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.CRS_ARR_TIME)) + " "
//						+ (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.CRS_DEP_TIME)) + " "
//						+ (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.QUARTER)) + " "
//						+ (double) FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN).hashCode() + " "
//						+ (double) FileRecord.getValueOf(fields, FileRecord.Field.DEST).hashCode() + " "
//						+ (double) FileRecord.getValueOf(fields, FileRecord.Field.CARRIER).hashCode() + " "
//						+ (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.DAY_OF_MONTH)) + " "
//						+ (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.DAY_OF_WEEK)) + " "
//						+ (double) Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.DISTANCE_GROUP)) + " "
//						+ (double) FileRecord.dateIsAroundHoliday(FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE), 2)
//						+ "");
				
				
				// key [FL_NUM]_[FL_DATE]_[CRS_DEP_TIME]
				String uniqueFlightKey = FileRecord.getValueOf(fields, FileRecord.Field.FL_NUM)+"_"+
									FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE)+"_"+
									FileRecord.getValueOf(fields, FileRecord.Field.CRS_DEP_TIME);
				
				double prediction=0.0;
				try {
					prediction = storedClassifier.get(FileRecord.getValueOf(fields, FileRecord.Field.MONTH)).classifyInstance(inst);
				} catch (Exception e) {
					System.err.println("failed to predict for flight " + entryNumber + " " + uniqueFlightKey);
					//e.printStackTrace();
					continue;
				}
				System.out.println("amable to predict for flight " + entryNumber + " " + uniqueFlightKey);
				predictionsMap.put(uniqueFlightKey, (prediction == 1.0 ? true : false));
//				System.out.println(uniqueFlightKey + " : " + prediction);
			}
		} catch (IOException e) {
			System.err.println("Unable to read an entry from the file : " + testInputFile);
			e.printStackTrace();
		}
	
		System.out.println(predictionsMap.size() + "final output");
	
	}*/
	
}
