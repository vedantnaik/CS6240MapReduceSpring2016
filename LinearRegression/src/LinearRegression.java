import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LinearRegression {

	public static class AirlineMapperValue implements Writable {
		DoubleWritable flightPrice;
		IntWritable flightMonth;
		BooleanWritable flightYearIs2015;
		BooleanWritable flightYearBetween2010_2014;
		IntWritable flightAirTime;
		IntWritable flightDistance;
		
		public AirlineMapperValue(DoubleWritable flightPrice, IntWritable flightMonth, BooleanWritable flightYearIs2015,
				BooleanWritable flightYearBetween2010_2014, IntWritable flightAirTime, IntWritable flightDistance) {
			this.flightPrice = flightPrice;
			this.flightMonth = flightMonth;
			this.flightYearIs2015 = flightYearIs2015;
			this.flightYearBetween2010_2014 = flightYearBetween2010_2014;
			this.flightAirTime = flightAirTime;
			this.flightDistance = flightDistance;
		}

		public AirlineMapperValue() {
			flightPrice = new DoubleWritable();
			flightMonth = new IntWritable();
			flightYearIs2015 = new BooleanWritable();
			flightYearBetween2010_2014 = new BooleanWritable();
			flightAirTime = new IntWritable();
			flightDistance = new IntWritable();
		}

		void set(DoubleWritable flightPrice, IntWritable flightMonth, BooleanWritable flightYearIs2015,
				BooleanWritable flightYearBetween2010_2014, IntWritable flightAirTime, IntWritable flightDistance) {
			this.flightPrice = flightPrice;
			this.flightMonth = flightMonth;
			this.flightYearIs2015 = flightYearIs2015;
			this.flightYearBetween2010_2014 = flightYearBetween2010_2014;
			this.flightAirTime = flightAirTime;
			this.flightDistance = flightDistance;
		}
		
		void setFromJava(double flightPrice, int flightMonth, boolean flightYearIs2015, boolean flightYearBetween2010_2014, int flightAirTime, int flightDistance) {
			this.flightPrice = new DoubleWritable(new Double(flightPrice));
			this.flightMonth = new IntWritable(new Integer(flightMonth));
			this.flightYearIs2015 = new BooleanWritable(new Boolean(flightYearIs2015));
			this.flightYearBetween2010_2014 = new BooleanWritable(new Boolean(flightYearBetween2010_2014));
			this.flightAirTime = new IntWritable(new Integer(flightAirTime));
			this.flightDistance = new IntWritable(new Integer(flightDistance));;
		}
		
		@Override
		public void readFields(DataInput inVal) throws IOException {
			flightPrice.readFields(inVal);
			flightMonth.readFields(inVal);
			flightYearIs2015.readFields(inVal);
			flightYearBetween2010_2014.readFields(inVal);
			flightAirTime.readFields(inVal);
			flightDistance.readFields(inVal);
		}

		@Override
		public void write(DataOutput outVal) throws IOException {
			flightPrice.write(outVal);
			flightMonth.write(outVal);
			flightYearIs2015.write(outVal);
			flightYearBetween2010_2014.write(outVal);
			flightAirTime.write(outVal);
			flightDistance.write(outVal);
		}

		@Override
		public String toString() {
			return "[Price:" + flightPrice.get() + " " 
					+ "Month:" + flightMonth.get() + " "
					+ "Is2015:" + (flightYearIs2015.get() ? "TRUE" : "FALSE") + " "
					+ "Between2010_2014:" + (flightYearBetween2010_2014.get() ? "TRUE" : "FALSE") + " "
					+ "AirTime:" + flightAirTime.get() + " "
					+ "Distance:"+ flightDistance.get() +"]";
		}

		public DoubleWritable getFlightPrice() {
			return flightPrice;
		}

		public void setFlightPrice(DoubleWritable flightPrice) {
			this.flightPrice = flightPrice;
		}

		public IntWritable getFlightMonth() {
			return flightMonth;
		}

		public void setFlightMonth(IntWritable flightMonth) {
			this.flightMonth = flightMonth;
		}

		public BooleanWritable getFlightYearIs2015() {
			return flightYearIs2015;
		}

		public void setFlightYearIs2015(BooleanWritable flightYearIs2015) {
			this.flightYearIs2015 = flightYearIs2015;
		}

		public BooleanWritable getFlightYearBetween2010_2014() {
			return flightYearBetween2010_2014;
		}

		public void setFlightYearBetween2010_2014(BooleanWritable flightYearBetween2010_2014) {
			this.flightYearBetween2010_2014 = flightYearBetween2010_2014;
		}

		public IntWritable getFlightAirTime() {
			return flightAirTime;
		}

		public void setFlightAirTime(IntWritable flightAirTime) {
			this.flightAirTime = flightAirTime;
		}

		public IntWritable getFlightDistance() {
			return flightDistance;
		}

		public void setFlightDistance(IntWritable flightDistance) {
			this.flightDistance = flightDistance;
		}
	}

	public static class AirlineMapper extends Mapper<Object, Text, Text, AirlineMapperValue> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String fileEntry = value.toString();
			fileEntry = fileEntry.replaceAll("\"", "");
			String correctedString = fileEntry.replaceAll(", ", ":");
			String[] fields = correctedString.split(",");
			
			if (FileRecord.csvHeaders.size() == fields.length && FileRecord.isRecordValid(fields)){
				String carMonthKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + ","
									+ FileRecord.getValueOf(fields,FileRecord.Field.MONTH);
				
				AirlineMapperValue amv = new AirlineMapperValue();
		
				int flyear = Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.YEAR));
				
				double flightPrice = Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.AVG_TICKET_PRICE));
				int flightMonth = Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.MONTH));
				boolean flightYearIs2015 = flyear == 2015;
				boolean flightYearBetween2010_2014 = flyear >= 2010 && flyear <=2014;
				int flightAirTime = Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.AIR_TIME));
				int flightDistance = Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.DISTANCE));
				
				amv.setFromJava(flightPrice, flightMonth, flightYearIs2015, flightYearBetween2010_2014, flightAirTime, flightDistance);
				
				context.write(new Text(carMonthKey), amv);
			}
		}
	}
	
	public static class AirlineMedianReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
		
		private HashMap<String, List<AirlineMapperValue>> carMapperOutput = new HashMap<String, List<AirlineMapperValue>>();
		private HashSet<String> activeIn2015Set = new HashSet<String>();
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			List<AirlineMapperValue> mapperList = new ArrayList<AirlineMapperValue>();
			
			String[] keyParts = key.toString().split(",");
			String flcarrier = keyParts[0];
			
			boolean is2015 = false;

			for (AirlineMapperValue eachAMV : listOfAMVs){
				mapperList.add(eachAMV);
				is2015 = eachAMV.getFlightYearIs2015().get() || is2015;
			}
			
			// Maintain a set of carriers active in 2015
			if(is2015){
				activeIn2015Set.add(flcarrier);
			}
			
			// Group all mapper value objects of one carrier from all months together in one list
			if(!carMapperOutput.containsKey(flcarrier)){
				carMapperOutput.put(flcarrier,new ArrayList<AirlineMapperValue>());
			}
			carMapperOutput.get(flcarrier).addAll(mapperList);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(String carKey : carMapperOutput.keySet()){
				if(activeIn2015Set.contains(carKey)){
					List<AirlineMapperValue> listOfAMVs = carMapperOutput.get(carKey);
					for(AirlineMapperValue amv : listOfAMVs){
						if (amv.getFlightYearBetween2010_2014().get()){
							
							String linearRegressionValues = amv.getFlightAirTime().get() + "\t" +
															amv.getFlightDistance().get() + "\t" +
															amv.getFlightPrice().get();
							
							context.write(new Text(carKey), new Text(linearRegressionValues));
						}
					}
				}
			}
		}
	}
	
	public static class AirlinePartitioner extends Partitioner<Text, AirlineMapperValue>{

		private static final String[] uc = {"9E", "AA", "AS", "B6", "DL", "EV", "F9", "FL", "HA", 
											"MQ", "NK", "OO", "UA", "US", "VX", "WN", "YV"}; 
		private static final ArrayList<String> UNIQUE_CARRIERS = new ArrayList<String>(Arrays.asList(uc));

		@Override
		public int getPartition(Text carMonthKey, AirlineMapperValue amv, int numberOfReducers) {
			String[] parts = carMonthKey.toString().split(",");
			String carrier = parts[0];
			return UNIQUE_CARRIERS.indexOf(carrier) % numberOfReducers;
		}		
	}
	
	
	public static void main(String[] args) throws Exception {

		if(args.length != 4){
			displayUsageAndExit();
		}
		
		String runType = args[0];
		String inputPath = args[1];
		String outputPath = args[2];
		
		if(!(areParamsValid(runType))){
			displayUsageAndExit();
		}
		
		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJobName("LinearRegression");
		job.setJarByClass(LinearRegression.class);

		job.setMapperClass(AirlineMapper.class);
		job.setReducerClass(AirlineMedianReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AirlineMapperValue.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);			
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		// Wait for the MapReduce job to complete before exiting application
		if(runType.equals("-pseudo")){
			if(job.waitForCompletion(true)){
				printEndTime(startTime, runType, System.getenv("HADOOP_HOME")+"/pseudo"+"Time.csv");
				System.exit(0);
			}
			System.exit(1);
		}
		
		// TODO: add time recording mechanism for EMR: Write file to S3
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static void printEndTime(long startTime, String runType, String outputDest) {
		long endTime = System.currentTimeMillis();
		long totalTime = (endTime - startTime) / 1000;
		System.out.println("\nRun type: " + runType.substring(1) + " took " + totalTime + " secs");
		
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

	private static void displayUsageAndExit() {
		System.err.println("Invalid inputs given.");
		System.err.println("USAGE:");
		
		System.err.println("\narg 0:");
		System.err.println("\t-pseudo\t\t:\trun on a pseudo cluster");
		System.err.println("\t-emr\t\t:\trun on amazon web services");
		
		System.err.println("\narg 1:");
		System.err.println("\t<Path to input>");
		
		System.err.println("\narg 2:");
		System.err.println("\t<Path to output>");
		
		System.exit(-1);
	}

	private static boolean areParamsValid(String runType) {
		return runType.equalsIgnoreCase("-pseudo") || runType.equalsIgnoreCase("-emr");
	}
}
