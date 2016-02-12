import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

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

		public AirlineMapperValue(AirlineMapperValue anotherAMV) {
			this.flightPrice = new DoubleWritable(anotherAMV.getFlightPrice().get());
			this.flightMonth = new IntWritable(anotherAMV.getFlightMonth().get());
			this.flightYearIs2015 = new BooleanWritable(anotherAMV.getFlightYearIs2015().get());
			this.flightYearBetween2010_2014 = new BooleanWritable(anotherAMV.getFlightYearBetween2010_2014().get());
			this.flightAirTime = new IntWritable(anotherAMV.getFlightAirTime().get());
			this.flightDistance = new IntWritable(anotherAMV.getFlightDistance().get());
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
				String carKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER);
				
				AirlineMapperValue amv = new AirlineMapperValue();
		
				String flYearStr = FileRecord.getValueOf(fields, FileRecord.Field.YEAR); 
				String flPriceStr = FileRecord.getValueOf(fields, FileRecord.Field.AVG_TICKET_PRICE);
				String flMonthStr = FileRecord.getValueOf(fields, FileRecord.Field.MONTH); 
				String flAirTimeStr = FileRecord.getValueOf(fields, FileRecord.Field.AIR_TIME);
				String flDistStr = FileRecord.getValueOf(fields, FileRecord.Field.DISTANCE);
				
				if(!flDistStr.equals("")){
					int flyear = Integer.parseInt(flYearStr);
					
					double flightPrice = Double.parseDouble(flPriceStr);
					int flightMonth = Integer.parseInt(flMonthStr);
					boolean flightYearIs2015 = flyear == 2015;
					boolean flightYearBetween2010_2014 = flyear >= 2010 && flyear <=2014;
					
					// TODO: mention in report n readme
					int flightAirTime;
					if(!flAirTimeStr.equals("")){
						flightAirTime = Integer.parseInt(flAirTimeStr);
					} else {
						// This value is guaranteed to be present since it is checked in the sanity check
						// Also, this is a better approximation of "time" with respect to the flight price
						// since ticket price is not determined by actual elapsed time, but scheduled time
						flightAirTime = Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.CRS_ELAPSED_TIME));
					}
					
					int flightDistance = Integer.parseInt(flDistStr);
					
					if(flightYearIs2015 || flightYearBetween2010_2014){
						amv.setFromJava(flightPrice, flightMonth, flightYearIs2015, flightYearBetween2010_2014, flightAirTime, flightDistance);
						context.write(new Text(carKey), amv);
					}
				}
			}
		}
	}
	
	public static class AirlineLinearRegressionReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			boolean is2015 = false;

			ArrayList<AirlineMapperValue> carAMVList = new ArrayList<AirlineMapperValue>();
			
			// If the carrier is active in 2015 (i.e. any flight has the flightYearIs2015 flag set)
			for (AirlineMapperValue eachAMV : listOfAMVs){
				carAMVList.add(new AirlineMapperValue(eachAMV));
				is2015 = eachAMV.getFlightYearIs2015().get() || is2015;
			}
			
			if(is2015){				
				for (AirlineMapperValue eachAMV : carAMVList){				
					// If this record is of flight between 2010-2014
					if(eachAMV.getFlightYearBetween2010_2014().get()){
						String lrCarrierOutput = eachAMV.getFlightDistance().get() + "\t" + eachAMV.getFlightAirTime().get() + "\t" + eachAMV.getFlightPrice().get(); 
						context.write(key, new Text(lrCarrierOutput));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if(args.length != 3){
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
		job.setReducerClass(AirlineLinearRegressionReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AirlineMapperValue.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);			
		
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
