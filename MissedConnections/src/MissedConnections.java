import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissedConnections {

	/*
	 * A connection is any pair of flight F and G of the same carrier such as F.Destination = G.Origin 
	 * 
	 * 	flt F	--->	SomeAirport		---> flt G
	 * 										G leaves 30mins-6hours after F arrives
	 * 
	 * and the scheduled departure of G is <= 6 hours and >= 30 minutes after the scheduled arrival of F.
	 * 
	 * A connection is missed when the actual arrival of F < 30 minutes before the actual departure of G.
	 * 
	 * Solution:
	 * 
	 * Output of mapper:
	 * 	Key	:	we want flights of the same carrier and same day
	 * 			so we propose a key like this	(hyphen separated)
	 * 			<CARRIER>-<DATE>
	 * 
	 * Since this will lead to a lot of keys, we could have a partitioner that sends dates of same carriers 
	 * to the same Reducer. I.E. based on <CARRIER>
	 * 
	 * Value :	We will need the following from each flight record:
	 * 			DESTINATION
	 * 			ORIGIN
	 * 			Scheduled Times
	 * 			Actual Times
	 * 
	 * */
	
	public static class AirlineMapperValue implements Writable {
		// TODO: 04_edge: Add long for time
		Text origin;
		Text destination;
		Text crsArrTime;
		Text crsDepTime;
		Text actualArrTime;
		Text actualDepTime;
		
		public AirlineMapperValue(){
			this.origin = new Text();
			this.destination = new Text();
			this.crsArrTime = new Text();
			this.crsDepTime = new Text();
			this.actualArrTime = new Text();
			this.actualDepTime = new Text();
		}
		
		public AirlineMapperValue(Text origin, Text destination, Text crsArrTime, Text crsDepTime,
				Text actualArrTime, Text actualDepTime) {
			this.origin = origin;
			this.destination = destination;
			this.crsArrTime = crsArrTime;
			this.crsDepTime = crsDepTime;
			this.actualArrTime = actualArrTime;
			this.actualDepTime = actualDepTime;
		}
		
		public AirlineMapperValue(AirlineMapperValue amv) {
			this.origin = amv.getOrigin();
			this.destination = amv.getDestination();
			this.crsArrTime = amv.getCrsArrTime();
			this.crsDepTime = amv.getCrsDepTime();
			this.actualArrTime = amv.getActualArrTime();
			this.actualDepTime = amv.getActualDepTime();
		}
		
		@Override
		public void readFields(DataInput inVal) throws IOException {
			origin.readFields(inVal);
			destination.readFields(inVal);
			crsArrTime.readFields(inVal);
			crsDepTime.readFields(inVal);
			actualArrTime.readFields(inVal);
			actualDepTime.readFields(inVal);
		}

		@Override
		public void write(DataOutput outVal) throws IOException {
			origin.write(outVal);
			destination.write(outVal);
			crsArrTime.write(outVal);
			crsDepTime.write(outVal);
			actualArrTime.write(outVal);
			actualDepTime.write(outVal);
		}

		@Override
		public String toString() {
			return "[Origin:" + origin.toString() + " " 
					+ "Dest:" + destination.toString() + " "
					+ "crsArr:" + crsArrTime.toString() + " "
					+ "crsDep:" + crsDepTime.toString() + " "
					+ "actualArr:" + actualArrTime.toString() + " "
					+ "actualDep:"+ actualDepTime.toString() +"]";
		}

		public Text getOrigin() {
			return origin;
		}

		public void setOrigin(Text origin) {
			this.origin = origin;
		}

		public Text getDestination() {
			return destination;
		}

		public void setDestination(Text destination) {
			this.destination = destination;
		}

		public Text getCrsArrTime() {
			return crsArrTime;
		}

		public void setCrsArrTime(Text crsArrTime) {
			this.crsArrTime = crsArrTime;
		}

		public Text getCrsDepTime() {
			return crsDepTime;
		}

		public void setCrsDepTime(Text crsDepTime) {
			this.crsDepTime = crsDepTime;
		}

		public Text getActualArrTime() {
			return actualArrTime;
		}

		public void setActualArrTime(Text actualArrTime) {
			this.actualArrTime = actualArrTime;
		}

		public Text getActualDepTime() {
			return actualDepTime;
		}

		public void setActualDepTime(Text actualDepTime) {
			this.actualDepTime = actualDepTime;
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
				String carDateKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + "-"
									+ FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE);
									// TODO: 04_edge:replace with year 
						
				Text origin = new Text(FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN));
				Text destination = new Text(FileRecord.getValueOf(fields, FileRecord.Field.DEST));
				
				Text crsArrTime = new Text(FileRecord.getValueOf(fields, FileRecord.Field.CRS_ARR_TIME));
				Text crsDepTime = new Text(FileRecord.getValueOf(fields, FileRecord.Field.CRS_DEP_TIME));
				Text actualArrTime = new Text(FileRecord.getValueOf(fields, FileRecord.Field.ARR_TIME));
				Text actualDepTime = new Text(FileRecord.getValueOf(fields, FileRecord.Field.DEP_TIME));
				
				// TODO: 04_edge: Calculate long time using system calendar and store in amv 
				
				AirlineMapperValue amv = new AirlineMapperValue(origin, destination, crsArrTime, crsDepTime, actualArrTime, actualDepTime); 
						
				context.write(new Text(carDateKey), amv);
			}
		}
	}
	
	public static class AirlineLinearRegressionReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
		
		HashMap<String, Long> carYearCountMap = new HashMap<String, Long>();
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			// Reducer will be called for EACH CARRIER on EACH DAY
			long missedConnectionCount = 0;
			
			//System.out.println("========================reducer================" + key.toString());
			
			String[] keyparts = key.toString().split("-");
			String carrier = keyparts[0];
			String year = keyparts[1];
			
			ArrayList<AirlineMapperValue> A_listOfAMVs = new ArrayList<AirlineMapperValue>();
			ArrayList<AirlineMapperValue> B_listOfAMVs = new ArrayList<AirlineMapperValue>();
			
			for(AirlineMapperValue amv : listOfAMVs){
				A_listOfAMVs.add(new AirlineMapperValue(amv));
				B_listOfAMVs.add(new AirlineMapperValue(amv));
			}
			
			for(AirlineMapperValue a_amv : A_listOfAMVs){
				for(AirlineMapperValue b_amv : B_listOfAMVs){
					if(missedConnection(a_amv, b_amv) || missedConnection(b_amv, a_amv)){
						missedConnectionCount += 1;
					}
				}
			}
			
			String carYearKey = carrier+"\t"+year;
			
			if(!carYearCountMap.containsKey(carYearKey)){
				carYearCountMap.put(carYearKey, new Long(0));
			}
			
			Long countSoFar = carYearCountMap.get(carYearKey) + missedConnectionCount;
			carYearCountMap.put(carYearKey, countSoFar);
		}

		@Override
		protected void cleanup(Reducer<Text, AirlineMapperValue, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(String carYearKey : carYearCountMap.keySet()){
				context.write(new Text(carYearKey), new Text(carYearCountMap.get(carYearKey) + ""));
			}
		}

		// Helper functions:
		
		private boolean missedConnection(AirlineMapperValue famv, AirlineMapperValue gamv) {
			if(isConnection(famv, gamv)){
				int actualTimeDiff = hhmmDiffInMins(famv.getActualArrTime().toString(), gamv.getActualDepTime().toString());
				if (actualTimeDiff < 30){
					return true;
				}
			}
			return false;
		}

		private boolean isConnection(AirlineMapperValue famv, AirlineMapperValue gamv) {
			if(famv.getDestination().toString().equals(gamv.getOrigin().toString())){
				int timeDiff = hhmmDiffInMins(gamv.getCrsDepTime().toString(), famv.getCrsArrTime().toString());
				if (timeDiff < 0){
					// G departure before F arrival, so cant be a connection
					return false;
				}
				
				if(timeDiff <= 360 && timeDiff >= 30){
					return true;
				}
			}
			return false;
		}

		// TODO: 04_edge: Change to handle time in java's long format
		private static int hhmmDiffInMins(String t1, String t2) {
			int t1HH = Integer.parseInt(t1.substring(0, 2));
			int t1MM = Integer.parseInt(t1.substring(2, 4));

			int t2HH = Integer.parseInt(t2.substring(0, 2));
			int t2MM = Integer.parseInt(t2.substring(2, 4));

			return (t1HH - t2HH) * 60 + (t1MM - t2MM);
		}
	}
	
	public static class AirlinePartitioner extends Partitioner<Text, AirlineMapperValue>{

		private static final String[] uc = {"9E", "AA", "AS", "B6", "DL", "EV", "F9", "FL", "HA", 
											"MQ", "NK", "OO", "UA", "US", "VX", "WN", "YV"}; 
		private static final ArrayList<String> UNIQUE_CARRIERS = new ArrayList<String>(Arrays.asList(uc));

		@Override
		public int getPartition(Text carKey, AirlineMapperValue amv, int numberOfReducers) {
			String[] keyparts = carKey.toString().split("-");
			return UNIQUE_CARRIERS.indexOf(keyparts[0]) % numberOfReducers;
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
		job.setJobName("MissedConnections");
		job.setJarByClass(MissedConnections.class);

		job.setMapperClass(AirlineMapper.class);
		job.setPartitionerClass(AirlinePartitioner.class);
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
