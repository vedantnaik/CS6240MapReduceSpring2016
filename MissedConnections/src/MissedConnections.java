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
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * PERFORMANCE PATCH _05
 * =====================
 * 
 * change mapper key to have airport along with carrier and year
 * 	- so now, in reducer, we will have list of flights grouped as
 * 			<carrier> <year> <airport> 
 * 							origin/dest 
 * 		for each record, write two values to the context
 * 
 * this will ensure that flights with same carriers, year and matching origin and destination
 * 
 * in the reducer:
 * 	we take list of values, and check timings.
 * 
 * */



public class MissedConnections {

	/*					SOLUTION DESCRIPTION
	 * 					====================
	 * 
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
	 * 	Key	:	we want flights of the same carrier and same year
	 * 			so we propose a key like this	(hyphen separated)
	 * 			<CARRIER>-<YEAR>
	 * 
	 * Since this will lead to a lot of keys, we could have a partitioner that sends dates of same carriers 
	 * to the same Reducer. I.E. based on <CARRIER>
	 * 
	 * Value :	We will need the following from each flight record:
	 * 			DESTINATION
	 * 			ORIGIN
	 * 			Scheduled arrival and departure times
	 * 			Actual arrival and departure times
	 * 			[NOTE: store the times in long, so that difference can be calculated for connections that span over
	 * 					days and months]
	 * */
	
	public static class AirlineMapperValue implements Writable {
		Text connectionLink;	// ORIGIN or DESTINATION

		LongWritable crsArrTime;
		LongWritable crsDepTime;
		LongWritable actualArrTime;
		LongWritable actualDepTime;
		
		public AirlineMapperValue(){
			this.connectionLink = new Text();
			this.crsArrTime = new LongWritable();
			this.crsDepTime = new LongWritable();
			this.actualArrTime = new LongWritable();
			this.actualDepTime = new LongWritable();
		}
		
		public AirlineMapperValue(Text connectionLink, LongWritable crsArrTime, LongWritable crsDepTime,
				LongWritable actualArrTime, LongWritable actualDepTime) {
			this.connectionLink = connectionLink;
			this.crsArrTime = crsArrTime;
			this.crsDepTime = crsDepTime;
			this.actualArrTime = actualArrTime;
			this.actualDepTime = actualDepTime;
		}
		
		public AirlineMapperValue(AirlineMapperValue amv) {
			this.connectionLink = new Text(amv.getConnectionLink().toString());
			this.crsArrTime = new LongWritable(amv.getCrsArrTime().get());
			this.crsDepTime = new LongWritable(amv.getCrsDepTime().get());
			this.actualArrTime = new LongWritable(amv.getActualArrTime().get());
			this.actualDepTime = new LongWritable(amv.getActualDepTime().get());
		}
		
		@Override
		public void readFields(DataInput inVal) throws IOException {
			connectionLink.readFields(inVal);
			crsArrTime.readFields(inVal);
			crsDepTime.readFields(inVal);
			actualArrTime.readFields(inVal);
			actualDepTime.readFields(inVal);
		}

		@Override
		public void write(DataOutput outVal) throws IOException {
			connectionLink.write(outVal);
			crsArrTime.write(outVal);
			crsDepTime.write(outVal);
			actualArrTime.write(outVal);
			actualDepTime.write(outVal);
		}

		@Override
		public String toString() {
			return "[FlightType:" + connectionLink.toString() + " " 
					+ "crsArr:" + new Date(crsArrTime.get()).toString() + " "
					+ "crsDep:" + new Date(crsDepTime.get()).toString() + " "
					+ "actualArr:" + new Date(actualArrTime.get()).toString() + " "
					+ "actualDep:"+ new Date(actualDepTime.get()).toString() +"]";
		}

		public Text getConnectionLink() {
			return connectionLink;
		}

		public void setConnectionLink(Text connectionLink) {
			this.connectionLink = connectionLink;
		}

		public LongWritable getCrsArrTime() {
			return crsArrTime;
		}

		public void setCrsArrTime(LongWritable crsArrTime) {
			this.crsArrTime = crsArrTime;
		}

		public LongWritable getCrsDepTime() {
			return crsDepTime;
		}

		public void setCrsDepTime(LongWritable crsDepTime) {
			this.crsDepTime = crsDepTime;
		}

		public LongWritable getActualArrTime() {
			return actualArrTime;
		}

		public void setActualArrTime(LongWritable actualArrTime) {
			this.actualArrTime = actualArrTime;
		}

		public LongWritable getActualDepTime() {
			return actualDepTime;
		}

		public void setActualDepTime(LongWritable actualDepTime) {
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
				String[] fldate = FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE).split("-");

				String year = fldate[0];
				String month = fldate[1];
				String day = fldate[2];
						
				long crsArrTime; 	
				long crsDepTime; 
				long actualArrTime;
				long actualDepTime;
				
				try {
					// Store java date in long so that it will for for finding connections that span over days and months
					crsArrTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.CRS_ARR_TIME));
					crsDepTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.CRS_DEP_TIME));
					actualArrTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.ARR_TIME));
					actualDepTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.DEP_TIME));
				} catch (ParseException pe) {
					System.err.println("Unable to create long date for a sane record!!");
					return;
				} catch (StringIndexOutOfBoundsException siobe) {
					System.err.println("Some value missing in sane record, although handeled in sanity check!!");
					return;
				}
				
				// a flight can be a destination OR the origin of a connection
				AirlineMapperValue amvDest = new AirlineMapperValue(new Text("DESTINATION"), 
						new LongWritable(crsArrTime), new LongWritable(crsDepTime), 
						new LongWritable(actualArrTime), new LongWritable(actualDepTime)); 				
				String carDestKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + "\t" + 
														FileRecord.getValueOf(fields, FileRecord.Field.DEST) + "\t" + year;
				context.write(new Text(carDestKey), amvDest);
				
				AirlineMapperValue amvOrig = new AirlineMapperValue(new Text("ORIGIN"), 
						new LongWritable(crsArrTime), new LongWritable(crsDepTime), 
						new LongWritable(actualArrTime), new LongWritable(actualDepTime)); 				
				String carOrigKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + "\t" + 
														FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN) + "\t" + year;
				context.write(new Text(carOrigKey), amvOrig);
				
			}
		}
		
		private long getJavaDateInLong(String year, String month, String day, String HHMM) throws ParseException{
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss");
			
			String str1 = year + "-" + month + "-" + day + "T" + HHMM.substring(0, 2) + ":" + HHMM.substring(2, 4) +":00";
			Date date1 = sf.parse(str1);
			return date1.getTime();
		}
	}
	
	public static class AirlineMissedConnectionsReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
		
		// Key should have CARRIER and YEAR
		HashMap<String, Double> carYearCountConnectionsMap = new HashMap<String, Double>();
		HashMap<String, Double> carYearCountMissedMap = new HashMap<String, Double>();
				
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			// Reduce will be called for EACH CARRIER and EACH YEAR
			// AND
			// for flights with matching origin and destination.
			double connectionCount = 0;
			double missedCount = 0;
			
			ArrayList<AirlineMapperValue> Orig_listOfAMVs = new ArrayList<AirlineMapperValue>();
			ArrayList<AirlineMapperValue> Dest_listOfAMVs = new ArrayList<AirlineMapperValue>();
			
			for(AirlineMapperValue amv : listOfAMVs){
				if(amv.getConnectionLink().toString().equalsIgnoreCase("ORIGIN")){
					Orig_listOfAMVs.add(new AirlineMapperValue(amv));					
				} else {
					Dest_listOfAMVs.add(new AirlineMapperValue(amv));
				}
			}

			for (AirlineMapperValue g_amv : Orig_listOfAMVs){
				for(AirlineMapperValue f_amv : Dest_listOfAMVs){
					if(isConnection(f_amv, g_amv)){
						connectionCount = connectionCount + 1;
						if(missedConnection(f_amv, g_amv)){
							missedCount = missedCount + 1;
						}
					}
				}
			}
			
			String[] keyParts = key.toString().split("\t");
			// carrier = keyParts[0] 
			// airport id = keyParts[1]
			// year = keyParts[2]
			String carYearKey = keyParts[0] + "\t" + keyParts[2]; 
			
			if (!carYearCountConnectionsMap.containsKey(carYearKey)){
				carYearCountConnectionsMap.put(carYearKey, (double) 0);
			}
			if (!carYearCountMissedMap.containsKey(carYearKey)){
				carYearCountMissedMap.put(carYearKey, (double) 0);
			}
			
			double connectionCountSoFar = carYearCountConnectionsMap.get(carYearKey);
			double missedCountSoFar = carYearCountMissedMap.get(carYearKey);
			
			connectionCountSoFar = connectionCountSoFar + connectionCount;
			missedCountSoFar = missedCountSoFar + missedCount;
			
			carYearCountConnectionsMap.put(carYearKey, connectionCountSoFar);
			carYearCountMissedMap.put(carYearKey, missedCountSoFar);
		}

		
		@Override
		protected void cleanup(Reducer<Text, AirlineMapperValue, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			for(String carYearKey : carYearCountConnectionsMap.keySet()){
				
				double missedCount = carYearCountMissedMap.get(carYearKey);
				double connectionCount = carYearCountConnectionsMap.get(carYearKey);
				
				double percentMissed = (missedCount/connectionCount) * 100;
				
				context.write(new Text(carYearKey), new Text(missedCount + "\t " + percentMissed));
				
			}
			
		}
		

		// Helper functions:
		
		private boolean missedConnection(AirlineMapperValue famv, AirlineMapperValue gamv) {
			// flights sent to this method HAVE to be connecting
			long actualTimeDiff = longTimeDifferenceInMins(famv.getActualArrTime().get(), gamv.getActualDepTime().get());
			if (actualTimeDiff < 30){
				return true;
			}
			return false;
		}

		private boolean isConnection(AirlineMapperValue famv, AirlineMapperValue gamv) {
			long timeDiff = longTimeDifferenceInMins(gamv.getCrsDepTime().get(), famv.getCrsArrTime().get());
			if(timeDiff <= 360 && timeDiff >= 30){
				return true;
			}
			return false;
		}

		private static long longTimeDifferenceInMins(long t1, long t2) {
			return TimeUnit.MILLISECONDS.toMinutes(t1 - t2);
		}
	}
	
	public static class AirlinePartitioner extends Partitioner<Text, AirlineMapperValue>{

		private static final String[] uc = {"9E", "AA", "AS", "B6", "DL", "EV", "F9", "FL", "HA", 
											"MQ", "NK", "OO", "UA", "US", "VX", "WN", "YV"}; 
		private static final ArrayList<String> UNIQUE_CARRIERS = new ArrayList<String>(Arrays.asList(uc));

		@Override
		public int getPartition(Text carKey, AirlineMapperValue amv, int numberOfReducers) {
			String[] keyparts = carKey.toString().split("\t");
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
		job.setReducerClass(AirlineMissedConnectionsReducer.class);
		
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
