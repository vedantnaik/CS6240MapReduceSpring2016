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
 * Authors: Vedant Naik, Rohan Joshi
 * */
public class MissedConnections {

	/*									SOLUTION DESCRIPTION
	 * 									====================
	 * 
	 * A connection is any pair of flight F and G of the same carrier such as F.Destination = G.Origin 
	 * 
	 * 				flight F	--->	SomeAirport		---> flight G
	 * 													G leaves 30mins-6hours after F arrives
	 * 
	 * and the scheduled departure of G is <= 6 hours and >= 30 minutes after the scheduled arrival of F.
	 * 
	 * A connection is missed when the actual arrival of F < 30 minutes before the actual departure of G.
	 * 
	 * Solution:
	 * 
	 * Mapper:
	 * 	Key	:	Initially we wanted flights of the same carrier and same year
	 * 			so we proposed a key like this
	 * 			<CARRIER><YEAR>
	 * 			In order to get the flights that are candidates for "connections", we add the airport ID
	 * 			i.e. Origin and destination, to the key as well.			
	 * 		 	To achieve this, for each record in mapper, write two values to the context, one with origin
	 * 			and other with destination.
	 * 			- so now, in reducer, we will have list of flights grouped as
	 * 			<CARRIER> <YEAR> <AIRPORT> 
	 * 							origin/dest 
	 * 	
	 * Partitioner:		
	 * 	Since the choice of key structure in mapper will lead to a lot of keys, we could have a partitioner that 
	 * 	sends values of same carriers to the same Reducer. I.E. based on <CARRIER> alone.
	 * 
	 * AirlineMapperValue:	
	 * 			We will need the following from each flight record:
	 * 			ConnectionType: Which will mark this value as an ORIGIN or DESTINATION
	 * 			Scheduled arrival and departure times
	 * 			Actual arrival and departure times
	 * 			[NOTE: store the times in long, so that difference can be calculated for connections that span over
	 * 					days and months as mentioned in piazza post @77]
	 * 
	 * */
	
	public static final String CONN_TYPE_ORIGIN = "ORIGIN"; 
	public static final String CONN_TYPE_DESTIN = "DESTIN";
	
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
			return 
					"["
//					+"FlightType:" + connectionLink.toString() + " " 
					+ "crsArr:" + new Date(crsArrTime.get()).toString() + " "
					+ "crsArr long:" + new Date(crsArrTime.get()).getTime() + " "
					
//					+ "crsDep:" + new Date(crsDepTime.get()).toString() + " "
//					+ "crsDep long:" + new Date(crsDepTime.get()).getTime() + " "

//					+ "actualArr:" + new Date(actualArrTime.get()).toString() + " "
//					+ "actualDep:"+ new Date(actualDepTime.get()).toString() 

//					+ "actualArr:" + new Date(actualArrTime.get()).getTime() + " "
//					+ "actualDep:"+ new Date(actualDepTime.get()).getTime() 
					+"]";
		}

		
		public long getTimeForComparison(){
			if (this.connectionLink.toString().equals(CONN_TYPE_ORIGIN)){
				return this.getCrsDepTime().get();
			}
			return this.getCrsArrTime().get();
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
					// Store java date in long so that it will help in finding connections that span over days and months
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
				AirlineMapperValue amvDest = new AirlineMapperValue(new Text(CONN_TYPE_DESTIN), 
																	new LongWritable(crsArrTime), 
																	new LongWritable(crsDepTime), 
																	new LongWritable(actualArrTime), 
																	new LongWritable(actualDepTime)); 				
				String carDestKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + "\t" + 
									FileRecord.getValueOf(fields, FileRecord.Field.DEST) + "\t" + 
									year;
				context.write(new Text(carDestKey), amvDest);
				
				AirlineMapperValue amvOrig = new AirlineMapperValue(new Text(CONN_TYPE_ORIGIN), 
																	new LongWritable(crsArrTime), 
																	new LongWritable(crsDepTime), 
																	new LongWritable(actualArrTime), 
																	new LongWritable(actualDepTime)); 				
				String carOrigKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + "\t" + 
									FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN) + "\t" + 
									year;
				context.write(new Text(carOrigKey), amvOrig);
				
			}
		}
		
		// Author: Vedant Naik
		// inspired by: http://stackoverflow.com/questions/3056703/simpledateformat
		private long getJavaDateInLong(String year, String month, String day, String HHMM) throws ParseException{
			
			HHMM = FileRecord.makeCompleteHHMM(HHMM);
			
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss");
			
			String str1 = year + "-" + month + "-" + day + "T" + HHMM.substring(0, 2) + ":" + HHMM.substring(2, 4) +":00";
			Date date1 = sf.parse(str1);
			return date1.getTime();
		}
	}
	
	public static class AirlineMissedConnectionsReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
		
		// for these maps, Key should have CARRIER and YEAR
		HashMap<String, Double> carYearCountConnectionsMap = new HashMap<String, Double>();
		HashMap<String, Double> carYearCountMissedMap = new HashMap<String, Double>();
			
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			// Reduce will be called for EACH CARRIER and EACH YEAR and for flights with matching origin and destination.
			double connectionCount = 0;
			double missedCount = 0;
			
			ArrayList<AirlineMapperValue> finalSortedList = new ArrayList<AirlineMapperValue>();
			
			for(AirlineMapperValue amv : listOfAMVs){
				finalSortedList = addToFinalSortedList(finalSortedList, new AirlineMapperValue(amv));
			}

			
			for(int destIndex = 0; destIndex < finalSortedList.size(); destIndex++){

				AirlineMapperValue destAMV = finalSortedList.get(destIndex);
				
				if(destAMV.getConnectionLink().toString().equals(CONN_TYPE_DESTIN)){
					int origIndex = destIndex + 1;
					boolean yetToCover6hrs = true;
					
					while(origIndex < finalSortedList.size() && yetToCover6hrs){
						long timeDiff = longTimeDifferenceInMins(destAMV.getTimeForComparison(), finalSortedList.get(origIndex).getTimeForComparison());
						if(timeDiff > -360){
							if(finalSortedList.get(origIndex).getConnectionLink().toString().equals(CONN_TYPE_ORIGIN)){
								if (timeDiff >= -360 && timeDiff <= -30){
									// connections
									connectionCount = connectionCount + 1;
										
									if (longTimeDifferenceInMins(destAMV.getActualArrTime().get(), finalSortedList.get(origIndex).getActualDepTime().get()) > -30){
										missedCount = missedCount + 1;
									}						
								}
							}
						} else {
							yetToCover6hrs = false;
						}
						
						origIndex = origIndex + 1;
					}
				}
			}
			
//			System.out.println("1 " + finalSortedList.size() + " " + origIndex + " " + destIndex + " " +timeDiff);
			
			
//			for(int destIndex = 0; destIndex < Dest_listOfAMVs.size(); destIndex++){
//				
//				AirlineMapperValue destAmv = Dest_listOfAMVs.get(destIndex);
//				
//				long destCrsArr = destAmv.getCrsArrTime().get();
//				long destActualArr = destAmv.getActualArrTime().get();
//				
//				if(Orig_listOfAMVs.size() == 0){
//					continue;
//				}
//				
//				long timeDiff = longTimeDifferenceInMins(destCrsArr, Orig_listOfAMVs.get(0).getCrsDepTime().get());
//				
//				for(int origIndex = 0; origIndex < Orig_listOfAMVs.size() && timeDiff > -360; origIndex++){
//					if (timeDiff >= -360 && timeDiff <= -30){
//						// connections
//						connectionCount = connectionCount + 1;
//						
//						if (longTimeDifferenceInMins(destActualArr, Orig_listOfAMVs.get(origIndex).getActualDepTime().get()) > -30){
//							missedCount = missedCount + 1;
//						}						
//					}
//					
//					timeDiff = longTimeDifferenceInMins(destCrsArr, Orig_listOfAMVs.get(origIndex).getCrsDepTime().get());
//				}	
//			}
			
			String[] keyParts = key.toString().split("\t");
			// carrier = keyParts[0] | airport id = keyParts[1] | year = keyParts[2]
			String carYearKey = keyParts[0] + "\t" + keyParts[2]; 
			
			if (!carYearCountConnectionsMap.containsKey(carYearKey)){
				carYearCountConnectionsMap.put(carYearKey, new Double(0));
			}
			if (!carYearCountMissedMap.containsKey(carYearKey)){
				carYearCountMissedMap.put(carYearKey, new Double(0));
			}
			
			double connectionCountSoFar = carYearCountConnectionsMap.get(carYearKey);
			double missedCountSoFar = carYearCountMissedMap.get(carYearKey);
			
			connectionCountSoFar = connectionCountSoFar + connectionCount;
			missedCountSoFar = missedCountSoFar + missedCount;
			
			carYearCountConnectionsMap.put(carYearKey, connectionCountSoFar);
			carYearCountMissedMap.put(carYearKey, missedCountSoFar);
		}
		
		private int nextOriginObjectIndex(ArrayList<AirlineMapperValue> finalSortedList, int origIndex) {
			
//			System.out.println("Input " + finalSortedList.size() + " index " + origIndex);
			while(finalSortedList.size() > origIndex && finalSortedList.get(origIndex).getConnectionLink().toString().equals(CONN_TYPE_DESTIN)){
				origIndex = origIndex + 1;
			}
			
			if (origIndex == finalSortedList.size()) {return -1;}
//			System.out.print(" given " + origIndex);
			return origIndex;
		}

		@Override
		protected void cleanup(Reducer<Text, AirlineMapperValue, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(String carYearKey : carYearCountConnectionsMap.keySet()){
				
				double missedCount = carYearCountMissedMap.get(carYearKey);
				double connectionCount = carYearCountConnectionsMap.get(carYearKey);
				
				double percentMissed = (missedCount/connectionCount) * 100;
				
				System.out.println("FROM REDUCER===================" + carYearKey + "\t" + connectionCount + "\t" + missedCount + "\t " + percentMissed);
				context.write(new Text(carYearKey), new Text(connectionCount + "\t" + missedCount + "\t " + percentMissed));
			}
		}
		
		// Helper functions:
		
		private ArrayList<AirlineMapperValue> addToFinalSortedList(ArrayList<AirlineMapperValue> finalSortedList, AirlineMapperValue amv) {
			long valToCompare = amv.getTimeForComparison();
			if (finalSortedList.size() == 0) {
				finalSortedList.add(amv);
	        } else if (finalSortedList.get(0).getTimeForComparison() > valToCompare) {
	        	finalSortedList.add(0, amv);
	        } else if (finalSortedList.get(finalSortedList.size() - 1).getTimeForComparison() < valToCompare) {
	        	finalSortedList.add(finalSortedList.size(), amv);
	        } else {
	            int index = 0;
	            while (finalSortedList.get(index).getTimeForComparison() < valToCompare) {
	            	index++;
	            }
	            finalSortedList.add(index, amv);
	        }
			return finalSortedList;
		}
		
		// inspired by http://stackoverflow.com/questions/18144820/inserting-into-sorted-linkedlist-java
//		private ArrayList<AirlineMapperValue> addToSortedOrigin(ArrayList<AirlineMapperValue> Orig_listOfAMVs, AirlineMapperValue amv){
//			long valToCompare = amv.getCrsDepTime().get();
//			if (Orig_listOfAMVs.size() == 0) {
//				Orig_listOfAMVs.add(amv);
//	        } else if (Orig_listOfAMVs.get(0).getCrsDepTime().get() > valToCompare) {
//	        	Orig_listOfAMVs.add(0, amv);
//	        } else if (Orig_listOfAMVs.get(Orig_listOfAMVs.size() - 1).getCrsDepTime().get() < valToCompare) {
//	        	Orig_listOfAMVs.add(Orig_listOfAMVs.size(), amv);
//	        } else {
//	            int index = 0;
//	            while (Orig_listOfAMVs.get(index).getCrsDepTime().get() < valToCompare) {
//	            	index++;
//	            }
//	            Orig_listOfAMVs.add(index, amv);
//	        }
//			return Orig_listOfAMVs;
//		}
//
//
//		// inspired by http://stackoverflow.com/questions/18144820/inserting-into-sorted-linkedlist-java
//		private ArrayList<AirlineMapperValue> addToSortedDest(ArrayList<AirlineMapperValue> Dest_listOfAMVs, AirlineMapperValue amv){			
//			long valToCompare = amv.getCrsArrTime().get();
//			if (Dest_listOfAMVs.size() == 0) {
//				Dest_listOfAMVs.add(amv);
//			} else if (Dest_listOfAMVs.get(0).getCrsArrTime().get() > valToCompare) {
//	        	Dest_listOfAMVs.add(0, amv);
//	        } else if (Dest_listOfAMVs.get(Dest_listOfAMVs.size() - 1).getCrsArrTime().get() < valToCompare) {
//	        	Dest_listOfAMVs.add(Dest_listOfAMVs.size(), amv);
//	        } else {
//	            int index = 0;
//	            while (Dest_listOfAMVs.get(index).getCrsArrTime().get() < valToCompare) {
//	            	index++;
//	            }
//	            Dest_listOfAMVs.add(index, amv);
//	        }
//			return Dest_listOfAMVs;
//		}

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
		//job.setPartitionerClass(AirlinePartitioner.class);
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
		System.err.println("\t-pseudo\t\t:\turn on a pseudo cluster");
		System.err.println("\t-emr\t\t:\turn on amazon web services");
		
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
