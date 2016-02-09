import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LinearRegression {

	public static class FileRecord{
		class Field {
			static final String CRS_ARR_TIME = "CRS_ARR_TIME";
			static final String CRS_DEP_TIME = "CRS_DEP_TIME";
			static final String CRS_ELAPSED_TIME = "CRS_ELAPSED_TIME";

			static final String ORIGIN_AIRPORT_ID = "ORIGIN_AIRPORT_ID";
			static final String ORIGIN_AIRPORT_SEQ_ID = "ORIGIN_AIRPORT_SEQ_ID";
			static final String ORIGIN_CITY_MARKET_ID = "ORIGIN_CITY_MARKET_ID";
			static final String ORIGIN_STATE_FIPS = "ORIGIN_STATE_FIPS";
			static final String ORIGIN_WAC = "ORIGIN_WAC";
			static final String DEST_AIRPORT_ID = "DEST_AIRPORT_ID";
			static final String DEST_AIRPORT_SEQ_ID = "DEST_AIRPORT_SEQ_ID";
			static final String DEST_CITY_MARKET_ID = "DEST_CITY_MARKET_ID";
			static final String DEST_STATE_FIPS = "DEST_STATE_FIPS";
			static final String DEST_WAC = "DEST_WAC";

			static final String ORIGIN = "ORIGIN";
			static final String ORIGIN_CITY_NAME = "ORIGIN_CITY_NAME";
			static final String ORIGIN_STATE_ABR = "ORIGIN_STATE_ABR";
			static final String ORIGIN_STATE_NM = "ORIGIN_STATE_NM";
			static final String DEST = "DEST";
			static final String DEST_CITY_NAME = "DEST_CITY_NAME";
			static final String DEST_STATE_ABR = "DEST_STATE_ABR";
			static final String DEST_STATE_NM = "DEST_STATE_NM";

			static final String CANCELLED = "CANCELLED";

			static final String ARR_TIME = "ARR_TIME";
			static final String DEP_TIME = "DEP_TIME";
			static final String ACTUAL_ELAPSED_TIME = "ACTUAL_ELAPSED_TIME";

			static final String ARR_DELAY = "ARR_DELAY";
			static final String ARR_DELAY_NEW = "ARR_DELAY_NEW";
			static final String ARR_DEL15 = "ARR_DEL15";

			static final String AVG_TICKET_PRICE = "AVG_TICKET_PRICE";

			static final String FL_DATE = "FL_DATE";
			static final String CARRIER = "CARRIER";
			
			static final String YEAR = "YEAR";
			static final String MONTH = "MONTH";
		}
		
		@SuppressWarnings("serial")
		public static final ArrayList<String> csvHeaders = new ArrayList<String>() {
		{add("YEAR");
		add("QUARTER");
		add("MONTH");
		add("DAY_OF_MONTH");
		add("DAY_OF_WEEK");
		add("FL_DATE");
		add("UNIQUE_CARRIER");
		add("AIRLINE_ID");
		add("CARRIER");
		add("TAIL_NUM");
		add("FL_NUM");
		add("ORIGIN_AIRPORT_ID");
		add("ORIGIN_AIRPORT_SEQ_ID");
		add("ORIGIN_CITY_MARKET_ID");
		add("ORIGIN");
		add("ORIGIN_CITY_NAME");
		add("ORIGIN_STATE_ABR");
		add("ORIGIN_STATE_FIPS");
		add("ORIGIN_STATE_NM");
		add("ORIGIN_WAC");
		add("DEST_AIRPORT_ID");
		add("DEST_AIRPORT_SEQ_ID");
		add("DEST_CITY_MARKET_ID");
		add("DEST");
		add("DEST_CITY_NAME");
		add("DEST_STATE_ABR");
		add("DEST_STATE_FIPS");
		add("DEST_STATE_NM");
		add("DEST_WAC");
		add("CRS_DEP_TIME");
		add("DEP_TIME");
		add("DEP_DELAY");
		add("DEP_DELAY_NEW");
		add("DEP_DEL15");
		add("DEP_DELAY_GROUP");
		add("DEP_TIME_BLK");
		add("TAXI_OUT");
		add("WHEELS_OFF");
		add("WHEELS_ON");
		add("TAXI_IN");
		add("CRS_ARR_TIME");
		add("ARR_TIME");
		add("ARR_DELAY");
		add("ARR_DELAY_NEW");
		add("ARR_DEL15");
		add("ARR_DELAY_GROUP");
		add("ARR_TIME_BLK");
		add("CANCELLED");
		add("CANCELLATION_CODE");
		add("DIVERTED");
		add("CRS_ELAPSED_TIME");
		add("ACTUAL_ELAPSED_TIME");
		add("AIR_TIME");
		add("FLIGHTS");
		add("DISTANCE");
		add("DISTANCE_GROUP");
		add("CARRIER_DELAY");
		add("WEATHER_DELAY");
		add("NAS_DELAY");
		add("SECURITY_DELAY");
		add("LATE_AIRCRAFT_DELAY");
		add("FIRST_DEP_TIME");
		add("TOTAL_ADD_GTIME");
		add("LONGEST_ADD_GTIME");
		add("DIV_AIRPORT_LANDINGS");
		add("DIV_REACHED_DEST");
		add("DIV_ACTUAL_ELAPSED_TIME");
		add("DIV_ARR_DELAY");
		add("DIV_DISTANCE");
		add("DIV1_AIRPORT");
		add("DIV1_AIRPORT_ID");
		add("DIV1_AIRPORT_SEQ_ID");
		add("DIV1_WHEELS_ON");
		add("DIV1_TOTAL_GTIME");
		add("DIV1_LONGEST_GTIME");
		add("DIV1_WHEELS_OFF");
		add("DIV1_TAIL_NUM");
		add("DIV2_AIRPORT");
		add("DIV2_AIRPORT_ID");
		add("DIV2_AIRPORT_SEQ_ID");
		add("DIV2_WHEELS_ON");
		add("DIV2_TOTAL_GTIME");
		add("DIV2_LONGEST_GTIME");
		add("DIV2_WHEELS_OFF");
		add("DIV2_TAIL_NUM");
		add("DIV3_AIRPORT");
		add("DIV3_AIRPORT_ID");
		add("DIV3_AIRPORT_SEQ_ID");
		add("DIV3_WHEELS_ON");
		add("DIV3_TOTAL_GTIME");
		add("DIV3_LONGEST_GTIME");
		add("DIV3_WHEELS_OFF");
		add("DIV3_TAIL_NUM");
		add("DIV4_AIRPORT");
		add("DIV4_AIRPORT_ID");
		add("DIV4_AIRPORT_SEQ_ID");
		add("DIV4_WHEELS_ON");
		add("DIV4_TOTAL_GTIME");
		add("DIV4_LONGEST_GTIME");
		add("DIV4_WHEELS_OFF");
		add("DIV4_TAIL_NUM");
		add("DIV5_AIRPORT");
		add("DIV5_AIRPORT_ID");
		add("DIV5_AIRPORT_SEQ_ID");
		add("DIV5_WHEELS_ON");
		add("DIV5_TOTAL_GTIME");
		add("DIV5_LONGEST_GTIME");
		add("DIV5_WHEELS_OFF");
		add("DIV5_TAIL_NUM");
		add("AVG_TICKET_PRICE");}};
		
		private static String getValueOf(String[] fields, String headerName){
			return fields[csvHeaders.indexOf(headerName)];
		}
		
		private static boolean isRecordValid(String[] fields) {

			float timeZone = 0;

			// CRSArrTime and CRSDepTime should not be zero
			// timeZone % 60 should be 0

			SimpleDateFormat format = new SimpleDateFormat("HHmm");

			String crsArrTime = fields[csvHeaders.indexOf(Field.CRS_ARR_TIME)];
			String crsDepTime = fields[csvHeaders.indexOf(Field.CRS_DEP_TIME)];
			String crsElapsedTime = fields[csvHeaders.indexOf(Field.CRS_ELAPSED_TIME)];

			try {
				Date CRSArrTime = (crsArrTime.equals("") ? null : format.parse(crsArrTime));
				Date CRSDepTime = (crsDepTime.equals("") ? null : format.parse(crsDepTime));
				float CRSElapsedTime = Float.parseFloat(crsElapsedTime);

				float crsDiff = hhmmDiff(crsArrTime, crsDepTime);

				timeZone = crsDiff - CRSElapsedTime;

				if (CRSArrTime.getTime() == 0.0 || CRSDepTime.getTime() == 0.0)
					return false;
				if ((timeZone % 60) != 0)
					return false;

			} catch (NumberFormatException e) {
				return false;
			} catch (ParseException e) {
				return false;
			}

			// AirportID, AirportSeqID, CityMarketID, StateFips, Wac should be
			// larger than 0

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_AIRPORT_ID)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_AIRPORT_SEQ_ID)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_CITY_MARKET_ID)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_STATE_FIPS)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.ORIGIN_WAC)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_AIRPORT_ID)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_AIRPORT_SEQ_ID)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_CITY_MARKET_ID)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_STATE_FIPS)]) <= 0)
				return false;

			if (Integer.parseInt(fields[csvHeaders.indexOf(Field.DEST_WAC)]) <= 0)
				return false;

			// Origin, Destination, CityName, State, StateName should not be empty

			if (fields[csvHeaders.indexOf(Field.ORIGIN)].equals(""))
				return false;

			if (fields[csvHeaders.indexOf(Field.ORIGIN_CITY_NAME)].equals(""))
				return false;

			if (fields[csvHeaders.indexOf(Field.ORIGIN_STATE_ABR)].equals(""))
				return false;

			if (fields[csvHeaders.indexOf(Field.ORIGIN_STATE_NM)].equals(""))
				return false;

			if (fields[csvHeaders.indexOf(Field.DEST)].equals(""))
				return false;

			if (fields[csvHeaders.indexOf(Field.DEST_CITY_NAME)].equals(""))
				return false;

			if (fields[csvHeaders.indexOf(Field.DEST_STATE_ABR)].equals(""))
				return false;

			if (fields[csvHeaders.indexOf(Field.DEST_STATE_NM)].equals(""))
				return false;

			// For flights that are not Cancelled:
			int cancelledDigit = 0;
			try{
				cancelledDigit = Integer.parseInt(fields[csvHeaders.indexOf(Field.CANCELLED)]);
			} catch (NumberFormatException e) {
				cancelledDigit = 0;
			}

			if (cancelledDigit != 1) {
				String arrTime = fields[csvHeaders.indexOf(Field.ARR_TIME)];
				String depTime = fields[csvHeaders.indexOf(Field.DEP_TIME)];
				String actElapsedTime = fields[csvHeaders.indexOf(Field.ACTUAL_ELAPSED_TIME)];
				
				try {
					long actualElapsedTime = Long.parseLong(actElapsedTime);
					long actualDiff = hhmmDiff(arrTime, depTime);

					long actualTimeZone = actualDiff - actualElapsedTime;

					long CRSElapsedTime = Long.parseLong(crsElapsedTime);
					long crsDiff = hhmmDiff(crsArrTime, crsDepTime);
					long newtimeZone = crsDiff - CRSElapsedTime;

					if (actualTimeZone != newtimeZone) {
						return false;
					}

					// if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
					// if ArrDelay < 0 then ArrDelayMinutes should be zero
					// if ArrDelayMinutes >= 15 then ArrDel15 should be false
					float arrDelay = Float.parseFloat(fields[csvHeaders.indexOf(Field.ARR_DELAY)]);

					float ArrDelayMinutes = Float.parseFloat(fields[csvHeaders.indexOf(Field.ARR_DELAY_NEW)]);

					float arrDel15 = Float.parseFloat(fields[csvHeaders.indexOf(Field.ARR_DEL15)]);

					if (arrDelay > 0.0) {
						if (arrDelay != ArrDelayMinutes) {
							return false;
						}
					}

					if (arrDelay < 0.0) {
						if (ArrDelayMinutes != 0) {
							return false;
						}
					}

					if (ArrDelayMinutes > 15.0) {
						if (arrDel15 != 1) {
							return false;
						}
					}

				} catch (NumberFormatException e) {
					// no entry found in record
					// format does not match
					return false;
				}
			}
			// Given sanity checks complete

			// Additional validations
			if (Float.parseFloat(fields[csvHeaders.indexOf(Field.AVG_TICKET_PRICE)]) > 999999)
				return false;

			return true;
		}

		private static int hhmmDiff(String arr, String dep) {
			int arrHH = Integer.parseInt(arr.substring(0, 2));
			int arrMM = Integer.parseInt(arr.substring(2, 4));

			int depHH = Integer.parseInt(dep.substring(0, 2));
			int depMM = Integer.parseInt(dep.substring(2, 4));

			if (Integer.parseInt(arr) > Integer.parseInt(dep)) {
				return (arrHH - depHH) * 60 + (arrMM - depMM);
			} else {
				// Cross over 24hr
				return (arrHH - depHH + 24) * 60 + (arrMM - depMM);
			}
		}
	} 
	
	public static class AirlineMapperValue implements Writable {
		DoubleWritable flightPrice;
		IntWritable flightMonth;
		BooleanWritable flightYearIs2015;

		public AirlineMapperValue(DoubleWritable flightPrice, BooleanWritable flightYearIs2015, IntWritable flightMonth) {
			this.flightPrice = flightPrice;
			this.flightYearIs2015 = flightYearIs2015;
			this.flightMonth = flightMonth;
		}

		public AirlineMapperValue() {
			flightPrice = new DoubleWritable();
			flightYearIs2015 = new BooleanWritable();
			flightMonth = new IntWritable();
		}

		void set(DoubleWritable flightPrice, BooleanWritable flightYearIs2015, IntWritable flightMonth) {
			this.flightPrice = flightPrice;
			this.flightYearIs2015 = flightYearIs2015;
			this.flightMonth = flightMonth;
		}
		
		void setFromJava(double flightPrice, boolean flightYearIs2015, int flightMonth) {
			this.flightPrice = new DoubleWritable(new Double(flightPrice));
			this.flightYearIs2015 = new BooleanWritable(new Boolean(flightYearIs2015));
			this.flightMonth = new IntWritable(new Integer(flightMonth));
		}
		
		void combineFromAnother(AirlineMapperValue otheramv){
			setFromJava(this.flightPrice.get() + otheramv.getFlightPrice().get(),
						otheramv.getFlightYearIs2015().get() || this.flightYearIs2015.get(),
						otheramv.getFlightMonth().get());
		} 
		
		@Override
		public void readFields(DataInput arg0) throws IOException {
			flightPrice.readFields(arg0);
			flightYearIs2015.readFields(arg0);
			flightMonth.readFields(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			flightPrice.write(arg0);
			flightYearIs2015.write(arg0);
			flightMonth.write(arg0);
		}

		@Override
		public String toString() {
			return "[" + flightPrice.get() + " " + (flightYearIs2015.get() ? "TRUE" : "FALSE") + " " + flightMonth.get() + "]";
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

		public void setFlightYear(BooleanWritable flightYearIs2015) {
			this.flightYearIs2015 = flightYearIs2015;
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
		
				double flightPrice = Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.AVG_TICKET_PRICE));
				boolean flightYearIs2015 = FileRecord.getValueOf(fields, FileRecord.Field.YEAR).equalsIgnoreCase("2015");
				int flightMonth = Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.MONTH));
				
				amv.setFromJava(flightPrice, flightYearIs2015, flightMonth);
				
				context.write(new Text(carMonthKey), amv);
			}
		}
	}
	
	public static class AirlineMedianReducer extends Reducer<Text, AirlineMapperValue, Text, DoubleWritable> {
		
		private HashMap<String, List<Double>> appendedMapperOutput = new HashMap<String, List<Double>>();
		private HashMap<String, Boolean> activeCarriersMap = new HashMap<String, Boolean>();
		private HashMap<String, Integer> flightCountMap = new HashMap<String, Integer>();
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			List<Double> mapperList = new ArrayList<Double>();
			
			boolean is2015 = false;
			
			for (AirlineMapperValue eachAMV : listOfAMVs){
				mapperList.add(eachAMV.getFlightPrice().get());
				is2015 = eachAMV.getFlightYearIs2015().get() || is2015;
			}
			
			appendedMapperOutput.put(key.toString(), mapperList);
			activeCarriersMap.put(key.toString(), new Boolean(is2015));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
						
			TreeMap<String, List<Double>> sortedAppendedMapperOutput = new TreeMap<String, List<Double>>();
			for(String carMonthKey : appendedMapperOutput.keySet()){
				sortedAppendedMapperOutput.put(carMonthKey, appendedMapperOutput.get(carMonthKey));
				flightCountMap.put(carMonthKey, new Integer(appendedMapperOutput.get(carMonthKey).size()));
			}
			
			ArrayList<String> top10Cars = listOfTop10Cars();
			
			for(String carMonthKey : sortedAppendedMapperOutput.keySet()){
				List<Double> listOfPrices = sortedAppendedMapperOutput.get(carMonthKey);
				
				Collections.sort(listOfPrices);
				
				Double fastMedian = getMedian(listOfPrices);
				
				String[] keySplit = carMonthKey.split(",");
				String flcarrier = keySplit[0];
				String flmonth = keySplit[1];
				
				String customKey = flmonth +"\t"+ flcarrier;
				
				if(top10Cars.contains(flcarrier)){
					context.write(new Text(customKey),new DoubleWritable(fastMedian));
				}
			}
		}

		private Double getMedian(List<Double> listOfPrices) {
			int len = listOfPrices.size();
			int middle = len / 2;
			if (len % 2 == 1) {
				return listOfPrices.get(middle);
			} else {
				return (listOfPrices.get(middle - 1) + listOfPrices.get(middle)) / 2;
			}
		}
		
		private ArrayList<String> listOfTop10Cars() {
			HashMap<String, Integer> carCountMap = new HashMap<String, Integer>();
			
			for(String carMonthKey : flightCountMap.keySet()){
				String[] keySplit = carMonthKey.split(",");
				String car = keySplit[0];
				
				if(!carCountMap.containsKey(car)){
					carCountMap.put(car, 0);
				}
				
				int count = carCountMap.get(car);
				count += flightCountMap.get(carMonthKey);
				carCountMap.put(car, count);
			}
			
			ArrayList<Entry<String, Integer>> sortedCarCountList = new ArrayList<Entry<String, Integer>>(carCountMap.entrySet());
			Collections.sort(sortedCarCountList, new Comparator<Map.Entry<String, Integer>>() {
				public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
					return -(o2.getValue()).compareTo(o1.getValue());
				}
			});
			
			int t = 0;
			ArrayList<String> listOfTop10 = new ArrayList<String>();
			for(Entry<String, Integer> e : sortedCarCountList){
				if(t == 10) break;
				listOfTop10.add(e.getKey());
				t += 1;
			}
			
			return listOfTop10;
		}
	}
	
	public static class AirlineFastMedianReducer extends Reducer<Text, AirlineMapperValue, Text, DoubleWritable> {
		
		private HashMap<String, List<Double>> appendedMapperOutput = new HashMap<String, List<Double>>();
		private HashMap<String, Boolean> activeCarriersMap = new HashMap<String, Boolean>();
		private HashMap<String, Integer> flightCountMap = new HashMap<String, Integer>();
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			List<Double> mapperList = new ArrayList<Double>();
			
			boolean is2015 = false;
			
			for (AirlineMapperValue eachAMV : listOfAMVs){
				mapperList.add(eachAMV.getFlightPrice().get());
				is2015 = eachAMV.getFlightYearIs2015().get() || is2015;
			}
			
			appendedMapperOutput.put(key.toString(), mapperList);
			activeCarriersMap.put(key.toString(), new Boolean(is2015));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
						
			TreeMap<String, List<Double>> sortedAppendedMapperOutput = new TreeMap<String, List<Double>>();
			for(String carMonthKey : appendedMapperOutput.keySet()){
				sortedAppendedMapperOutput.put(carMonthKey, appendedMapperOutput.get(carMonthKey));
				flightCountMap.put(carMonthKey, new Integer(appendedMapperOutput.get(carMonthKey).size()));
			}
			
			ArrayList<String> top10Cars = listOfTop10Cars();
			
			for(String carMonthKey : sortedAppendedMapperOutput.keySet()){
				List<Double> listOfPrices = sortedAppendedMapperOutput.get(carMonthKey);
				
				Double fastMedian = QuickSelect.quickSelect(listOfPrices, listOfPrices.size() / 2);
				
				String[] keySplit = carMonthKey.split(",");
				String flcarrier = keySplit[0];
				String flmonth = keySplit[1];
				
				String customKey = flmonth +"\t"+ flcarrier;
				
				if(top10Cars.contains(flcarrier)){
					context.write(new Text(customKey),new DoubleWritable(fastMedian));
				}
			}
		}
		
		private ArrayList<String> listOfTop10Cars() {
			HashMap<String, Integer> carCountMap = new HashMap<String, Integer>();
			
			for(String carMonthKey : flightCountMap.keySet()){
				String[] keySplit = carMonthKey.split(",");
				String car = keySplit[0];
				
				if(!carCountMap.containsKey(car)){
					carCountMap.put(car, 0);
				}
				
				int count = carCountMap.get(car);
				count += flightCountMap.get(carMonthKey);
				carCountMap.put(car, count);
			}
			
			ArrayList<Entry<String, Integer>> sortedCarCountList = new ArrayList<Entry<String, Integer>>(carCountMap.entrySet());
			Collections.sort(sortedCarCountList, new Comparator<Map.Entry<String, Integer>>() {
				public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
					return -(o2.getValue()).compareTo(o1.getValue());
				}
			});
			
			int t = 0;
			ArrayList<String> listOfTop10 = new ArrayList<String>();
			for(Entry<String, Integer> e : sortedCarCountList){
				if(t == 10) break;
				listOfTop10.add(e.getKey());
				t += 1;
			}
			
			return listOfTop10;
		}
		
		/*
		 * QuickSelect class
		 * For sorting lists:
		 * Referred from: Cory Hardman's Blog
		 * 
		 * http://www.coryhardman.com/2011/03/finding-median-in-almost-linear-time.html
		 */
		public static class QuickSelect {
		    public static Double quickSelect(List <Double> values, int k)
		    {
		        int left = 0;
		        int right = values.size() - 1;
		        Random rand = new Random();
		        while(true)
		        {
		            int partionIndex = rand.nextInt(right - left + 1) + left;
		            int newIndex = partition(values, left, right, partionIndex);
		            int q = newIndex - left + 1;
		            if(k == q)
		            {
		                return values.get(newIndex);
		            }
		            else if(k < q)
		            {
		                right = newIndex - 1;
		            }
		            else
		            {
		                k -= q;
		                left = newIndex + 1;
		            }
		        }
		    }
		    private static int partition(List <Double> values, int left, int right, int partitionIndex)
		    {
		        Double partionValue = values.get(partitionIndex);
		        int newIndex = left;
		        Double temp = values.get(partitionIndex);
		        values.set(partitionIndex, values.get(right));
		        values.set(right, temp);
		        for(int i = left; i < right; i++)
		        {
		            if(values.get(i).compareTo(partionValue) < 0)
		            {
		                temp = values.get(i);
		                values.set(i, values.get(newIndex));
		                values.set(newIndex, temp);
		                newIndex++;
		            }
		        }
		        temp = values.get(right);
		        values.set(right, values.get(newIndex));
		        values.set(newIndex, temp);
		        return newIndex;
		    }
		}
		
	}
		
	public static class AirlineMeanReducer extends Reducer<Text, AirlineMapperValue, Text, DoubleWritable> {
		
		private HashMap<String, AirlineMapperValue> addedMapperOutput = new HashMap<String, AirlineMapperValue>();
		private HashMap<String, Integer> flightCountMap = new HashMap<String, Integer>();
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
			AirlineMapperValue amvForAll = new AirlineMapperValue();
			amvForAll.setFromJava(0.0, false, 0);
			
			int count = 0;
			
			for (AirlineMapperValue eachAMV : listOfAMVs){
				amvForAll.combineFromAnother(eachAMV);
				count += 1;
			}
			
			addedMapperOutput.put(key.toString(), amvForAll);
			flightCountMap.put(key.toString(), new Integer(count));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
						
			TreeMap<String, AirlineMapperValue> sortedAddedMapperOutput = new TreeMap<String, AirlineMapperValue>();
			for(String carMonthKey : addedMapperOutput.keySet()){
				sortedAddedMapperOutput.put(carMonthKey, addedMapperOutput.get(carMonthKey));
			}
			
			ArrayList<String> top10Cars = listOfTop10Cars();
			
			for(String carMonthKey : sortedAddedMapperOutput.keySet()){
				AirlineMapperValue amv = sortedAddedMapperOutput.get(carMonthKey);
				double flightPrice = amv.getFlightPrice().get();
				int flightCount = flightCountMap.get(carMonthKey).intValue();

				double avgPrice = flightPrice/flightCount;
				
				String[] keySplit = carMonthKey.split(",");
				String flcarrier = keySplit[0];
				String flmonth = keySplit[1];
				
				String customKey = flmonth +"\t"+ flcarrier;
				
				if(top10Cars.contains(flcarrier)){
					context.write(new Text(customKey), new DoubleWritable(avgPrice));
				}
			}
		}

		private ArrayList<String> listOfTop10Cars() {
			HashMap<String, Integer> carCountMap = new HashMap<String, Integer>();
			
			for(String carMonthKey : flightCountMap.keySet()){
				String[] keySplit = carMonthKey.split(",");
				String car = keySplit[0];
				
				if(!carCountMap.containsKey(car)){
					carCountMap.put(car, 0);
				}
				
				int count = carCountMap.get(car);
				count += flightCountMap.get(carMonthKey);
				carCountMap.put(car, count);
			}
			
			ArrayList<Entry<String, Integer>> sortedCarCountList = new ArrayList<Entry<String, Integer>>(carCountMap.entrySet());
			Collections.sort(sortedCarCountList, new Comparator<Map.Entry<String, Integer>>() {
				public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
					return -(o2.getValue()).compareTo(o1.getValue());
				}
			});
			
			int t = 0;
			ArrayList<String> listOfTop10 = new ArrayList<String>();
			for(Entry<String, Integer> e : sortedCarCountList){
				if(t == 10) break;
				listOfTop10.add(e.getKey());
				t += 1;
			}
			
			return listOfTop10;
		}
	}
	
	public static void main(String[] args) throws Exception {

		if(args.length != 4){
			displayUsageAndExit();
		}
		
		String valueType = args[0];
		String runType = args[1];
		String inputPath = args[2];
		String outputPath = args[3];
		
		if(!(areParamsValid(valueType, runType))){
			displayUsageAndExit();
		}
		
		if (runType.equalsIgnoreCase("-s") || runType.equalsIgnoreCase("-mt")){
			long startTime = System.currentTimeMillis();

//			MultiThreadComparison.main(args);
			
			String outputDest = "timeOutput/allTimes.csv";
			printEndTime(startTime, valueType, runType, outputDest);
		} else {
			long startTime = System.currentTimeMillis();

			Configuration conf = new Configuration();
			
			Job job = Job.getInstance(conf);
			job.setJobName("Comparisons");
			job.setJarByClass(LinearRegression.class);

			job.setMapperClass(AirlineMapper.class);
			
			switch(valueType){
			case "-mean": 
				job.setReducerClass(AirlineMeanReducer.class);
				break;
			case "-median": 
				job.setReducerClass(AirlineMedianReducer.class);
				break;
			case "-fastMedian": 
				job.setReducerClass(AirlineFastMedianReducer.class);
				break;
			}

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(AirlineMapperValue.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);			
			
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			// Wait for the MapReduce job to complete before exiting application
			if(runType.equals("-pseudo")){
				if(job.waitForCompletion(true)){
					printEndTime(startTime, valueType, runType, System.getenv("HADOOP_HOME")+"/pseudo"+valueType.substring(1)+"Time.csv");
					System.exit(0);
				}
				System.exit(1);
			}
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

	private static void printEndTime(long startTime, String valueType, String runType, String outputDest) {
		long endTime = System.currentTimeMillis();
		long totalTime = (endTime - startTime) / 1000;
		System.out.println("\nRun type: " + runType.substring(1) + " Value type: " + valueType.substring(1) + " took " + totalTime + " secs");
		
		String lineToWrite = runType.substring(1) + "," + valueType.substring(1) + "," + totalTime;
		
		FileWriter writer;
		try {
			writer = new FileWriter(outputDest, true);
			writer.append(lineToWrite+"\n");
			writer.close();
		} catch (FileNotFoundException e) {
			System.err.println("Unable to write in timeOutput [file not found] " + valueType + " " + runType);
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.err.println("Unable to write in timeOutput [unsupported encoding] " + valueType + " " + runType);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void displayUsageAndExit() {
		System.err.println("Invalid inputs given.");
		System.err.println("USAGE:");
		System.err.println("\narg 0:");
		System.err.println("\t-mean\t\t:\tcalculate monthly mean");
		System.err.println("\t-median\t\t:\tcalculate monthly median");
		System.err.println("\t-fastMedain\t:\tcalculate monthly fast mean");
		
		System.err.println("\narg 1:");
		System.err.println("\t-s\t\t:\trun in serial");
		System.err.println("\t-mt\t\t:\trun in multi threaded");
		System.err.println("\t-pseudo\t\t:\trun on a pseudo cluster");
		System.err.println("\t-emr\t\t:\trun on amazon web services");
		
		System.err.println("\narg 2:");
		System.err.println("\t<Path to input>");
		
		System.err.println("\narg 3:");
		System.err.println("\t<Path to output>");
		
		System.exit(-1);
	}

	private static boolean areParamsValid(String valueType, String runType) {
		return ((valueType.equalsIgnoreCase("-mean") || valueType.equalsIgnoreCase("-median") || valueType.equalsIgnoreCase("-fastMedian")) 
				&& 
				(runType.equalsIgnoreCase("-s") || runType.equalsIgnoreCase("-mt") || runType.equalsIgnoreCase("-pseudo") || runType.equalsIgnoreCase("-emr")));
	}
}
