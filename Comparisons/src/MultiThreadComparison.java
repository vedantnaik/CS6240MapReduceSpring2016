import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
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
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

public class MultiThreadComparison {
	private static long validCount = 0; // values of F
	private static long invalidCount = 0; // value of K
	private static HashMap<String, ArrayList<Float>> carMonthPricesMap = new HashMap<String, ArrayList<Float>>();
	// stores a list of all prices from the data files, for each carrier,month
	private static HashSet<String> activeIn15 = new HashSet<String>();
	// stores a set of all carriers active in the year 2015

	private static HashMap<String, Float> valueMap = new HashMap<String, Float>();
	// stores value of prices for a carrier month key
	
	public static void main(String[] args) throws InterruptedException {
		
		String valueType = args[0];
		String runType = args[1];
		String inputPath = args[2];
		String outputPath = args[3];
		
		if (runType.equals("-mt")) {
			parallelMode(valueType, inputPath);
		} else if (runType.equals("-s")) {
			sequentialMode(valueType, inputPath);
		}
		
		postProcessingCalculations(valueType);
		ArrayList<String> top10Cars = listOfTop10Carriers();
		displayOutput(valueType, runType, outputPath, top10Cars);
	}

	/*
	 * Runs the program in sequential mode.
	 */
	private static void sequentialMode(String valueType, String DIR) throws InterruptedException {
		File dataFolder = new File(DIR);

		for (File fileEntry : dataFolder.listFiles()) {
			CSVGZProcessorThread thisFile = new CSVGZProcessorThread(fileEntry.toString());
			thisFile.start();
			thisFile.join();
			// since we don't start another thread till current one dies, we
			// ensure the
			// files are processed sequentially
		}
	}

	/*
	 * Runs the program in parallel mode.
	 */
	private static void parallelMode(String valueType, String DIR) throws InterruptedException {
		File dataFolder = new File(DIR);

		HashMap<String, CSVGZProcessorThread> fileThreadsMap = new HashMap<String, CSVGZProcessorThread>();
		for (File fileEntry : dataFolder.listFiles()) {
			fileThreadsMap.put(fileEntry.toString(), new CSVGZProcessorThread(fileEntry.toString()));
		}
		for (CSVGZProcessorThread listedThread : fileThreadsMap.values()) {
			listedThread.start();
		}
		for (CSVGZProcessorThread listedThread : fileThreadsMap.values()) {
			listedThread.join();
		}
	}

	/*
	 * After all files are read, carrierPricesMap and activeIn15 contains
	 * necessary values. Medians and averages are calculated using these values
	 * and displayed.
	 */
	private static void postProcessingCalculations(String valueType) {
		for (String carrier : carMonthPricesMap.keySet()) {
			ArrayList<Float> prices = carMonthPricesMap.get(carrier);
			switch (valueType) {
			case "-mean":
				calculateAverages(carrier, prices);
				break;
			case "-median":
				calculateMedians(carrier, prices);
				break;
			case "-fastMedian":
				calculateFastMedians(carrier, prices);
				break;
			}
//			sortAverages();
		}
	}

	private static ArrayList<String> listOfTop10Carriers() {
		HashMap<String, Integer> carCountMap = new HashMap<String, Integer>();
		
		for(String carMonthKey : carMonthPricesMap.keySet()){
			String[] keySplit = carMonthKey.split(",");
			String car = keySplit[0];
			
			if(!carCountMap.containsKey(car)){
				carCountMap.put(car, 0);
			}
			
			int count = carCountMap.get(car);
			count += carMonthPricesMap.get(carMonthKey).size();
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

	private static void calculateMedians(String carrier, ArrayList<Float> prices) {
		Collections.sort(prices);
		int len = prices.size();
		int middle = len / 2;
		if (len % 2 == 1) {
			valueMap.put(carrier, prices.get(middle));
		} else {
			valueMap.put(carrier, (float) ((prices.get(middle - 1) + prices.get(middle)) / 2.0));
		}
	}

	private static void calculateAverages(String carrier, ArrayList<Float> prices) {
		float avg = 0;
		for (float p : prices) {
			avg += p;
		}
		avg = avg / prices.size();
		valueMap.put(carrier, avg);
	}

	private static void calculateFastMedians(String carrier, ArrayList<Float> prices) {
		valueMap.put(carrier, QuickSelect.quickSelect(prices, prices.size() / 2));
	}
	
	/*
	 * QuickSelect class
	 * For sorting lists:
	 * Referred from: Cory Hardman's Blog
	 * 
	 * http://www.coryhardman.com/2011/03/finding-median-in-almost-linear-time.html
	 */
	public static class QuickSelect {
	    public static Float quickSelect(List <Float> values, int k)
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
	    private static int partition(List <Float> values, int left, int right, int partitionIndex)
	    {
	    	Float partionValue = values.get(partitionIndex);
	        int newIndex = left;
	        Float temp = values.get(partitionIndex);
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
	
	private static void displayOutput(String valueType, String runType, String outputPath, ArrayList<String> top10Cars) {
		String fileName = valueType.substring(1) + "_" + runType.substring(1) + ".txt";
		String outputDest = outputPath + "/" + fileName;
		PrintWriter writer;
		try {
			writer = new PrintWriter(outputDest, "ASCII");
			for (String carMonthKey : valueMap.keySet()){
				String[] keys = carMonthKey.split(",");
				String carrierId = keys[0];
				String month = keys[1];
				if(top10Cars.contains(carrierId)){
					writer.println(month + " " + carrierId + " " + valueMap.get(carMonthKey));
				}
			}
			writer.close();
		} catch (FileNotFoundException e) {
			System.err.println("Unable to write to file [file not found] " + fileName);
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.err.println("Unable to write to file [unsupported encoding] " + fileName);
			e.printStackTrace();
		}
		
		System.out.println(invalidCount + "");
		System.out.println(validCount + "");
	}

	/*
	 * Methods to accumulate values from the threads. Called at the end of run()
	 * method of CSVGZProcessorThread.
	 */

	public static synchronized void increaseValidCount(long count) {
		validCount += count;
	}

	public static synchronized void increaseInvalidCount(long count) {
		invalidCount += count;
	}

	public static synchronized void addAllPricesForCarrierMonth(String carrier, ArrayList<Float> prices) {
		ArrayList<Float> allPrices = null;
		if (carMonthPricesMap.containsKey(carrier)) {
			allPrices = MultiThreadComparison.carMonthPricesMap.get(carrier);
		} else {
			allPrices = new ArrayList<Float>();
		}

		allPrices.addAll(prices);
		MultiThreadComparison.carMonthPricesMap.put(carrier, allPrices);
	}

	public static synchronized void addAllActiveIn15(HashSet<String> carriers) {
		MultiThreadComparison.activeIn15.addAll(carriers);
	}

}

class CSVGZProcessorThread extends Thread {
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

	private ArrayList<String> csvHeader = new ArrayList<String>();
	private String csvGzFilename;	// each thread will run for a separate file

	private long fileValidCount = 0; // F value for this file
	private long fileInvalidCount = 0; // K value for this file
	private HashMap<String, ArrayList<Float>> fileCarMonthPricesMap = new HashMap<String, ArrayList<Float>>();
	// stores a list of all prices from the data files, for each carrier; only for this file
	private HashSet<String> fileActiveIn15 = new HashSet<String>();
	// stores a set of all carriers active in January 2015; only for this file
	
	public CSVGZProcessorThread(String csvgzfname) {
		csvGzFilename = csvgzfname;
	}

	@Override
	public void run() {
		try {
			processTarFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// After the file is processed, we merge the file values with values in MultifileProcessor class
		synchronizeFileValues();
	}

	private void synchronizeFileValues() {
		MultiThreadComparison.increaseInvalidCount(fileInvalidCount);
		MultiThreadComparison.increaseValidCount(fileValidCount);
		
		for (String carMonthKey : fileCarMonthPricesMap.keySet()) {
			MultiThreadComparison.addAllPricesForCarrierMonth(carMonthKey, fileCarMonthPricesMap.get(carMonthKey));
		}
		
		MultiThreadComparison.addAllActiveIn15(fileActiveIn15);
	}

	/*
	 * This method will completely process a file assigned to this thread.
	 * for each record
	 * - perform sanity check
	 * - if record is valid
	 *   - increment valid count
	 * 	 - add record values to map
	 *   - if record is from January 2015
	 *     - add carrier name to activeInJan15 set
	 * - else  
	 *   - increment invalid count
	 */
	public void processTarFile() throws IOException {
		InputStream fileStream = null;
		InputStream gzipStream = null;
		BufferedReader bufferedReader = null;
		try {
			fileStream = new FileInputStream(csvGzFilename);
			gzipStream = new GZIPInputStream(fileStream);
			Reader decoder = new InputStreamReader(gzipStream, "ASCII");
			bufferedReader = new BufferedReader(decoder);

			String fileEntry;

			csvHeader = new ArrayList<String>(Arrays.asList(bufferedReader.readLine().split(",")));
			//csvHeader.remove(csvHeader.size() - 1);
			
			int csvHeaderLen = csvHeader.size();
			
			while ((fileEntry = bufferedReader.readLine()) != null) {
				fileEntry = fileEntry.replaceAll("\"", "");
				String correctedString = fileEntry.replaceAll(", ", ":");
				String[] fields = correctedString.split(",");
				
				if (csvHeaderLen == fields.length && isRecordValid(fields)) {
					fileValidCount += 1;

					String carrierId = fields[csvHeader.indexOf(Field.CARRIER)];
					
					float ticketPrice = Float.parseFloat(fields[csvHeader.indexOf(Field.AVG_TICKET_PRICE)]);
					String carMonthKey = carrierId + "," + fields[csvHeader.indexOf(Field.MONTH)];
					ArrayList<Float> allPrices = null;
					if (fileCarMonthPricesMap.containsKey(carMonthKey)) {
						allPrices = fileCarMonthPricesMap.get(carMonthKey);
					} else {
						allPrices = new ArrayList<Float>();
					}

					allPrices.add(new Float(ticketPrice));
					fileCarMonthPricesMap.put(carMonthKey, allPrices);

					if (Integer.parseInt(fields[csvHeader.indexOf(Field.YEAR)]) == 2015) {
						fileActiveIn15.add(carrierId);
					}
				} else {
					fileInvalidCount += 1;
				}
			}

		} catch (FileNotFoundException e) {
			System.err.println("File not found");
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.err.println("Unable to decode in given file.");
			e.printStackTrace();
		} finally {
			fileStream.close();
			gzipStream.close();
			bufferedReader.close();
		}

	}

	private boolean isRecordValid(String[] fields) {

		float timeZone = 0;

		// CRSArrTime and CRSDepTime should not be zero
		// timeZone % 60 should be 0

		SimpleDateFormat format = new SimpleDateFormat("HHmm");

		String crsArrTime = fields[csvHeader.indexOf(Field.CRS_ARR_TIME)];
		String crsDepTime = fields[csvHeader.indexOf(Field.CRS_DEP_TIME)];
		String crsElapsedTime = fields[csvHeader.indexOf(Field.CRS_ELAPSED_TIME)];

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

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.ORIGIN_AIRPORT_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.ORIGIN_AIRPORT_SEQ_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.ORIGIN_CITY_MARKET_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.ORIGIN_STATE_FIPS)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.ORIGIN_WAC)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.DEST_AIRPORT_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.DEST_AIRPORT_SEQ_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.DEST_CITY_MARKET_ID)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.DEST_STATE_FIPS)]) <= 0)
			return false;

		if (Integer.parseInt(fields[csvHeader.indexOf(Field.DEST_WAC)]) <= 0)
			return false;

		// Origin, Destination, CityName, State, StateName should not be empty

		if (fields[csvHeader.indexOf(Field.ORIGIN)].equals(""))
			return false;

		if (fields[csvHeader.indexOf(Field.ORIGIN_CITY_NAME)].equals(""))
			return false;

		if (fields[csvHeader.indexOf(Field.ORIGIN_STATE_ABR)].equals(""))
			return false;

		if (fields[csvHeader.indexOf(Field.ORIGIN_STATE_NM)].equals(""))
			return false;

		if (fields[csvHeader.indexOf(Field.DEST)].equals(""))
			return false;

		if (fields[csvHeader.indexOf(Field.DEST_CITY_NAME)].equals(""))
			return false;

		if (fields[csvHeader.indexOf(Field.DEST_STATE_ABR)].equals(""))
			return false;

		if (fields[csvHeader.indexOf(Field.DEST_STATE_NM)].equals(""))
			return false;

		// For flights that are not Cancelled:

		int cancelledDigit = Integer.parseInt(fields[csvHeader.indexOf(Field.CANCELLED)]);

		if (cancelledDigit != 1) {

			String arrTime = fields[csvHeader.indexOf(Field.ARR_TIME)];
			String depTime = fields[csvHeader.indexOf(Field.DEP_TIME)];
			String actElapsedTime = fields[csvHeader.indexOf(Field.ACTUAL_ELAPSED_TIME)];

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

				float arrDelay = Float.parseFloat(fields[csvHeader.indexOf(Field.ARR_DELAY)]);

				float ArrDelayMinutes = Float.parseFloat(fields[csvHeader.indexOf(Field.ARR_DELAY_NEW)]);

				float arrDel15 = Float.parseFloat(fields[csvHeader.indexOf(Field.ARR_DEL15)]);

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

		if (Float.parseFloat(fields[csvHeader.indexOf(Field.AVG_TICKET_PRICE)]) > 999999)
			return false;

		return true;
	}

	private int hhmmDiff(String arr, String dep) {
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
