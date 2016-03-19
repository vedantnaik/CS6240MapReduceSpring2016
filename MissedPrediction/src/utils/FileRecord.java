package utils;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.math.NumberUtils;

/*
 * Authors: Vedant Naik, Rohan Joshi
 * */
public class FileRecord {
	public class Field {
		public static final String CRS_ARR_TIME = "CRS_ARR_TIME";
		public static final String CRS_DEP_TIME = "CRS_DEP_TIME";
		public static final String CRS_ELAPSED_TIME = "CRS_ELAPSED_TIME";

		public static final String ORIGIN_AIRPORT_ID = "ORIGIN_AIRPORT_ID";
		public static final String ORIGIN_AIRPORT_SEQ_ID = "ORIGIN_AIRPORT_SEQ_ID";
		public static final String ORIGIN_CITY_MARKET_ID = "ORIGIN_CITY_MARKET_ID";
		public static final String ORIGIN_STATE_FIPS = "ORIGIN_STATE_FIPS";
		public static final String ORIGIN_WAC = "ORIGIN_WAC";
		public static final String DEST_AIRPORT_ID = "DEST_AIRPORT_ID";
		public static final String DEST_AIRPORT_SEQ_ID = "DEST_AIRPORT_SEQ_ID";
		public static final String DEST_CITY_MARKET_ID = "DEST_CITY_MARKET_ID";
		public static final String DEST_STATE_FIPS = "DEST_STATE_FIPS";
		public static final String DEST_WAC = "DEST_WAC";

		public static final String ORIGIN = "ORIGIN";
		public static final String ORIGIN_CITY_NAME = "ORIGIN_CITY_NAME";
		public static final String ORIGIN_STATE_ABR = "ORIGIN_STATE_ABR";
		public static final String ORIGIN_STATE_NM = "ORIGIN_STATE_NM";
		public static final String DEST = "DEST";
		public static final String DEST_CITY_NAME = "DEST_CITY_NAME";
		public static final String DEST_STATE_ABR = "DEST_STATE_ABR";
		public static final String DEST_STATE_NM = "DEST_STATE_NM";

		public static final String CANCELLED = "CANCELLED";

		public static final String ARR_TIME = "ARR_TIME";
		public static final String DEP_TIME = "DEP_TIME";
		public static final String ACTUAL_ELAPSED_TIME = "ACTUAL_ELAPSED_TIME";

		public static final String ARR_DELAY = "ARR_DELAY";
		public static final String ARR_DELAY_NEW = "ARR_DELAY_NEW";
		public static final String ARR_DEL15 = "ARR_DEL15";

		public static final String AVG_TICKET_PRICE = "AVG_TICKET_PRICE";

		public static final String FL_DATE = "FL_DATE";
		public static final String CARRIER = "CARRIER";

		public static final String YEAR = "YEAR";
		public static final String MONTH = "MONTH";
		
		public static final String DISTANCE = "DISTANCE";
		public static final String AIR_TIME = "AIR_TIME";
		
		public static final String DIV_DISTANCE = "DIV_DISTANCE";
		
		public static final String FL_NUM = "FL_NUM";
		
		public static final String DAY_OF_MONTH = "DAY_OF_MONTH";
		public static final String DAY_OF_WEEK = "DAY_OF_WEEK";
		
		public static final String TAXI_OUT = "TAXI_OUT";
		public static final String TAXI_IN = "TAXI_IN";
		
		public static final String DISTANCE_GROUP = "DISTANCE_GROUP";
		
		public static final String QUARTER = "QUARTER";
		
	}

	public static final String[] csvh = { "YEAR", "QUARTER", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "FL_DATE",
			"UNIQUE_CARRIER", "AIRLINE_ID", "CARRIER", "TAIL_NUM", "FL_NUM", "ORIGIN_AIRPORT_ID",
			"ORIGIN_AIRPORT_SEQ_ID", "ORIGIN_CITY_MARKET_ID", "ORIGIN", "ORIGIN_CITY_NAME", "ORIGIN_STATE_ABR",
			"ORIGIN_STATE_FIPS", "ORIGIN_STATE_NM", "ORIGIN_WAC", "DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID",
			"DEST_CITY_MARKET_ID", "DEST", "DEST_CITY_NAME", "DEST_STATE_ABR", "DEST_STATE_FIPS", "DEST_STATE_NM",
			"DEST_WAC", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_NEW", "DEP_DEL15", "DEP_DELAY_GROUP",
			"DEP_TIME_BLK", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY",
			"ARR_DELAY_NEW", "ARR_DEL15", "ARR_DELAY_GROUP", "ARR_TIME_BLK", "CANCELLED", "CANCELLATION_CODE",
			"DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "FLIGHTS", "DISTANCE", "DISTANCE_GROUP",
			"CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "FIRST_DEP_TIME",
			"TOTAL_ADD_GTIME", "LONGEST_ADD_GTIME", "DIV_AIRPORT_LANDINGS", "DIV_REACHED_DEST",
			"DIV_ACTUAL_ELAPSED_TIME", "DIV_ARR_DELAY", "DIV_DISTANCE", "DIV1_AIRPORT", "DIV1_AIRPORT_ID",
			"DIV1_AIRPORT_SEQ_ID", "DIV1_WHEELS_ON", "DIV1_TOTAL_GTIME", "DIV1_LONGEST_GTIME", "DIV1_WHEELS_OFF",
			"DIV1_TAIL_NUM", "DIV2_AIRPORT", "DIV2_AIRPORT_ID", "DIV2_AIRPORT_SEQ_ID", "DIV2_WHEELS_ON",
			"DIV2_TOTAL_GTIME", "DIV2_LONGEST_GTIME", "DIV2_WHEELS_OFF", "DIV2_TAIL_NUM", "DIV3_AIRPORT",
			"DIV3_AIRPORT_ID", "DIV3_AIRPORT_SEQ_ID", "DIV3_WHEELS_ON", "DIV3_TOTAL_GTIME", "DIV3_LONGEST_GTIME",
			"DIV3_WHEELS_OFF", "DIV3_TAIL_NUM", "DIV4_AIRPORT", "DIV4_AIRPORT_ID", "DIV4_AIRPORT_SEQ_ID",
			"DIV4_WHEELS_ON", "DIV4_TOTAL_GTIME", "DIV4_LONGEST_GTIME", "DIV4_WHEELS_OFF", "DIV4_TAIL_NUM",
			"DIV5_AIRPORT", "DIV5_AIRPORT_ID", "DIV5_AIRPORT_SEQ_ID", "DIV5_WHEELS_ON", "DIV5_TOTAL_GTIME",
			"DIV5_LONGEST_GTIME", "DIV5_WHEELS_OFF", "DIV5_TAIL_NUM", "AVG_TICKET_PRICE" };
	public static final ArrayList<String> csvHeaders = new ArrayList<String>(Arrays.asList(csvh));

	public static final List<String> HOLIDAY_LIST = Arrays.asList("2011-01-01", "2011-01-17", "2011-02-14", "2011-02-21", "2011-04-24", "2011-05-08", "2011-05-30", 
			"2011-06-19", "2011-07-04", "2011-09-05", "2011-10-10", "2011-10-31", "2011-11-11", "2011-11-24", "2011-12-24", "2011-12-25", 
			"2011-12-26", "2011-12-31", "2012-01-01", "2012-01-02", "2012-01-16", "2012-02-14", "2012-02-20", "2012-04-08", "2012-05-13", 
			"2012-06-17", "2012-07-04", "2012-09-03", "2012-10-08", "2012-10-31", "2012-11-06", "2012-11-11", "2012-11-22", "2012-12-24", 
			"2012-12-25", "2012-12-31", "2013-01-01", "2013-01-21", "2013-02-14", "2013-02-18", "2013-03-31", "2013-03-31", "2013-05-12", 
			"2013-05-27", "2013-06-16", "2013-06-16", "2013-07-04", "2013-09-02", "2013-10-14", "2013-10-31", "2013-11-11", "2013-11-28", 
			"2013-12-24", "2013-12-25", "2013-12-31", "2014-01-01", "2014-01-20", "2014-02-14", "2014-02-17", "2014-04-13", "2014-04-20", 
			"2014-05-11", "2014-05-26", "2014-06-15", "2014-07-04", "2014-09-01", "2014-10-13", "2014-10-31", "2014-11-11", "2014-11-27", 
			"2014-12-24", "2014-12-25", "2014-12-31", "2015-01-01", "2015-01-19", "2015-02-14", "2015-02-16", "2015-04-05", "2015-04-13", 
			"2015-05-10", "2015-05-25", "2015-06-21", "2015-07-03", "2015-07-04", "2015-09-07", "2015-10-12", "2015-10-31", "2015-11-11", 
			"2015-11-26", "2015-12-24", "2015-12-25", "2015-12-26", "2015-12-31");
	
	public static String getValueOf(String[] fields, String headerName) {
		return fields[csvHeaders.indexOf(headerName)];
	}
	
	public static boolean isValidationRecordValid(String[] validRecords){
		boolean valIsCorrect = validRecords[1].equalsIgnoreCase("TRUE") || validRecords[1].equalsIgnoreCase("FALSE");
		
		String[] flnum_fldate_crsDep = validRecords[0].split("_");
		
		boolean keyIsCorrect = NumberUtils.isDigits(flnum_fldate_crsDep[0]) 
				&& flnum_fldate_crsDep[1].matches("\\d{4}-\\d{2}-\\d{2}")
				&& NumberUtils.isDigits(flnum_fldate_crsDep[2]);
		
		if(keyIsCorrect && valIsCorrect){
			return true;
		}
		
		return false;
	}
	
	public static boolean isTestRecordValid(String[] fields) {
		
		if(fields[csvHeaders.indexOf(Field.MONTH)].equals("") || fields[csvHeaders.indexOf(Field.MONTH)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.FL_DATE)].equals("") || fields[csvHeaders.indexOf(Field.FL_DATE)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.CRS_ARR_TIME)].equals("") || fields[csvHeaders.indexOf(Field.CRS_ARR_TIME)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.CRS_DEP_TIME)].equals("") || fields[csvHeaders.indexOf(Field.CRS_DEP_TIME)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.QUARTER)].equals("") || fields[csvHeaders.indexOf(Field.QUARTER)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.ORIGIN)].equals("") || fields[csvHeaders.indexOf(Field.ORIGIN)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.DEST)].equals("") || fields[csvHeaders.indexOf(Field.DEST)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.CARRIER)].equals("") || fields[csvHeaders.indexOf(Field.CARRIER)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.FL_NUM)].equals("") || fields[csvHeaders.indexOf(Field.FL_NUM)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.DAY_OF_MONTH)].equals("") || fields[csvHeaders.indexOf(Field.DAY_OF_MONTH)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.DAY_OF_WEEK)].equals("") || fields[csvHeaders.indexOf(Field.DAY_OF_WEEK)].equalsIgnoreCase("NA")){
			return false;
		}
		
		if(fields[csvHeaders.indexOf(Field.DISTANCE_GROUP)].equals("") || fields[csvHeaders.indexOf(Field.DISTANCE_GROUP)].equalsIgnoreCase("NA")){
			return false;
		}
		
		return true;
	}
	
	public static boolean isRecordValid(String[] fields) {

		// Special quick checks for MissingConnections assignment
		if (fields[csvHeaders.indexOf(Field.CRS_ARR_TIME)].equals("") ||
			fields[csvHeaders.indexOf(Field.CRS_DEP_TIME)].equals("") ||
			fields[csvHeaders.indexOf(Field.ARR_TIME)].equals("") ||
			fields[csvHeaders.indexOf(Field.DEP_TIME)].equals("")){
			return false;
		}
		
		
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
		try {
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

	public static String makeCompleteHHMM(String hhmm){
		if (hhmm.length() == 3){
			hhmm = "0"+hhmm;
		}
		
		if (hhmm.length() == 2){
			hhmm = "00"+hhmm;
		}
		
		if (hhmm.length() == 1){
			hhmm = "000"+hhmm;
		}
		
		if (hhmm.length() == 0){
			hhmm = "0000";
		}
		
		return hhmm;
	}
	
	public static int hhmmDiff(String arr, String dep) {
		
		arr = makeCompleteHHMM(arr);
		dep = makeCompleteHHMM(dep);
		
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

	
	public static int dateIsAroundHoliday(String flightDate, int dayBuffer) {
		
		String[] yyyyMMdd = flightDate.split("-");
		int mm = Integer.parseInt(yyyyMMdd[1]);
		int dd = Integer.parseInt(yyyyMMdd[2]);
		
		for(String hday : HOLIDAY_LIST){
			String[] hday_yyyyMMdd = hday.split("-");
			int hmm = Integer.parseInt(hday_yyyyMMdd[1]);
			int hdd = Integer.parseInt(hday_yyyyMMdd[2]);
				
			if(mm == hmm){
				if((hdd - dayBuffer) < dd && dd < (hdd + dayBuffer)){
					return 1;
				}
			}
		}
		
		return 0;
	}


}