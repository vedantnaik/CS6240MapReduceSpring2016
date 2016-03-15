import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instances;

public class RFModelMaker {

	static class Constants {
		public static final int PRED_CLASS_INDEX = 11;
		
		public static final List<String> LOCATIONS = Arrays.asList("ATL", "LAX", "ORD", "DFW", "JFK", "DEN", "SFO", "CLT", "LAS", "PHX", "IAH",
				"MIA", "SEA", "EWR", "MCO", "MSP", "DTW", "BOS", "PHL", "LGA", "FLL", "BWI", "IAD", "MDW", "SLC", "DCA",
				"HNL", "SAN", "TPA", "PDX", "STL", "HOU", "BNA", "AUS", "OAK", "MCI", "MSY", "RDU", "SJC", "SNA", "DAL",
				"SMF", "SJU", "SAT", "RSW", "PIT", "CLE", "IND", "MKE", "CMH", "N/A");
	}
	
	private static ArrayList<Attribute> airlineAttributes;
	
	public RFModelMaker() {
		
		airlineAttributes = new ArrayList<Attribute>();
		
		airlineAttributes.add(new Attribute("crsArrTime", 		1));
		airlineAttributes.add(new Attribute("crsDepTime", 		2));
		airlineAttributes.add(new Attribute("quarter", 			3));
		airlineAttributes.add(new Attribute("originAirport", 	4));
		airlineAttributes.add(new Attribute("destAirport", 		5));
		airlineAttributes.add(new Attribute("carrier", 			6));
		airlineAttributes.add(new Attribute("dayOfMonth", 		7));
		airlineAttributes.add(new Attribute("dayOfWeek", 		8));
		airlineAttributes.add(new Attribute("distanceGroup", 	9));
		airlineAttributes.add(new Attribute("isHoliday", 		10));
		
		ArrayList<String> predVals = new ArrayList<String>();
		predVals.add("1");
		predVals.add("0");
		airlineAttributes.add(new Attribute("predClass", predVals, Constants.PRED_CLASS_INDEX));
	}
	
	public static ArrayList<Attribute> getAirlineAttributes(){
		System.out.println(airlineAttributes.size() + "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		return airlineAttributes;
	}
}
