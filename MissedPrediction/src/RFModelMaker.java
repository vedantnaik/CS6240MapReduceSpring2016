import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

public class RFModelMaker {

	static class Constants {
		public static final int PRED_CLASS_INDEX = 10;
		
		public static final List<String> LOCATIONS = Arrays.asList("ATL", "LAX", "ORD", "DFW", "JFK", "DEN", "SFO", "CLT", "LAS", "PHX", "IAH",
				"MIA", "SEA", "EWR", "MCO", "MSP", "DTW", "BOS", "PHL", "LGA", "FLL", "BWI", "IAD", "MDW", "SLC", "DCA",
				"HNL", "SAN", "TPA", "PDX", "STL", "HOU", "BNA", "AUS", "OAK", "MCI", "MSY", "RDU", "SJC", "SNA", "DAL",
				"SMF", "SJU", "SAT", "RSW", "PIT", "CLE", "IND", "MKE", "CMH", "N/A");
	
		private static final List<String> CARRIERS_LIST = Arrays.asList("9E", "AA", "AS", "B6", "DL", "EV", "F9", "FL", "HA", 
				"MQ", "NK", "OO", "UA", "US", "VX", "WN", "YV"); 

	}
	
	private static ArrayList<Attribute> airlineAttributes;
	
	public RFModelMaker() {
		
		airlineAttributes = new ArrayList<Attribute>();
		
		airlineAttributes.add(new Attribute("crsArrTime"));
		airlineAttributes.add(new Attribute("crsDepTime"));
		airlineAttributes.add(new Attribute("quarter"));
		airlineAttributes.add(new Attribute("originAirport"));
		airlineAttributes.add(new Attribute("destAirport"));
		airlineAttributes.add(new Attribute("carrier"));
		airlineAttributes.add(new Attribute("dayOfMonth"));
		airlineAttributes.add(new Attribute("dayOfWeek"));
		airlineAttributes.add(new Attribute("distanceGroup"));
		airlineAttributes.add(new Attribute("isHoliday"));
		
		ArrayList<String> predVals = new ArrayList<String>();
		predVals.add("1.0");
		predVals.add("0.0");
		airlineAttributes.add(new Attribute("predClass", predVals));
	}
	
	public static ArrayList<Attribute> getAirlineAttributes(){
		return airlineAttributes;
	}
	
	public static void writeModelToFileSystem(RandomForest rfClassifer, Reducer<Text, AirlineMapperValue, Text, Text>.Context context, Text key) throws IOException {
		Configuration conf = context.getConfiguration();
		
		FileSystem fileSystem = FileSystem.get(URI.create(conf.get("rfModelLocation")), conf);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(conf.get("rfModelLocation")+"/"+key.toString()));				
		try {
			SerializationHelper.write(fsDataOutputStream, rfClassifer);
		} catch (Exception e) {
			e.printStackTrace();
		}
		fsDataOutputStream.close();
	}
}
