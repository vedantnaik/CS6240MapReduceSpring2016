import java.io.IOException;

/*
 * -------------------------------------------------------------------------------------------------------
 * 										RequestProcessorMapper
 * 
 * This class takes the connections that are calculated from the previous step and maps them into:
 * 
 * Key => Year Month Day OriginAirport DestinationAirport
 * Value => TYPE Year Month Day OriginAirport DestinationAirport FlightNumber(Flight 1) FlightNumber(Flight 2) Duration
 * where,
 * TYPE = REDUCER_OUTPUT or REQUEST_FILE
 * 
 * The TYPE categorizes the emitted key-value pair to be either an ouput record from the reducer
 * of the previous step or record that is picked up from the request file
 * 
 * -------------------------------------------------------------------------------------------------------
 */


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RequestProcessorMapper extends Mapper<Object, Text, Text, Text> {

	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] fileEntryValues = value.toString().replace(",", "\t").split("\t");
		
		if (fileEntryValues.length == 8){
			// output file
			String TYPE = "REDUCER_OUTPUT";
			String origin = fileEntryValues[0];
			String dest = fileEntryValues[1];
			String flnum1 = fileEntryValues[2];
			String flnum2 = fileEntryValues[3];
			String duration = fileEntryValues[4];
			String year = fileEntryValues[5];
			String month = fileEntryValues[6];
			String day = fileEntryValues[7];
			
			String mapperKey = year + "\t" + month + "\t" + day + "\t" + origin + "\t" + dest;			
			
			String missedFileRecordFormatValue = year+","+month+","+day+","+origin+","+dest+","+flnum1+","+flnum2+","+duration;
			
			String mapperValue = TYPE + "\t" + missedFileRecordFormatValue;
			
			context.write(new Text(mapperKey), new Text(mapperValue));
			
		} else {
			// request file
			System.out.println("fetching from Request file=============="+value.toString());
			String TYPE = "REQUEST_FILE";
			String year = fileEntryValues[0];
			String month = fileEntryValues[1];
			String day = fileEntryValues[2];
			String origin = fileEntryValues[3];
			String dest = fileEntryValues[4];
			String _ignore = fileEntryValues[5];
			
			String mapperKey = year + "\t" + month + "\t" + day + "\t" + origin + "\t" + dest;			
			
			String missedFileRecordFormatValue = year+","+month+","+day+","+origin+","+dest;
			
			String mapperValue = TYPE + "\t" + missedFileRecordFormatValue;
			System.out.println("key => " + mapperKey + " value => " + mapperValue);
			context.write(new Text(mapperKey), new Text(mapperValue));
		}
	
	}
	
}
