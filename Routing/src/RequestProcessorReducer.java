import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RequestProcessorReducer extends Reducer<Text, Text, Text, Text> {

	
	HashMap<String, String> finalOutputMap = new HashMap<String, String>(); 
	
	@Override
	protected void reduce(Text key, Iterable<Text> listOfAMVs, Context context) throws IOException, InterruptedException {
		//System.out.println("Reducer for " + key.toString());
		
		String requestedRecord = "init";
		//System.out.println("Set request record : " +requestedRecord);
		String minimumRecord = "init";
		//System.out.println("Set minumum record : " +minimumRecord);
		double minDuration = 9999999.0;
		
		for(Text eachAMViter : listOfAMVs){
			Text eachAMV = new Text(eachAMViter);				
			
			String[] eachAMVparts = eachAMV.toString().split("\t");
			
			String recordType = eachAMVparts[0];
//			System.out.println(recordType + " << type is " + recordType.equalsIgnoreCase("REDUCER_OUTPUT"));
			String recordValue = eachAMVparts[1]; //year+","+month+","+day+","+origin+","+dest+","+flnum1+","+flnum2+","+duration;

			if(recordType.equalsIgnoreCase("REDUCER_OUTPUT")) {
				
				double recordDuration = Double.parseDouble(recordValue.split(",")[7]);
				boolean pr = recordDuration < minDuration;
//				System.out.println(recordDuration + " gives " + pr);
				
				if(recordDuration < minDuration) {
					minDuration = recordDuration;
					minimumRecord = recordValue;					
					System.out.println("Set minumum record : " +minimumRecord);
				}
						
			} else {
//				year+","+month+","+day+","+origin+","+dest
				requestedRecord = recordValue + ",0"; // added ignore val
				System.out.println("Set request record : " +requestedRecord);

			}
			//System.out.println(requestedRecord + " === " + minimumRecord);
		}
		//System.out.println("Key : "+requestedRecord + " | " + " value : " + minimumRecord);
		
		finalOutputMap.put(requestedRecord, minimumRecord);
		
	}
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		for(String keyVal : finalOutputMap.keySet()){
			context.write(new Text(keyVal), new Text(finalOutputMap.get(keyVal)));
		}
	}
	
}
