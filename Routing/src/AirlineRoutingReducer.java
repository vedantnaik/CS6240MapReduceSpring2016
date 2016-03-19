import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomForest;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class AirlineRoutingReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
		System.out.println("Reducer for " + key.toString());

		String[] keyParts = key.toString().split("\t");
		String month = keyParts[3];
		
//		Classifier rfClassifier = null;
//		try {
//			 rfClassifier = RFModelMaker.getMonthModelFromFileSystem(context, new Text(month));
//			 if(rfClassifier == null) {System.err.println("CLASSIFIER MISSING!! month : " + key.toString()); return;}
//		} catch (Exception e) {
//			System.err.println("Problem reading the model for month " + key.toString());
//			e.printStackTrace();
//			return;
//		} // do we need this/.
		
		
		//RFModelMaker rfModel = new RFModelMaker();
		//Instances testingInstances = new Instances("Testing", rfModel.getAirlineAttributes(), RFModelMaker.Constants.ATTR_SIZE); 
		//testingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
		
		System.out.println("--------------------------------------------------");
		System.out.println("------------------ENTERED REDUCER-----------------");
		System.out.println("--------------------------------------------------");
		
		
		ArrayList<AirlineMapperValue> Orig_listOfAMVs = new ArrayList<AirlineMapperValue>();
		ArrayList<AirlineMapperValue> Dest_listOfAMVs = new ArrayList<AirlineMapperValue>();
		
		for(AirlineMapperValue amv : listOfAMVs){
			if(amv.getConnectionLink().toString().equalsIgnoreCase(AirlineMapperValue.CONN_TYPE_ORIGIN)){
				Orig_listOfAMVs.add(new AirlineMapperValue(amv));	
			} else {
				Dest_listOfAMVs.add(new AirlineMapperValue(amv));
			}
		}
		
		for (AirlineMapperValue incoming_amv : Dest_listOfAMVs){
			
			/*Instance inst = new DenseInstance(RFModelMaker.Constants.ATTR_SIZE);
			inst.setDataset(testingInstances);
			inst.setValue(0, (double) incoming_amv.getCrsArrTime().get());
			inst.setValue(1, (double) incoming_amv.getCrsDepTime().get());
			inst.setValue(2, (double) incoming_amv.getQuarter().get());
			inst.setValue(3, (double) incoming_amv.getOriginAirport().toString().hashCode());
			inst.setValue(4, (double) incoming_amv.getDestAirport().toString().hashCode());
			inst.setValue(5, (double) incoming_amv.getCarrier().toString().hashCode());
			inst.setValue(6, (double) incoming_amv.getDayOfMonth().get());
			inst.setValue(7, (double) incoming_amv.getDayOfWeek().get());
			inst.setValue(8, (double) incoming_amv.getDistanceGroup().get());
			inst.setValue(9, (double) incoming_amv.getIsHoliday().get());

			inst.setValue(RFModelMaker.Constants.PRED_CLASS_INDEX, Double.NaN);
			
			boolean g_amv_isDelayed;
			try {
				g_amv_isDelayed = (rfClassifier.classifyInstance(inst) == 1.0 ? true : false);
			} catch (Exception e) {
				System.err.println("Unable to predict for flight ");
				e.printStackTrace();
				continue;
			}*/
			
			//if(!g_amv_isDelayed){
			if(true){
				for (AirlineMapperValue outgoing_amv : Orig_listOfAMVs) {
					if (isConnection(outgoing_amv, incoming_amv)) {
						
						// The output from this reducer should be of the form:
						
						// incoming origin + outgoing destination <=> {incoming flight number + outgoing flight number + (total duration)}
						// This will be read by the next job
						// An example output is: (one itinerary)
						/*
						 * NY BOS <=> xxxx + xxxx + 3000
						 * SFO BOS <=> xxxx + xxxx + 9000
						 * CHA SFO <=> xxxx + xxxx + 5000
						 * */
						// The subsequent job will read this and compare it to the request file
						// The format of the request file is: (the last field is to be ignored)
						// Year Month Day   Origin Destination   Ignore
						// 2004	 10	   4	BWI	    PHL	           37
						
						// If a match is found, then the itinerary will be compared to the Missed04 file
						// if it matches, then the duration will be added by 100 and pushed into a hashmap
						// if no match is found, then the itinerary will be pushed as it is into a hashmap
						// hashmap key could be the origin+dest and the values will be a list of flight itineraries 
						// we then get the minimum for each origin + dest and print it with the two flight numbers
						
						
						
						// summing the duration of the two legs of the flight
						double sumDuration = Integer.parseInt(incoming_amv.getCrsElapsedTime().toString()) 
											+ Integer.parseInt(outgoing_amv.getCrsElapsedTime().toString());
						
						// output of the format:
						//Origin Dest    incoming flnum   outgoing flnum   duration of two legs
						// NY     BOS <=>     xxxx       +    xxxx       +      3000
						// System.out.println("inside n square:: " + sumDuration);
						context.write(new Text(incoming_amv.getOrigin().toString() + "\t" + outgoing_amv.getDest().toString()), 
									new Text(incoming_amv.getFlNum().toString() + "\t" + outgoing_amv.getFlNum().toString() + "\t" + sumDuration + "\t"
											+ incoming_amv.getYear().toString() + "\t" + incoming_amv.getMonth().toString() + "\t" + incoming_amv.getDayOfMonth().toString()
											));
					}
				} 
			}
			
		}
		
		
		
	}

	private boolean isConnection(AirlineMapperValue outgoing_amv, AirlineMapperValue incoming_amv) {
		long timeDiff = longTimeDifferenceInMins(outgoing_amv.getCrsDepTime_long().get(), incoming_amv.getCrsArrTime_long().get());
		if(timeDiff < 60 && timeDiff > 30){
			return true;
		}
		return false;
	}

	private static long longTimeDifferenceInMins(long t1, long t2) {
		return TimeUnit.MILLISECONDS.toMinutes(t1 - t2);
	}
}