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
		
		Classifier rfClassifier = null;
		try {
			 rfClassifier = RFModelMaker.getMonthModelFromFileSystem(context, new Text(month));
			 if(rfClassifier == null) {System.err.println("CLASSIFIER MISSING!! month : " + key.toString()); return;}
		} catch (Exception e) {
			System.err.println("Problem reading the model for month " + key.toString());
			e.printStackTrace();
			return;
		}
		
		
		RFModelMaker rfModel = new RFModelMaker();
		Instances testingInstances = new Instances("Testing", rfModel.getAirlineAttributes(), RFModelMaker.Constants.ATTR_SIZE); 
		testingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
		
		ArrayList<AirlineMapperValue> Orig_listOfAMVs = new ArrayList<AirlineMapperValue>();
		ArrayList<AirlineMapperValue> Dest_listOfAMVs = new ArrayList<AirlineMapperValue>();
		
		for(AirlineMapperValue amv : listOfAMVs){
			if(amv.getConnectionLink().toString().equalsIgnoreCase(AirlineMapperValue.CONN_TYPE_ORIGIN)){
				Orig_listOfAMVs.add(new AirlineMapperValue(amv));	
			} else {
				Dest_listOfAMVs.add(new AirlineMapperValue(amv));
			}
		}
		
		for (AirlineMapperValue incomming_amv : Dest_listOfAMVs){
		
			Instance inst = new DenseInstance(RFModelMaker.Constants.ATTR_SIZE);
			inst.setDataset(testingInstances);
			inst.setValue(0, (double) incomming_amv.getCrsArrTime().get());
			inst.setValue(1, (double) incomming_amv.getCrsDepTime().get());
			inst.setValue(2, (double) incomming_amv.getQuarter().get());
			inst.setValue(3, (double) incomming_amv.getOriginAirport().toString().hashCode());
			inst.setValue(4, (double) incomming_amv.getDestAirport().toString().hashCode());
			inst.setValue(5, (double) incomming_amv.getCarrier().toString().hashCode());
			inst.setValue(6, (double) incomming_amv.getDayOfMonth().get());
			inst.setValue(7, (double) incomming_amv.getDayOfWeek().get());
			inst.setValue(8, (double) incomming_amv.getDistanceGroup().get());
			inst.setValue(9, (double) incomming_amv.getIsHoliday().get());

			inst.setValue(RFModelMaker.Constants.PRED_CLASS_INDEX, Double.NaN);
			
			boolean g_amv_isDelayed;
			try {
				g_amv_isDelayed = (rfClassifier.classifyInstance(inst) == 1.0 ? true : false);
			} catch (Exception e) {
				System.err.println("Unable to predict for flight ");
				e.printStackTrace();
				continue;
			}
			
			if(!g_amv_isDelayed){
				for (AirlineMapperValue outgoing_amv : Orig_listOfAMVs) {
					if (isConnection(outgoing_amv, incomming_amv)) {
						
						double sumDuration = Integer.parseInt(incomming_amv.getCrsElapsedTime().toString()) + Integer.parseInt(outgoing_amv.getCrsElapsedTime().toString());
						context.write(new Text(incomming_amv.getFlNum() + "/t" + outgoing_amv.getFlNum()), new Text(incomming_amv.getCarrierId() + "\t" + sumDuration));
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