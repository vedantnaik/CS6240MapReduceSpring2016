import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utils.AirlineMapperValue;
import utils.FileRecord;
import utils.RFModelMaker;
import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomForest;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class RFPredictorReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
	
		Classifier rfClassifier = null;
		try {
			 rfClassifier = RFModelMaker.getMonthModelFromFileSystem(context, key);
			 if(rfClassifier == null) {System.err.println("CLASSIFIER MISSING!! month : " + key.toString()); return;}
		} catch (Exception e) {
			System.err.println("Problem reading the model for month " + key.toString());
			e.printStackTrace();
			return;
		}
		
		RFModelMaker rfModel = new RFModelMaker();
		Instances testingInstances = new Instances("Testing", rfModel.getAirlineAttributes(), RFModelMaker.Constants.ATTR_SIZE); 
		testingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
		
		for (AirlineMapperValue eachAMVref : listOfAMVs){
			AirlineMapperValue amv = new AirlineMapperValue(eachAMVref);
			
			String[] dateParts = amv.getFlDate().toString().split("-");
			String yearVal = dateParts[0];
			String monthVal = dateParts[1];
			
			Instance inst = RFModelMaker.getTestingInstance(amv, yearVal, monthVal, testingInstances);
			
			/*Instance inst = new DenseInstance(RFModelMaker.Constants.ATTR_SIZE);
			inst.setDataset(testingInstances);
			inst.setValue(0, (double) amv.getCrsArrTime().get());
			inst.setValue(1, (double) amv.getCrsDepTime().get());
			inst.setValue(2, (double) amv.getQuarter().get());
			inst.setValue(3, (double) yearVal.hashCode());
			inst.setValue(4, (double) monthVal.hashCode());
			inst.setValue(5, (double) amv.getFlNum().get());
			inst.setValue(6, (double) amv.getOriginAirport().toString().hashCode());
			inst.setValue(7, (double) amv.getDestAirport().toString().hashCode());
			inst.setValue(8, (double) amv.getCarrier().toString().hashCode());
			inst.setValue(9, (double) amv.getDayOfMonth().get());
			inst.setValue(10, (double) amv.getDayOfWeek().get());
			inst.setValue(11, (double) amv.getDistanceGroup().get());
			inst.setValue(12, (double) amv.getIsHoliday().get());
			inst.setValue(RFModelMaker.Constants.PRED_CLASS_INDEX, Double.NaN);*/
			
			String uniqueFlightKey = amv.getFlNum().get()
								+"_"+ amv.getFlDate().toString()
								+"_"+ FileRecord.makeCompleteHHMM(amv.getCrsDepTime().get() + "");
			
			String predValString;
			try {
				predValString = (rfClassifier.classifyInstance(inst) == 1.0 ? "TRUE" : "FALSE");
			} catch (Exception e) {
				System.err.println("Unable to predict for flight " + uniqueFlightKey);
				e.printStackTrace();
				continue;
			}
			
			context.write(new Text(uniqueFlightKey), new Text(predValString));
			
		}
	
	}
	
}
