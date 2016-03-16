import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.trees.RandomForest;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

public class AirlineDelayedPredictionsReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
		
			RFModelMaker rfModel = new RFModelMaker();
			Instances trainingInstances = new Instances("Training", rfModel.getAirlineAttributes(), RFModelMaker.Constants.ATTR_SIZE); 
			trainingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
			
			System.out.println("Reducer for " + key.toString());
			
			for (AirlineMapperValue eachAMVref : listOfAMVs){
				AirlineMapperValue amv = new AirlineMapperValue(eachAMVref);
				
				Instance inst = new DenseInstance(11);
				inst.setDataset(trainingInstances);
				inst.setValue(0, (double) amv.getCrsArrTime().get());
				inst.setValue(1, (double) amv.getCrsDepTime().get());
				inst.setValue(2, (double) amv.getQuarter().get());
				inst.setValue(3, (double) amv.getOriginAirport().toString().hashCode());
				inst.setValue(4, (double) amv.getDestAirport().toString().hashCode());
				inst.setValue(5, (double) amv.getCarrier().toString().hashCode());
				inst.setValue(6, (double) amv.getDayOfMonth().get());
				inst.setValue(7, (double) amv.getDayOfWeek().get());
				inst.setValue(8, (double) amv.getDistanceGroup().get());
				inst.setValue(9, (double) amv.getIsHoliday().get());
	
				inst.setValue(RFModelMaker.Constants.PRED_CLASS_INDEX, amv.getArrDelay().get()+"");
				
				trainingInstances.add(inst);
			}
			
			//trainingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
			
			RandomForest rfClassifer = new RandomForest();
			try {
				rfClassifer.setMaxDepth(5);
//				rfClassifer.setDebug(false);
				rfClassifer.setNumTrees(12);
				
				rfClassifer.buildClassifier(trainingInstances);
				
			} catch (Exception e) {
				System.err.println("Unable to build random forest classifier:");
				e.printStackTrace();
			}
			
			try {
				RFModelMaker.writeModelToFileSystem(rfClassifer, context, key);
			} catch (Exception e) {
				System.err.println("Unable to write model to folder.");
				e.printStackTrace();
			}
			
		}

	}