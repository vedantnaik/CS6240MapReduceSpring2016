import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.AirlineMapperValue;
import utils.RFModelMaker;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.trees.RandomForest;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class DelayedTrainReducer extends Reducer<Text, AirlineMapperValue, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<AirlineMapperValue> listOfAMVs, Context context) throws IOException, InterruptedException {
		
			RFModelMaker rfModel = new RFModelMaker();
			Instances trainingInstances = new Instances("Training", rfModel.getAirlineAttributes(), RFModelMaker.Constants.ATTR_SIZE); 
			trainingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
			
			System.out.println("Reducer for " + key.toString());
			
			for (AirlineMapperValue eachAMVref : listOfAMVs){
				AirlineMapperValue amv = new AirlineMapperValue(eachAMVref);
	
				String[] dateParts = amv.getFlDate().toString().split("-");
				String yearVal = dateParts[0];
				String monthVal = dateParts[1];
				
				Instance inst = RFModelMaker.getTrainingInstance(amv, yearVal, monthVal, trainingInstances);
				
				/*// TODO: amv, month, year : take and create instance from rfmodelhelper
				Instance inst = new DenseInstance(RFModelMaker.Constants.ATTR_SIZE);
				inst.setDataset(trainingInstances);
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
	
				inst.setValue(RFModelMaker.Constants.PRED_CLASS_INDEX, amv.getArrDelay().get()+"");*/
	
				trainingInstances.add(inst);
				inst = null;
				amv = null;
			}
			
			//trainingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
			
			RandomForest rfClassifer = new RandomForest();
			try {
				rfClassifer.setMaxDepth(7);
				rfClassifer.setNumTrees(7);
				
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
			rfClassifer = null;
		}

	}