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
		
			Instances trainingInstances = new Instances("Training", RFModelMaker.getAirlineAttributes(), 11); 
			trainingInstances.setClassIndex(11);
			
			System.out.println("Reducer for " + key.toString());
			
			for (AirlineMapperValue eachAMVref : listOfAMVs){
				AirlineMapperValue amv = new AirlineMapperValue(eachAMVref);
				
				Instance inst = new DenseInstance(13);
				inst.setDataset(trainingInstances);
				inst.setValue(1, amv.getCrsArrTime().get());
				inst.setValue(2, amv.getCrsDepTime().get());
				inst.setValue(3, amv.getQuarter().toString());
				inst.setValue(4, amv.getOriginAirport().toString());
				inst.setValue(5, amv.getDestAirport().toString());
				inst.setValue(6, amv.getCarrier().toString());
				inst.setValue(7, amv.getDayOfMonth().get());
				inst.setValue(8, amv.getDayOfWeek().get());
				inst.setValue(9, amv.getDistanceGroup().get());
				inst.setValue(10, amv.getIsHoliday().get());
	
				inst.setValue(RFModelMaker.Constants.PRED_CLASS_INDEX, amv.getArrDelay().get());
				
				trainingInstances.add(inst);
			}
			
			trainingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
			
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
			
			writeModelToFileSystem(rfClassifer, context, key); 
			
		}

		private void writeModelToFileSystem(RandomForest rfClassifer, Reducer<Text, AirlineMapperValue, Text, Text>.Context context, Text key) throws IOException {
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