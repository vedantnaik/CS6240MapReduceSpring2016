import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVReader;
import utils.AirlineMapperValue;
import weka.classifiers.Classifier;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class RFPredictor {
	public static void main(String[] args) {
		
		String runType = args[0];
		String inputTrainPath = args[1];
		String inputTestPath = args[2];
		String outputPath = args[3];
		String rfModel = args[4];
		String train_or_test = args[5];
		
		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();
		
		conf.set("rfModelLocation", rfModel);
		conf.set("testLocation", inputTestPath);
		conf.set("outputLocation", outputPath);
		
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("RFPredictor");
			job.setJarByClass(RFPredictor.class);
			
			job.setMapperClass(RFPredictorMapper.class);
			job.setReducerClass(RFPredictorReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(AirlineMapperValue.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(inputTestPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));			
			
			if(job.waitForCompletion(true)){
				System.exit(0);
			}
		
		} catch (IllegalArgumentException | IOException e) {
			System.err.println("RF PREDICTOR CLASS");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("RF PREDICTOR CLASS");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.err.println("RF PREDICTOR CLASS");
			e.printStackTrace();
		}
	
	}
}
