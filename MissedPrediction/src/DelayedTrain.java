import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.AirlineMapperValue;

public class DelayedTrain {
	
	
	public static void main(String[] args) {
		
		String runType = args[0];
		String inputTrainPath = args[1];
		String inputTestPath = args[2];
		String outputPath = args[3];
		String rfModel = args[4];
		String predictionMode = args[5];
		String validationFile = args[6];
		
		Configuration conf = new Configuration();
		
		conf.set("rfModelLocation", rfModel);
		conf.set("testLocation", inputTestPath);
		conf.set("outputLocation", outputPath);
		
		try {
			Job job = Job.getInstance(conf);
			job.setJobName("DelayedTrain");
			job.setJarByClass(DelayedTrain.class);
			
			job.setMapperClass(DelayedTrainMapper.class);
			job.setReducerClass(DelayedTrainReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(AirlineMapperValue.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);			
			FileInputFormat.addInputPath(job, new Path(inputTrainPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			if(job.waitForCompletion(true)){
				System.exit(0);
			}
			
		} catch (IllegalArgumentException | IOException e) {
			System.err.println("DELAYED TRAIN CLASS");
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("DELAYED TRAIN CLASS");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.err.println("DELAYED TRAIN CLASS");
			e.printStackTrace();
		}
	
	}
}
