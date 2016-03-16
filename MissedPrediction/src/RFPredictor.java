import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import au.com.bytecode.opencsv.CSVReader;
import weka.classifiers.Classifier;
import weka.core.Instances;

public class RFPredictor {

	public static void main(String[] args) {
		
		
		String modelFolder = args[0];
		String testInputFile = args[1];
		String outputFolder = args[2];
		HashMap<String, Classifier> storedClassifier;
		
		
		try {
			storedClassifier = RFModelMaker.getModelsFromFileSystem(modelFolder);
//			System.out.println(storedClassifier.toString());
		} catch (Exception e) {
			System.err.println("Unable to read stored models");
			e.printStackTrace();
			System.exit(1);
		}
		
		BufferedReader buffered = null;
		try {
			InputStream fileStream = new FileInputStream(testInputFile);
	    	InputStream gzipStream = new GZIPInputStream(fileStream);
	    	Reader decoder = new InputStreamReader(gzipStream, "ASCII");
	    	buffered = new BufferedReader(decoder);
		} catch (IOException e) {
			System.err.println("Unable to read test input file");
			e.printStackTrace();
			System.exit(1);
		}
		
		
		RFModelMaker rfModel = new RFModelMaker();
		Instances testingInstances = new Instances("Testing", rfModel.getAirlineAttributes(), RFModelMaker.Constants.ATTR_SIZE); 
		testingInstances.setClassIndex(RFModelMaker.Constants.PRED_CLASS_INDEX);
		
		HashMap<String, Boolean> predictionOutput = new HashMap<String, Boolean>();
		String fileEntryString;
		
		try {
			while((fileEntryString = buffered.readLine())!=null){
				
				String fileEntry = fileEntryString.toString();
				fileEntry = fileEntry.replaceAll("\"", "");
				String correctedString = fileEntry.replaceAll(", ", ":");
				correctedString = correctedString.substring(correctedString.indexOf(",")+1);
				String[] fields = correctedString.split(",");
				
				System.out.println(correctedString);
			}
		} catch (IOException e) {
			System.err.println("Unable to read an entry from the file : " + testInputFile);
			e.printStackTrace();
		}
	
	
	}
	
}
