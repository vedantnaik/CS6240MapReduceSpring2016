import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

public class ComparePredictions {

	public static void main(String[] args) {
		
		long chutValidCount = 0;
		
		String predictionFolder = args[3];
		String validationFile = args[6];
		
		HashMap<String, Boolean> validationMap = new HashMap<String, Boolean>();
		
		BufferedReader validationBuffered = null;
		try {
			// READ VALIDAITON DATA
			System.out.println("Begin reading validation file...");
			InputStream fileStream = new FileInputStream(validationFile);
	    	InputStream gzipStream = new GZIPInputStream(fileStream);
	    	Reader decoder = new InputStreamReader(gzipStream, "ASCII");
	    	validationBuffered = new BufferedReader(decoder);

	    	String validationEntryString;
	    	
	    	while((validationEntryString = validationBuffered.readLine())!=null){
	    		String validationEntry[] = validationEntryString.split(",");
	    		if(FileRecord.isValidationRecordValid(validationEntry)){
	    			validationMap.put(validationEntry[0], validationEntry[1].equalsIgnoreCase("TRUE"));	    			
	    		} else {
	    			chutValidCount++;
	    		}
	    		
	    	}
	    	
	    	validationBuffered.close();
	    	decoder.close();
	    	gzipStream.close();
	    	fileStream.close();
	    	
	    	// CHECK AGAINST PREDICTIONS
	    	System.out.println("Begin reading predictions file(s)...");
	    	
	    	long truePositive = 0;
	    	long falseNegative = 0;
	    	long falsePositive = 0;
	    	long trueNegative = 0;

	    	// debig
	    	long DEBUG_validationNotFoundCount = 0;
	    	long DEBUG_partFileCount = 0;
	    	HashMap<String, Boolean> DEBUG_predMap = new HashMap<String, Boolean>();
	    	ArrayList<String> keysNotInVal = new ArrayList<String>();
	    	
	    	File predDir = new File(predictionFolder);

			for (File predPartFile : predDir.listFiles()) {
				System.out.println("Reading predictions file : " + predPartFile.toString());
		    	
				FileInputStream partFileInputStream = new FileInputStream(predPartFile);
		    	Reader partFileDecoder = new InputStreamReader(partFileInputStream, "ASCII");
		    	BufferedReader partFileBuffered = new BufferedReader(partFileDecoder);

		    	String partFIleEntryString;
		    	
		    	while((partFIleEntryString = partFileBuffered.readLine())!=null){
		    		
//		    		System.out.println(partFIleEntryString);
		    		String predictionEntry[] = partFIleEntryString.split("\t");
		    		String uniqueFlightKey = predictionEntry[0];
		    		boolean flightDelayPrediction = predictionEntry[1].equalsIgnoreCase("TRUE");
		    		
		    		if(!validationMap.containsKey(uniqueFlightKey)){
		    			keysNotInVal.add(uniqueFlightKey);
		    		}
		    		
		    		DEBUG_partFileCount++;
		    		DEBUG_predMap.put(uniqueFlightKey, flightDelayPrediction);
		    		
		    	}		    	
		    	
		    	partFileBuffered.close();
		    	partFileBuffered.close();
		    	partFileInputStream.close();
			}
			
			
			System.out.println("Calculating confusion matrix..");
			ArrayList<String> keysNotInPred = new ArrayList<String>();
			
			for (String pKey : DEBUG_predMap.keySet()){
				if (validationMap.containsKey(pKey)){
	    			boolean flightDelayActualValue = validationMap.get(pKey);
	    			boolean flightDelayPrediction = DEBUG_predMap.get(pKey); 
	    			
	    			if (flightDelayActualValue){
	    				if (flightDelayPrediction){
	    					truePositive++;
	    				} else {
	    					falseNegative++;
	    				}
	    			} else {
	    				if (flightDelayPrediction){
	    					falsePositive++;
	    				} else {
	    					trueNegative++;
	    				}
	    			}
	    		}				
			}
			
//			System.out.println("Print missing common keys (if any) with  >>>...");
//			for (String pkey : keysNotInPred){
//				if (keysNotInVal.contains(pkey)){
//					System.out.println(">>> " + pkey);
//				}
//			}
			
			
			double recall = (double) truePositive / (truePositive + falseNegative);
			double precision = (double) truePositive / (truePositive + falsePositive);
			double accuracy = (double) (truePositive + trueNegative) / (truePositive + falsePositive + trueNegative + falseNegative);
			
			System.out.println("Final Result...");
			System.out.println("Precision : "+ precision);
			System.out.println("Recall : "+ recall);
			System.out.println("Accuracy : "+ accuracy);
			
			System.out.println("True Positive: " + truePositive);
			System.out.println("False Negative: " + falseNegative);
			System.out.println("True Negative: " + trueNegative);
			System.out.println("False Positive: " + falsePositive);
			
			////////////////////////////
			System.out.println("DEBUG_validationNotFoundCount : " + DEBUG_validationNotFoundCount);
			System.out.println("validationMap size : " + validationMap.size());
			System.out.println("DEBUG_partFileCount  : " + DEBUG_partFileCount );
			System.out.println("DEBUG_predMap size " + DEBUG_predMap.size());
			System.out.println("chutValidCount : " + chutValidCount);
			////////////////////////////
			
			PrintWriter writer = new PrintWriter("evalOutput/confusionMatrix.txt", "UTF-8");
			writer.println("True Positive: " + truePositive);
			writer.println("False Negative: " + falseNegative);
			writer.println("True Negative: " + trueNegative);
			writer.println("False Positive: " + falsePositive);
			
			writer.println("Precision :"+ precision);
			writer.println("Recall :"+ recall);
			writer.println("Accuracy :"+ accuracy);
			
			writer.close();
			
		} catch (IOException e) {
			System.err.println("Unable to read file");
			e.printStackTrace();
			System.exit(1);
		}
		
		
	
	}
	
}
