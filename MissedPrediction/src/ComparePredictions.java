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
	    		if(validationEntryString.length() == 0) {continue;}
	    		String validationEntry[] = validationEntryString.split(",");
	    		if(FileRecord.isValidationRecordValid(validationEntry)){
	    			validationMap.put(validationEntry[0], validationEntry[1].equalsIgnoreCase("TRUE"));	    			
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

	    	// debug
	    	long DEBUG_partFileCount = 0;
	    	HashMap<String, Boolean> DEBUG_predMap = new HashMap<String, Boolean>();
	    	ArrayList<String> DEBUG_keysNotInVal = new ArrayList<String>();
	    	
	    	File predDir = new File(predictionFolder);

			for (File predPartFile : predDir.listFiles()) {
				System.out.println("Reading predictions file : " + predPartFile.toString());
		    	
				FileInputStream partFileInputStream = new FileInputStream(predPartFile);
		    	Reader partFileDecoder = new InputStreamReader(partFileInputStream, "ASCII");
		    	BufferedReader partFileBuffered = new BufferedReader(partFileDecoder);

		    	String partFIleEntryString;
		    	
		    	while((partFIleEntryString = partFileBuffered.readLine()) != null){
		    		String predictionEntry[] = partFIleEntryString.split("\t");
		    		String uniqueFlightKey = predictionEntry[0];
		    		boolean flightDelayPrediction = predictionEntry[1].equalsIgnoreCase("TRUE");
		    		
		    		if(!validationMap.containsKey(uniqueFlightKey)){
		    			DEBUG_keysNotInVal.add(uniqueFlightKey);
		    		}
		    		
		    		DEBUG_partFileCount++;
		    		DEBUG_predMap.put(uniqueFlightKey, flightDelayPrediction);
		    	}		    	
		    	
		    	partFileBuffered.close();
		    	partFileDecoder.close();
		    	partFileInputStream.close();
			}
			
			
			System.out.println("Calculating confusion matrix..");
			long recordCountNotInPred = 0;
			
			for (String pKey : validationMap.keySet()){
					
    			boolean flightDelayActualValue = validationMap.get(pKey);
    			boolean flightDelayPrediction = (DEBUG_predMap.containsKey(pKey) ? DEBUG_predMap.get(pKey) : false); 
    			
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
			
			double recall = (double) truePositive / (truePositive + falseNegative) * 100;
			double precision = (double) truePositive / (truePositive + falsePositive) * 100;
			double accuracy = (double) (truePositive + trueNegative) / (truePositive + falsePositive + trueNegative + falseNegative) * 100;
			
			System.out.println("Final Result...");
			System.out.println("Precision : "+ precision + " %");
			System.out.println("Recall : "+ recall + " %");
			System.out.println("Accuracy : "+ accuracy + " %");
			
			System.out.println("True Positive: " + truePositive);
			System.out.println("False Negative: " + falseNegative);
			System.out.println("True Negative: " + trueNegative);
			System.out.println("False Positive: " + falsePositive);
			
			////////////////////////////
			System.out.println("\nvalidationMap size : " + validationMap.size());
			System.out.println("DEBUG_partFileCount  : " + DEBUG_partFileCount );
			System.out.println("DEBUG_predMap size " + DEBUG_predMap.size());
			System.out.println("COUNT of records not in pred: " + recordCountNotInPred + "\n");
			////////////////////////////
			
			PrintWriter writer = new PrintWriter("evalOutput/confusionMatrix.txt", "UTF-8");
			writer.println("True Positive: " + truePositive);
			writer.println("False Negative: " + falseNegative);
			writer.println("True Negative: " + trueNegative);
			writer.println("False Positive: " + falsePositive);
			
			writer.println("Precision :"+ precision + "%");
			writer.println("Recall :"+ recall + "%");
			writer.println("Accuracy :"+ accuracy + "%");
			
			writer.close();
			
		} catch (IOException e) {
			System.err.println("Unable to read file");
			e.printStackTrace();
			System.exit(1);
		}
		
		
	
	}
	
}
