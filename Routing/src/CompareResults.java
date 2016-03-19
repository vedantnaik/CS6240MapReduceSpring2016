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

public class CompareResults {
	public static void main(String[] args) {
			
		//String predictionFolder = args[3];
		//String validationFile = args[6];
		String predictionFolder = "/Users/rohanjoshi/Documents/outputRequest";
		String validationFile = "/Users/rohanjoshi/Documents/04missed.csv.gz";
		BufferedReader validationBuffered = null;
		try {
			// CHECK AGAINST PREDICTIONS
	    	System.out.println("Begin reading predictions file(s)...");
	    	
	    	// debug
	    	ArrayList<String> predList = new ArrayList<String>();
	    	System.out.println(predictionFolder);
	    	File predDir = new File(predictionFolder);
	    	
	    	System.out.println(predDir.toString());
	    	
			for (File predPartFile : predDir.listFiles()) {
				System.out.println("Reading predictions file : " + predPartFile.toString());
		    	
				FileInputStream partFileInputStream = new FileInputStream(predPartFile);
		    	Reader partFileDecoder = new InputStreamReader(partFileInputStream, "ASCII");
		    	BufferedReader partFileBuffered = new BufferedReader(partFileDecoder);

		    	String partFIleEntryString;
		    	
		    	while((partFIleEntryString = partFileBuffered.readLine()) != null){
		    		String predictionEntry[] = partFIleEntryString.split("\t");
		    		String requestRecordCsv = predictionEntry[0];
//		    		year+","+month+","+day+","+origin+","+dest+","+0
		    		
		    		String predictedRecordCsv = predictionEntry[1];
//		    		/year+","+month+","+day+","+origin+","+dest+","+flnum1+","+flnum2+","+duration;
		    		
		    		String predictionInValidationFormat = predictedRecordCsv.substring(0, predictedRecordCsv.lastIndexOf(","));
		    		predList.add(predictionInValidationFormat);
		    	}		    	
		    	
		    	partFileBuffered.close();
		    	partFileDecoder.close();
		    	partFileInputStream.close();
			}
			
			
			
			
			
			
			// READ VALIDAITON DATA
			System.out.println("Begin reading validation file...");
			
			long incorrectSuggestions = 0;
			
			InputStream fileStream = new FileInputStream(validationFile);
	    	InputStream gzipStream = new GZIPInputStream(fileStream);
	    	Reader decoder = new InputStreamReader(gzipStream, "ASCII");
	    	validationBuffered = new BufferedReader(decoder);

	    	String validationEntryString;
	    	
	    	while((validationEntryString = validationBuffered.readLine())!=null){
	    		if(validationEntryString.length() == 0) {continue;}
	    		
	    		if (predList.contains(validationEntryString)){
	    			incorrectSuggestions++;
	    		}
	    		
	    		
	    	}
	    	
	    	validationBuffered.close();
	    	decoder.close();
	    	gzipStream.close();
	    	fileStream.close();
	    	
	    	
	    	System.out.println("The program suggested " + incorrectSuggestions + " such connections that were actually missed.");
	    	
	    	
		} catch (IOException e) {
			System.err.println("Unable to read file");
			e.printStackTrace();
			System.exit(1);
		}
	
	}
	
}
