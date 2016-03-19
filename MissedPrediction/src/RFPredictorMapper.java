import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utils.AirlineMapperValue;
import utils.FileRecord;

public class RFPredictorMapper extends Mapper<Object, Text, Text, AirlineMapperValue> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String fileEntry = value.toString();
		fileEntry = fileEntry.replaceAll("\"", "");
		String correctedString = fileEntry.replaceAll(", ", ":");
		String entryNumber = correctedString.substring(0, correctedString.indexOf(","));
		correctedString = correctedString.substring(correctedString.indexOf(",")+1);
		String[] fields = correctedString.split(",");
		
		if(!FileRecord.isTestRecordValid(fields)){
			return;
		}
		
		String monthKey = FileRecord.getValueOf(fields,FileRecord.Field.MONTH);
		
		String flightDate = FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE);
		AirlineMapperValue amv;
		
		try {
			amv = new AirlineMapperValue(
				new IntWritable(Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.CRS_ARR_TIME))),
				new IntWritable(Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.CRS_DEP_TIME))), 
				new IntWritable(Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.QUARTER))),
				new Text(FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN)), 
				new Text(FileRecord.getValueOf(fields, FileRecord.Field.DEST)), 
				new Text(FileRecord.getValueOf(fields, FileRecord.Field.CARRIER)), 
				new IntWritable(Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.FL_NUM))), 
				new IntWritable(Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.DAY_OF_MONTH))),
				new IntWritable(Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.DAY_OF_WEEK))),
				new IntWritable(Integer.parseInt(FileRecord.getValueOf(fields, FileRecord.Field.DISTANCE_GROUP))),
				new DoubleWritable( 0 ),
				new Text(flightDate),
				new IntWritable(FileRecord.dateIsAroundHoliday(flightDate, 2)));
		} catch (NumberFormatException e) {
			return;
		}
		
		context.write(new Text(monthKey), amv);
		
	}
	
}
