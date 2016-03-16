import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AirlineMapper extends Mapper<Object, Text, Text, AirlineMapperValue> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String fileEntry = value.toString();
			fileEntry = fileEntry.replaceAll("\"", "");
			String correctedString = fileEntry.replaceAll(", ", ":");
			String[] fields = correctedString.split(",");
			
			if (FileRecord.csvHeaders.size() == fields.length && FileRecord.isRecordValid(fields)){
				
				String carMonthKey = FileRecord.getValueOf(fields,FileRecord.Field.MONTH);
				
				String flightDate = FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE);
				
				AirlineMapperValue amv = new AirlineMapperValue(
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
						new DoubleWritable( Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.ARR_DELAY)) > 0 ? 1 : 0 ),
						new Text(flightDate),
						new IntWritable(FileRecord.dateIsAroundHoliday(flightDate, 2)));
							
				context.write(new Text(carMonthKey), amv);
			}
		}
	}