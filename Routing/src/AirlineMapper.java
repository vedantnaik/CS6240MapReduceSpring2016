import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
			
			/*System.out.println("--------------------------------------------------");
			System.out.println("------------------ENTERED MAPPER------------------");
			System.out.println("--------------------------------------------------");
			
			System.out.println("--------------------------------------------------");
			System.out.println("-----------------CHECKING RECORDS------------------");
			System.out.println("--------------------------------------------------");
			*/
			
			if (FileRecord.csvHeaders.size() + 1 == fields.length && FileRecord.isTestRecordValid(fields)){
				
				String carMonthKey = FileRecord.getValueOf(fields,FileRecord.Field.MONTH);
				
				String flightDate = FileRecord.getValueOf(fields, FileRecord.Field.FL_DATE);
				String[] fldate = flightDate.split("-");
				//String year = fldate[0];
				//String month = fldate[1];
				//String day = fldate[2];
				String year = FileRecord.getValueOf(fields, FileRecord.Field.YEAR);
				String month = FileRecord.getValueOf(fields, FileRecord.Field.MONTH);
				String day = FileRecord.getValueOf(fields, FileRecord.Field.DAY_OF_MONTH);
				long crsArrTime; 	
				long crsDepTime; 
				long actualArrTime;
				long actualDepTime;
				int flNum;
				String distance;
				String originId;
				String destinationId;
				String carrierId;
				String crsElapsedTime;
				String dest;
				String origin;
				
				try {
					// Store java date in long so that it will help in finding connections that span over days and months
					crsArrTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.CRS_ARR_TIME));
					crsDepTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.CRS_DEP_TIME));
					//actualArrTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.ARR_TIME));
					//actualDepTime = getJavaDateInLong(year, month, day, FileRecord.getValueOf(fields, FileRecord.Field.DEP_TIME));
				
					flNum = Integer.parseInt((FileRecord.getValueOf(fields, FileRecord.Field.FL_NUM)));
					
					
				} catch (ParseException pe) {
					System.err.println("Unable to create long date for a sane record!!");
					return;
				} catch (StringIndexOutOfBoundsException siobe) {
					System.err.println("Some value missing in sane record, although handeled in sanity check!!");
					return;
				}
				
				flNum = Integer.parseInt((FileRecord.getValueOf(fields, FileRecord.Field.FL_NUM)));
				distance = FileRecord.getValueOf(fields, FileRecord.Field.DISTANCE);
				destinationId = (FileRecord.getValueOf(fields, FileRecord.Field.DEST_AIRPORT_ID));
				carrierId = (FileRecord.getValueOf(fields,FileRecord.Field.CARRIER));
				crsElapsedTime = FileRecord.getValueOf(fields, FileRecord.Field.CRS_ELAPSED_TIME);
				dest = FileRecord.getValueOf(fields, FileRecord.Field.DEST);
				origin = FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN);
				
				// make for dest
				AirlineMapperValue amvDest = new AirlineMapperValue(
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
						//new DoubleWritable( Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.ARR_DELAY)) > 0 ? 1 : 0 ),
						new Text(flightDate),
						new IntWritable(FileRecord.dateIsAroundHoliday(flightDate, 2)),
						new Text(AirlineMapperValue.CONN_TYPE_DEST), 
						new LongWritable(crsArrTime), 
						new LongWritable(crsDepTime), 
						new Text(distance),  
						new Text(carrierId), 
						new Text(day), 
						new Text(month), 
						new Text(crsElapsedTime),
						new Text(dest),
						new Text(origin),
						new Text(year));
							
				String carDestYearMonthKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + "\t" + 
						FileRecord.getValueOf(fields, FileRecord.Field.DEST) + "\t" +
						FileRecord.getValueOf(fields, FileRecord.Field.YEAR) + "\t" +
						FileRecord.getValueOf(fields, FileRecord.Field.MONTH);
				
				context.write(new Text(carDestYearMonthKey), amvDest);
				
				// make for origin
				AirlineMapperValue amvOrig = new AirlineMapperValue(
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
						//new DoubleWritable( Double.parseDouble(FileRecord.getValueOf(fields, FileRecord.Field.ARR_DELAY)) > 0 ? 1 : 0 ),
						new Text(flightDate),
						new IntWritable(FileRecord.dateIsAroundHoliday(flightDate, 2)),
						new Text(AirlineMapperValue.CONN_TYPE_ORIGIN), 
						new LongWritable(crsArrTime), 
						new LongWritable(crsDepTime), 
						new Text(distance), 
						new Text(carrierId), 
						new Text(day), 
						new Text(month), 
						new Text(crsElapsedTime),
						new Text(dest),
						new Text(origin),
						new Text(year));
							
				String carOrigYearMonthKey = FileRecord.getValueOf(fields, FileRecord.Field.CARRIER) + "\t" + 
						FileRecord.getValueOf(fields, FileRecord.Field.ORIGIN) + "\t" +
						FileRecord.getValueOf(fields, FileRecord.Field.YEAR) + "\t" +
						FileRecord.getValueOf(fields, FileRecord.Field.MONTH);
				
				//System.out.println("--------------------AT CONTEXT.WRITE------------------");
				//System.out.println("key carrier -> " + (FileRecord.getValueOf(fields, FileRecord.Field.CARRIER).toString()));
				//System.out.println("value -> " + crsDepTime + " " + crsArrTime);
				
				context.write(new Text(carOrigYearMonthKey), amvOrig);
				
			}
		}
		
		// inspired by: http://stackoverflow.com/questions/3056703/simpledateformat
		private long getJavaDateInLong(String year, String month, String day, String HHMM) throws ParseException{
			
			HHMM = FileRecord.makeCompleteHHMM(HHMM);
			
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss");
			
			String str1 = year + "-" + month + "-" + day + "T" + HHMM.substring(0, 2) + ":" + HHMM.substring(2, 4) +":00";
			Date date1 = sf.parse(str1);
			return date1.getTime();
		}
	}


