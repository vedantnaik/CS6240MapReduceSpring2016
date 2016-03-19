import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AirlineMapperValue implements Writable {
	
	public static final String CONN_TYPE_ORIGIN = "ORIGIN"; 
	public static final String CONN_TYPE_DEST = "DESTINATION";
	
	IntWritable crsArrTime;
	IntWritable crsDepTime;
	IntWritable quarter;
	Text originAirport;
	Text destAirport;
	Text carrier;
	IntWritable flNum;
	IntWritable dayOfMonth;
	IntWritable dayOfWeek;
	IntWritable distanceGroup;	
	DoubleWritable arrDelay;
	Text flDate;
	IntWritable isHoliday;
	
	Text connectionLink;	// ORIGIN or DESTINATION
	
	LongWritable crsArrTime_long;
	LongWritable crsDepTime_long;
	
	Text distance;
	Text carrierId;
	Text day;
	Text month;
	Text crsElapsedTime;
	Text dest;
	Text origin;
	
	
	public AirlineMapperValue(){
		this.crsArrTime = new IntWritable();
		this.crsDepTime = new IntWritable();

		this.quarter = new IntWritable();
		
		this.originAirport = new Text();
		this.destAirport = new Text();
		this.carrier = new Text();
		this.flNum = new IntWritable();
		
		this.dayOfMonth = new IntWritable();
		this.dayOfWeek = new IntWritable();
		this.distance = new Text();
		this.carrierId = new Text();
		this.day = new Text();
		this.month = new Text();
		this.crsElapsedTime = new Text();
		this.distanceGroup = new IntWritable();
		this.connectionLink = new Text();
		this.crsArrTime_long = new LongWritable();
		this.crsDepTime_long = new LongWritable();
		//this.arrDelay = new DoubleWritable();
	
		this.flDate = new Text();
		this.isHoliday = new IntWritable();
		this.dest = new Text();
		this.origin = new Text();
	}
	
// removed: ARR_DELAY
	public AirlineMapperValue(IntWritable crsArrTime, IntWritable crsDepTime, IntWritable quarter,
			Text originAirport, Text destAirport, Text carrier, IntWritable flNum, IntWritable dayOfMonth,
			IntWritable dayOfWeek, IntWritable distanceGroup, Text flDate, 
			IntWritable isHoliday, Text connectionLink, LongWritable crsArrTime_long, LongWritable crsDepTime_long,
			Text distance,  
			Text carrierId, Text day, Text month, Text crsElapsedTime, Text dest, Text origin) {
		super();
		this.crsArrTime = crsArrTime;
		this.crsDepTime = crsDepTime;
		this.quarter = quarter;
		this.originAirport = originAirport;
		this.destAirport = destAirport;
		this.carrier = carrier;
		this.flNum = flNum;
		this.dayOfMonth = dayOfMonth;
		this.dayOfWeek = dayOfWeek;
		//this.distance = distance;
		this.distanceGroup = distanceGroup;
		this.connectionLink = connectionLink;
		//this.arrDelay = arrDelay;
		this.flDate = flDate;
		this.isHoliday = isHoliday;
		this.crsArrTime_long = crsArrTime_long;
		this.crsDepTime_long = crsDepTime_long;
		this.flNum = flNum;
		this.distance = distance;
		this.carrierId = carrierId;
		this.day = day;
		this.month = month;
		this.crsElapsedTime = crsElapsedTime;
		this.dest = dest;
		this.origin = origin;
	}



	public AirlineMapperValue(AirlineMapperValue amv) {
		this.crsArrTime = new IntWritable(amv.getCrsArrTime().get());
		this.crsDepTime = new IntWritable(amv.getCrsDepTime().get());
		this.quarter = new IntWritable(amv.getQuarter().get());
		
		this.originAirport = new Text(amv.getOriginAirport().toString());
		this.destAirport = new Text(amv.getDestAirport().toString());
		this.carrier = new Text(amv.getCarrier().toString());
		
		this.flNum = new IntWritable(amv.getFlNum().get());
		
		this.dayOfMonth = new IntWritable(amv.getDayOfMonth().get());
		this.dayOfWeek = new IntWritable(amv.getDayOfWeek().get());
		//this.distance = new Text(amv.getDistance().toString());
		this.distanceGroup = new IntWritable(amv.getDistanceGroup().get());
		
		//this.arrDelay = new DoubleWritable(amv.getArrDelay().get());

		this.flDate = new Text(amv.getFlDate().toString());
		this.isHoliday = new IntWritable(amv.getIsHoliday().get());
		
		this.connectionLink = new Text(amv.getConnectionLink().toString());
		this.crsArrTime_long = new LongWritable(amv.getCrsArrTime_long().get());
		this.crsDepTime_long = new LongWritable(amv.getCrsDepTime_long().get());
		this.distance = new Text(amv.getDistance().toString());
		this.carrierId = new Text(amv.getCarrierId().toString());
		this.day = new Text(amv.getDay().toString());
		this.month = new Text(amv.getMonth().toString());
		this.crsElapsedTime = new Text(amv.getCrsElapsedTime());
		this.dest = new Text(amv.getDest());
		this.origin = new Text(amv.getOrigin());
	}
	
	@Override
	public void readFields(DataInput inVal) throws IOException {
		crsArrTime.readFields(inVal);
		crsDepTime.readFields(inVal);
		quarter.readFields(inVal);
		
		originAirport.readFields(inVal);
		destAirport.readFields(inVal);
		carrier.readFields(inVal);
		
		flNum.readFields(inVal);
		
		dayOfMonth.readFields(inVal);
		dayOfWeek.readFields(inVal);
		
		distanceGroup.readFields(inVal);
//		distance.readFields(inVal);
		//arrDelay.readFields(inVal);
		
		flDate.readFields(inVal);
		isHoliday.readFields(inVal);
		
		connectionLink.readFields(inVal);
		crsArrTime_long.readFields(inVal);
		crsDepTime_long.readFields(inVal);

		distance.readFields(inVal);
		carrierId.readFields(inVal);
		day.readFields(inVal);
		month.readFields(inVal);
		crsElapsedTime.readFields(inVal);
		dest.readFields(inVal);
		origin.readFields(inVal);
	}

	@Override
	public void write(DataOutput outVal) throws IOException {
		crsArrTime.write(outVal);
		crsDepTime.write(outVal);
		quarter.write(outVal);
		
		originAirport.write(outVal);
		destAirport.write(outVal);
		carrier.write(outVal);
		
		flNum.write(outVal);
		
		dayOfMonth.write(outVal);
		dayOfWeek.write(outVal);
//		distance.write(outVal);
		distanceGroup.write(outVal);
		
		//arrDelay.write(outVal);
		
		flDate.write(outVal);
		isHoliday.write(outVal);
		
		connectionLink.write(outVal);
		crsArrTime_long.write(outVal);
		crsDepTime_long.write(outVal);

		distance.write(outVal);
		carrierId.write(outVal);
		day.write(outVal);
		month.write(outVal);
		crsElapsedTime.write(outVal);
		dest.write(outVal);
		origin.write(outVal);
	}

	@Override
	public String toString() {
		// TODO
		return 
				"["
				+ "crsArr:" + crsArrTime.get() + " "
				+ "crsDep:" + crsDepTime.get() + " "
				+ "actualArr:" + quarter.get() + " "

				+ "originAirportId: " + originAirport.toString() + " "
				+ "destAirportId: " + destAirport.toString() + " "
				+ "carrier: " + carrier.toString() + " "
				
				+ "flNum" + flNum.get() + " "
				
				+ "dayOfMonth: " + dayOfMonth.get() + " "
				+ "dayOfWeek: " + dayOfWeek.get() + " "
				
				+ "distanceGroup: " + distanceGroup.get() + " "
				
				+ "arrDelay: " + arrDelay.get() + " "
				
				+ "flDate: " + flDate.toString() + " "
				+ "isHoliday: " + isHoliday.get() + " "
						
				+ "]";
	}

	public Text getOrigin() {
		return origin;
	}


	public void setOrigin(Text origin) {
		this.origin = origin;
	}


	public Text getDest() {
		return dest;
	}


	public void setDest(Text dest) {
		this.dest = dest;
	}


	public IntWritable getCrsArrTime() {
		return crsArrTime;
	}

	public void setCrsArrTime(IntWritable crsArrTime) {
		this.crsArrTime = crsArrTime;
	}

	public IntWritable getCrsDepTime() {
		return crsDepTime;
	}

	public void setCrsDepTime(IntWritable crsDepTime) {
		this.crsDepTime = crsDepTime;
	}

	public IntWritable getQuarter() {
		return quarter;
	}

	public void setQuarter(IntWritable quarter) {
		this.quarter = quarter;
	}

	public Text getOriginAirport() {
		return originAirport;
	}

	public void setOriginAirport(Text originAirport) {
		this.originAirport = originAirport;
	}

	public Text getDestAirport() {
		return destAirport;
	}

	public void setDestAirport(Text destAirport) {
		this.destAirport = destAirport;
	}

	public Text getCarrier() {
		return carrier;
	}

	public void setCarrier(Text carrier) {
		this.carrier = carrier;
	}

	public IntWritable getFlNum() {
		return flNum;
	}

	public void setFlNum(IntWritable flNum) {
		this.flNum = flNum;
	}

	public IntWritable getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(IntWritable dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public IntWritable getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(IntWritable dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public IntWritable getDistanceGroup() {
		return distanceGroup;
	}

	public void setDistanceGroup(IntWritable distanceGroup) {
		this.distanceGroup = distanceGroup;
	}

	/*public DoubleWritable getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(DoubleWritable arrDelay) {
		this.arrDelay = arrDelay;
	}*/


	public Text getFlDate() {
		return flDate;
	}


	public void setFlDate(Text flDate) {
		this.flDate = flDate;
	}


	public IntWritable getIsHoliday() {
		return isHoliday;
	}


	public void setIsHoliday(IntWritable isHoliday) {
		this.isHoliday = isHoliday;
	}
	
	public Text getCrsElapsedTime() {
		return crsElapsedTime;
	}

	public void setCrsElapsedTime(Text crsElapsedTime) {
		this.crsElapsedTime = crsElapsedTime;
	}

	public Text getMonth() {
		return month;
	}

	public void setMonth(Text month) {
		this.month = month;
	}

	public Text getDay() {
		return day;
	}

	public void setDay(Text day) {
		this.day = day;
	}

	public Text getCarrierId() {
		return carrierId;
	}

	public void setCarrierId(Text carrierId) {
		this.carrierId = carrierId;
	}


	public Text getDistance() {
		return distance;
	}

	public void setDistance(Text distance) {
		this.distance = distance;
	}

	public Text getConnectionLink() {
		return connectionLink;
	}

	public void setConnectionLink(Text connectionLink) {
		this.connectionLink = connectionLink;
	}

	public LongWritable getCrsArrTime_long() {
		return crsArrTime_long;
	}

	public void setCrsArrTime(LongWritable crsArrTime_long) {
		this.crsArrTime_long = crsArrTime_long;
	}

	public LongWritable getCrsDepTime_long() {
		return crsDepTime_long;
	}

	public void setCrsDepTime(LongWritable crsDepTime_long) {
		this.crsDepTime_long = crsDepTime_long;
	}

}