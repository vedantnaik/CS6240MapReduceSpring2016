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
		
		this.distanceGroup = new IntWritable();
		
		this.arrDelay = new DoubleWritable();
	
		this.flDate = new Text();
		this.isHoliday = new IntWritable();
	}
	

	public AirlineMapperValue(IntWritable crsArrTime, IntWritable crsDepTime, IntWritable quarter,
			Text originAirport, Text destAirport, Text carrier, IntWritable flNum, IntWritable dayOfMonth,
			IntWritable dayOfWeek, IntWritable distanceGroup, DoubleWritable arrDelay, Text flDate, 
			IntWritable isHoliday) {
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
		this.distanceGroup = distanceGroup;
		this.arrDelay = arrDelay;
		this.flDate = flDate;
		this.isHoliday = isHoliday;
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
		
		this.distanceGroup = new IntWritable(amv.getDistanceGroup().get());
		
		this.arrDelay = new DoubleWritable(amv.getArrDelay().get());

		this.flDate = new Text(amv.getFlDate().toString());
		this.isHoliday = new IntWritable(amv.getIsHoliday().get());
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
		
		arrDelay.readFields(inVal);
		
		flDate.readFields(inVal);
		isHoliday.readFields(inVal);
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
		
		distanceGroup.write(outVal);
		
		arrDelay.write(outVal);
		
		flDate.write(outVal);
		isHoliday.write(outVal);
	}

	@Override
	public String toString() {
		return 
				"["
				+ "crsArr:" + new Date(crsArrTime.get()).toString() + " "
				+ "crsDep:" + new Date(crsDepTime.get()).toString() + " "
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

	public DoubleWritable getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(DoubleWritable arrDelay) {
		this.arrDelay = arrDelay;
	}


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
}