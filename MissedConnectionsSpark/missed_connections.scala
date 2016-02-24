import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object MissedConnections {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Missed Connections").
            setMaster("local")
        val sc = new SparkContext(conf)

        //val connectionCount = sc.accumulator(0, "connectionCount")       

        // Input
        val saneRecords = sc.textFile("all/*.csv.gz").
            map { _.replaceAll("\"", "").replaceAll(", ", ":").split(",") }.
            filter (flRecord => { flRecord(0) != "YEAR" && FileRecord.isRecordValid(flRecord) })

        val origins = saneRecords.
            keyBy (flRecord => {FileRecord.getValueOf(flRecord, FileRecord.CARRIER) + "\t" + 
		                        FileRecord.getValueOf(flRecord, FileRecord.ORIGIN) + "\t" + 
                                FileRecord.getValueOf(flRecord, FileRecord.YEAR) })

        val dest = saneRecords.
            keyBy (flRecord => {FileRecord.getValueOf(flRecord, FileRecord.CARRIER) + "\t" + 
		                        FileRecord.getValueOf(flRecord, FileRecord.DEST) + "\t" + 
                                FileRecord.getValueOf(flRecord, FileRecord.YEAR) })

        
        val mappedPairs = origins.join(dest)
        // key : ([ fields[] ], [ fields[] ])
        
        mappedPairs.foreach(x => { 
                    val originList = x._2._1;
                    val destList = x._2._2;
                    println (x._1 + " :: " + 
                            FileRecord.getValueOf(originList, FileRecord.CRS_ARR_TIME) + " | " + 
                            FileRecord.getValueOf(destList, FileRecord.CRS_DEP_TIME) )
                    
                    //connectionCount += 1;
                            

                    } )


        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
