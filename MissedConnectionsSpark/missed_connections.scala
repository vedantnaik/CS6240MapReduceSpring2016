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

        val connectionCount = sc.accumulator(0, "connectionCount")       
        val missedCount = sc.accumulator(0, "missedCount")       

        // Input
        val saneRecords = sc.textFile("all/*.csv.gz").
            map { _.replaceAll("\"", "").replaceAll(", ", ":").split(",") }.
            filter (flRecord => { flRecord(0) != "YEAR" && FileRecord.isRecordValid(flRecord) && flRecord.length == 110 })

        saneRecords.foreach(x => {println ("sane record!!! " + x)})

        val origins = saneRecords.
            keyBy (flRecord => {FileRecord.getValueOf(flRecord, FileRecord.CARRIER) + "\t" + 
		                        FileRecord.getValueOf(flRecord, FileRecord.ORIGIN) + "\t" + 
                                FileRecord.getValueOf(flRecord, FileRecord.YEAR) }).
            map (x => {
                val (k, v) = x;
                k -> Array(FileRecord.getDateFieldInLong(v, FileRecord.CRS_ARR_TIME), 
                           FileRecord.getDateFieldInLong(v, FileRecord.CRS_DEP_TIME),
                           FileRecord.getDateFieldInLong(v, FileRecord.ARR_TIME), 
                           FileRecord.getDateFieldInLong(v, FileRecord.DEP_TIME))
            })

        val dest = saneRecords.
            keyBy (flRecord => {FileRecord.getValueOf(flRecord, FileRecord.CARRIER) + "\t" + 
		                        FileRecord.getValueOf(flRecord, FileRecord.DEST) + "\t" + 
                                FileRecord.getValueOf(flRecord, FileRecord.YEAR) }).
            map (x => {
                val (k, v) = x;
                k -> Array(FileRecord.getDateFieldInLong(v, FileRecord.CRS_ARR_TIME), 
                           FileRecord.getDateFieldInLong(v, FileRecord.CRS_DEP_TIME),
                           FileRecord.getDateFieldInLong(v, FileRecord.ARR_TIME), 
                           FileRecord.getDateFieldInLong(v, FileRecord.DEP_TIME))
            })

        
        val mappedPairs = origins.cogroup(dest)
        // key : (fields[], fields[])
        
        mappedPairs.foreach(x => { 
          
            val cogroupOriginVals = x._2._1;
            val cogroupDestVals = x._2._2;
//            println("origin")
//            cogroupOriginVals.foreach(fields => { fields.foreach(y => { print(y + " : ") }) })
//            println("dest")
//            cogroupOriginVals.foreach(fields => { fields.foreach(y => { print(y + " : ") }) })

            for (g <- cogroupOriginVals){
             for (f <- cogroupDestVals){
                val farr = f(0)
                val gdep = g(1)

                val factArr = f(2)
                val gactDep = g(3)
 
                //println (x._1 + " == " +farr + " ::> " + gdep + " ::><:: " + factArr + " ::> " + gactDep)                

                if(MissedConnectionsUtil.isConnection(farr, gdep))
                        {
                            println("============CONNECTION!!=================")
				            connectionCount += 1;
				            if(MissedConnectionsUtil.missedConnection(factArr, gactDep)){
					            missedCount += 1;
                                println("============MISSED!!=================")
				            }
			            }
                    }
                   }
                } )

        println(" Connections " + connectionCount)
        println(" Missed " + missedCount)


        // Shut down Spark, avoid errors.
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
