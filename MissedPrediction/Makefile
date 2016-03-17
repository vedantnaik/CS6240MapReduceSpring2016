format:
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

vedant:
	hadoop fs -mkdir -p /user/vedant
	hadoop fs -mkdir -p /user/vedant/input
	hadoop fs -mkdir -p /user/vedant/models
	hadoop fs -put all input
	hadoop fs -put testData input

clearAll:
	hadoop fs -rm -r /user/vedant*
	clear

clearPredictions:
	hadoop fs -rm -r /user/vedant/predOutput*

clearAllOutput:
	hadoop fs -rm -r /user/vedant/trainPseudo*
	hadoop fs -rm -r /user/vedant/predOutput*
	hadoop fs -rm -r /user/vedant/trainPseudo*

clearLocal:
	rm -r outputPseudo*
	rm -r models*

createJarWithDependencies:
	mvn clean compile assembly:single
	touch ${HADOOP_HOME}/pseudoTime.csv
	cp target/DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar .

completePseudo:
	hadoop jar DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar -pseudo input/all input/testData trainPseudo models training
	hadoop jar DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar -pseudo input/all input/testData predOutput models testing
	hadoop fs -get predOutput .
	java -jar DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar -pseudo input/all input/testData predOutput models evaluation validateData/98validate.csv.gz

pseudo:
	hadoop jar DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar -pseudo input/all input/testData outputPseudo models training
	
pseudoTest:
	hadoop jar DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar -pseudo input/all input/testData predOutput models testing
	hadoop fs -get predOutput .
	
localComparison:
	java -jar DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar -pseudo input/all input/testData predOutput models evaluation validateData/98validate.csv.gz
										
emr:
	./cloud.sh

