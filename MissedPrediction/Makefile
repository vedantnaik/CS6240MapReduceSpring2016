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

clearOutput:
	hadoop fs -rm -r /user/vedant/output*

pseudo:
	mvn clean compile assembly:single
	touch ${HADOOP_HOME}/pseudoTime.csv
	cp target/DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	hadoop jar DelayedPredictions-0.0.1-SNAPSHOT-jar-with-dependencies.jar -pseudo input/all input/testData outputPseudo models
	hadoop fs -get outputPseudo .
	cat ${HADOOP_HOME}/pseudoTime.csv
	cat outputPseudo/part-*

emr:
	./cloud.sh

