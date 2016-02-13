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
	hadoop fs -put all input

localDirs:
	mkdir localOutput
	mkdir timeOutput
	touch timeOutput/allTimes.csv

jar:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar *.java
	jar cvf LinearRegression.jar *.class

pseudoLR:
	touch ${HADOOP_HOME}/pseudoTime.csv
	hadoop jar LinearRegression.jar LinearRegression -pseudo input/all outputPseudo
	hadoop fs -get outputPseudo .
	cat ${HADOOP_HOME}/pseudoTime.csv >> timeOutput/allTime.csv
	rm -f ${HADOOP_HOME}/pseudoTime.csv

emr:
	./cloud.sh

clearhadoop:
	hadoop fs -rm -r /user/vedant

clearClassJarJava:
	rm -f *.class
	rm -f *.java
	rm -f *.jar

clearLocalDirs:
	rm -rf localOutput
	rm -rf timeOutput

clearAll:
	hadoop fs -rm -r /user/vedant
	rm -f *.class
	rm -f *.java
	rm -f *.jar
	rm -rf localOutput
	rm -rf timeOutput

pullJava:
	cp src/*.java .

testharness:
	hadoop fs -mkdir -p /user/vedant
	hadoop fs -mkdir -p /user/vedant/input
	hadoop fs -put all input
	mkdir localOutput
	mkdir timeOutput
	touch timeOutput/allTimes.csv
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar *.java
	jar cvf LinearRegression.jar *.class

