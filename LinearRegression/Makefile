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

sequentialMean:
	touch localOutput/mean_s.txt
	java LinearRegression -mean -s all localOutput

sequentialFastMedian:
	touch localOutput/fastMedian_s.txt
	java LinearRegression -fastMedian -s all localOutput

sequentialMedian:
	touch localOutput/median_s.txt
	java LinearRegression -median -s all localOutput

multiThreadMean:
	touch localOutput/mean_mt.txt
	java LinearRegression -mean -mt all localOutput

multiThreadFastMedian:
	touch localOutput/fastMedian_mt.txt
	java LinearRegression -fastMedian -mt all localOutput

multiThreadMedian:
	touch localOutput/median_mt.txt
	java LinearRegression -median -mt all localOutput

pseudoMedian:
	touch ${HADOOP_HOME}/pseudomedianTime.csv
	hadoop jar LinearRegression.jar LinearRegression -median -pseudo input/all outputPseudoMedian
	hadoop fs -get outputPseudoMedian .
	cat ${HADOOP_HOME}/pseudomedianTime.csv >> timeOutput/allTime.csv
	rm -f ${HADOOP_HOME}/pseudomedianTime.csv

pseudoFastMedian:
	touch ${HADOOP_HOME}/pseudofastMedianTime.csv
	hadoop jar LinearRegression.jar LinearRegression -fastMedian -pseudo input/all outputPseudoFastMedian
	hadoop fs -get outputPseudoFastMedian .
	cat ${HADOOP_HOME}/pseudofastMedianTime.csv >> timeOutput/allTime.csv
	rm -f ${HADOOP_HOME}/pseudofastMedianTime.csv

pseudoMean:
	touch ${HADOOP_HOME}/pseudomeanTime.csv
	hadoop jar LinearRegression.jar LinearRegression -mean -pseudo input/all outputPseudoMean
	hadoop fs -get outputPseudoMean .
	cat ${HADOOP_HOME}/pseudomeanTime.csv >> timeOutput/allTime.csv
	rm -f ${HADOOP_HOME}/pseudomeanTime.csv

emr:
	run.sh -mean -median -fastMean

clearhadoop:
	hadoop fs -rm -r /user/vedant

clearClassJarJava:
	rm -f *.class
	rm -f *.java
	rm -f *.jar

clearLocalDirs:
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
	touch localOutput/mean_s.txt
	java LinearRegression -mean -s all localOutput
	touch localOutput/fastMedian_s.txt
	java LinearRegression -fastMedian -s all localOutput
	touch localOutput/median_s.txt
	java LinearRegression -median -s all localOutput
	touch localOutput/mean_mt.txt
	java LinearRegression -mean -mt all localOutput
	touch localOutput/fastMedian_mt.txt
	java LinearRegression -fastMedian -mt all localOutput
	touch localOutput/median_mt.txt
	java LinearRegression -median -mt all localOutput
	touch ${HADOOP_HOME}/pseudomedianTime.csv
	hadoop jar LinearRegression.jar LinearRegression -median -pseudo input/all outputPseudoMedian
	hadoop fs -get outputPseudoMedian .
	cat ${HADOOP_HOME}/pseudomedianTime.csv >> timeOutput/allTimes.csv
	rm -f ${HADOOP_HOME}/pseudomedianTime.csv
	touch ${HADOOP_HOME}/pseudofastMedianTime.csv
	hadoop jar LinearRegression.jar LinearRegression -fastMedian -pseudo input/all outputPseudoFastMedian
	hadoop fs -get outputPseudoFastMedian .
	cat ${HADOOP_HOME}/pseudofastMedianTime.csv >> timeOutput/allTimes.csv
	rm -f ${HADOOP_HOME}/pseudofastMedianTime.csv
	touch ${HADOOP_HOME}/pseudomeanTime.csv
	hadoop jar LinearRegression.jar LinearRegression -mean -pseudo input/all outputPseudoMean
	hadoop fs -get outputPseudoMean .
	cat ${HADOOP_HOME}/pseudomeanTime.csv >> timeOutput/allTimes.csv
	rm -f ${HADOOP_HOME}/pseudomeanTime.csv
	aws s3 ls s3://${BUCKET_NAME}
	aws s3 mb s3://${BUCKET_NAME}
	aws s3 cp all/ s3://${BUCKET_NAME}/input/all --recursive
	aws s3 cp ClusterAnalysis.jar s3://${BUCKET_NAME}
	run.sh -mean -median -fastMean
	cat ${HADOOP_HOME}/emrmeanTime.csv >> timeOutput/allTimes.csv
	cat ${HADOOP_HOME}/emrmedianTime.csv >> timeOutput/allTimes.csv
	cat ${HADOOP_HOME}/emrfastMedianTime.csv >> timeOutput/allTimes.csv
	Rscript compare_plot.r timeOutput timeLinearRegression.pdf

