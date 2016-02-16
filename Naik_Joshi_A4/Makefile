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

pseudo:	
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar *.java
	jar cvf LinearRegression.jar *.class
	hadoop fs -put all input
	touch ${HADOOP_HOME}/pseudoTime.csv
	hadoop jar LinearRegression.jar LinearRegression -pseudo input/all outputPseudo
	hadoop fs -get outputPseudo .
	cat ${HADOOP_HOME}/pseudoTime.csv
	rm -f ${HADOOP_HOME}/pseudoTime.csv
	Rscript -e "rmarkdown::render('LinearReport.Rmd')" outputPseudo

emr:
	./cloud.sh
	Rscript -e "rmarkdown::render('LinearReport.Rmd')" outputAws
