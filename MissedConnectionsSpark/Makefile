

run:
	rm -rf out*
	mkdir outputPseudo
	sbt "run all1 outputPseudo/part-out.txt"
	cat out*/part*

emr: 
	./emr_spark.sh <BUCKET_NAME>

clean:
	rm -rf out derby.log metastore_db project target
