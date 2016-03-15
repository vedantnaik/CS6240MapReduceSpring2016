
run:
	rm -rf out*
	mkdir outputPseudo
	sbt "run all outputPseudo -psu"
	cat out*/part*

emr:
	sbt package
	cp /target/scala-2.10/*.jar .
	./emr_spark.sh <BUCKET_NAME>

clean:
	rm -rf out derby.log metastore_db project target

