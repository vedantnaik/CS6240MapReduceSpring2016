

run:
	rm -rf out*
	mkdir outputPseudo
	sbt "run all1 outputPseudo/part-out.txt"
	cat out*/part*

clean:
	rm -rf out derby.log metastore_db project target
