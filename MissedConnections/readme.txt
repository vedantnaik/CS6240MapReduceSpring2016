--------------------------------------------
README for Assignment 5 : Missed Connections
--------------------------------------------
Team members: Vedant Naik, Rohan Joshi

---------
CONTENTS:
---------
The project folder/zip contains the following files
- readme.txt
- src
--- FileRecord.java
--- MissedConnections.java
- Makefile

-------------------------
Requirements for project:
-------------------------
- Linux Environment
- Java 1.7+
- Hadoop 2.7.1+
- Text editor to view the files or any IDE

-----------------------------------------------------
How to run the project in the pseudo-distributed mode
-----------------------------------------------------
- Ensure that the local hadoop is running
- Type jps and ensure datanode and namenode are up and running
- Download the tar.gz project
- Unzip the files in the File system
- Navigate to the project directory using the terminal
- Ensure that the Makefile is present in the unzipped folder
- In the terminal type: make pseudo

-----------------------------------------
What happens when you type 'make pseudo':
-----------------------------------------
- The jar is created using the source code
- A new directory inside your HDFS is created
- The data source is copied inside the output directory under the newly created user in HDFS
- The job is then started using the jar and the input that was just copied
- The output is then copied to a folder called outputPseudo in the project directory

---------
ANALYSIS:
---------

-----------------
REFERENCE OUTPUT:
-----------------

-----------
CONCLUSION:
-----------
