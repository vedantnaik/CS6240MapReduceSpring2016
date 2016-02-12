Readme for Assignment 4 : Linear Regression
-------------------------------------------

Team Members:
Vedant Naik
Rohan Joshi

System Requirements for getting the assignment to work:
-------------------------------------------------------
- Linux Environment
- Java 1.7
- Hadoop 2.7.1+
- RStudio
- R packages (*** elaborate further ***)
- jq - a tool that allows us to grep the data that the AWS EMR cluster sends back
- panic for R
- LaTEX for your OS (These two packages are required to knit pdf through R)

Path and variables that need to be set in order to get the project to compile and build successfully:
---------------------------------------------------------------------------------------------------
- JAVA_HOME
- HADOOP_HOME

Reference labels:
-----------------
- <BUCKET_NAME> : The name of the S3 bucket
- <DOWNLOAD_PATH> : The path of the folder where the output files are download
- <HADOOP_HOME> : The path of folder where hadoop lives in your system

Requirements for the project to run successfully on AWS:
--------------------------------------------------------
- The CLI should be configured correctly so that it can run commands and interact with your AWS account
- The script that runs the job on AWS will show the progress, please make sure you have the terminal window open till the script completes
- It is assumed that the reviewer will run the job on his own AWS account to prevent the exchange of keys
- Please make sure that the output format for AWS CLI is set to 'json' before running the program in AWS mode

Requirements to get the program to work on the local hadoop or pseudo mode:
---------------------------------------------------------------------------
- Please ensure that the JAVA_HOME is pointing to the right location
- Please ensure that the HADOOP_HOME is pointing to the right location
- Please ensure that the namenode, datanode are working prior to running the program - 'jps' is the command that will show you the running daemons
- Please ensure that you have the input files in the correct format

Details about the script that runs the job on AWS:
--------------------------------------------------
- The script contains comments that make it easier to understand the code
- There are respective commands that install the jq tool based on the OS you are using. Please uncomment the one that corresponds to your OS

Where do you have to plug in the configurations of your own system:
-------------------------------------------------------------------
- You will have to specify the local path where the log and output files will be downloaded in the script.cfg file. The places are marked with <DOWNLOAD_PATH>
- You will have to specify your bucket name on S3 in the script.cfg file.  The places are marked with <BUCKET_NAME>


Details about the Makefile:
---------------------------
*** elaborate further ***

Instructions to run the program on pseudo mode:
-----------------------------------------------
- run the 'make ***' command

Instructions to run the program on AWS:
---------------------------------------
- run the 'make cloud' command

Requirements for the R script:
------------------------------
- Please ensure that R is installed on the system with the packages ggplot2, dplyr installed
- The R script pulls the files from the output folder of the pseudo mode and the output folder of the AWS mode
- This requires that you put in all the paths wherever specified

Details about the R script:
---------------------------
*** elaborate further ***

Reference output:
-----------------

Analysis:
---------

Conclusion:
-----------

