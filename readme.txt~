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
- R packages:
	- dplyr
	- ggplot2
	- library(R.utils)
- jq - a tool that allows us to grep the data that the AWS EMR cluster sends back
- pandoc for R
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
- *** the download path for the folder where the script downloads the AWS output files is relative to the directory in which you extracted the project ***
- please ensure that you are in the project directory when you run the commands (i.e, you have cd)

Requirements to get the program to work on the local hadoop or pseudo mode:
---------------------------------------------------------------------------
- Please ensure that the JAVA_HOME is pointing to the right location
- Please ensure that the HADOOP_HOME is pointing to the right location
- Give execution (chmod -R 777) to your HADOOP_HOME
- Please ensure that the namenode, datanode are working prior to running the program - 'jps' is the command that will show you the running daemons
- Please ensure that you have the input files in the correct format in the folder titled 'all' in the same folder as the Makefile

Details about the script that runs the job on AWS:
--------------------------------------------------
- The bucket name on S3 needs to be unique.  If you get an error saying that the bucketname exists then please change the bucket name in the script.cfg file
and try again
- The script contains comments that make it easier to understand the code
- There are respective commands that install the jq tool based on the OS you are using. Please uncomment the one that corresponds to your OS

Where do you have to plug in the configurations of your own system:
-------------------------------------------------------------------
- You will have to specify the local path where the log and output files will be downloaded in the script.cfg file. The places are marked with <DOWNLOAD_PATH>
- You will have to specify your bucket name on S3 in the script.cfg file.  The places are marked with <BUCKET_NAME>

Instructions to run the program on pseudo mode:
-----------------------------------------------
- make pseudo: 
	- create the jar file using the java source
	- upload input files from 'all' to the hdfs
	- runs the jar
	- pulls output from hdfs to the local folder "outputPseudo"
	- runs the R markdown script to generate the report.  This step uses the output from the previous step
	
Instructions to run the program on AWS:
---------------------------------------
- run the 'make cloud' command
	- The script pulls the bucketname from the script.cfg file
	- Using this bucket name, a new bucket will be created on AWS
	- The input files from 'all' will be copied to the bucket/input/all folder
	- The jar is then copied from the project folder into the S3 bucket
	- The cluster is then started with the jar, input/all folder and the output is put into the bucket/output folder
	- The script keeps polling for the status, and when the status turns to 'TERMINATED', the output files from AWS are copied into outputAws

Requirements for the R script:
------------------------------
- Please ensure that R is installed on the system with the packages ggplot2, dplyr installed
- The R script pulls the files from the output folder of the pseudo mode and the output folder of the AWS mode
- This requires that you put in all the paths wherever specified

Details about the R script:
---------------------------
- explained in the report

Error that can come up in R:
----------------------------

You should install a recommended TeX distribution for your platform:

  Windows: MiKTeX (Complete) - http://miktex.org/2.9/setup
    (NOTE: Be sure to download the Complete rather than Basic installation)
    
      Mac OS X: TexLive 2013 (Full) - http://tug.org/mactex/
        (NOTE: Download with Safari rather than Chrome _strongly_ recommended)
        
          Linux: Use system package manager
          
- This requires LaTeX for Linux

Reference output:
-----------------
HA, EV, MQ, OO, US, B6, WN, UA, DL, NK, VX, AS, F9, AA
(Of these, NK does not have any data in 2010-2014)

The cheapest airline is:: 
F9

Conclusion:
-----------
- We are using MSE to determine the best fit. In this case, the time variable has a better linear regression model. Hence, we use this to predict prices
of all carriers. We are predicting based on the mean value of time from the entire dataset(here, 111.05). Of all the predicted prices(for all carriers) 
F9 gives the cheapest predicted price. Hence, F9 is the cheapest.
The MSE for most of the carriers is lesser for time variable compared to the distance variable
