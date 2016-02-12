#!/bin/bash

# authors Rohan Joshi, Vedant Naik

# The flow of the program ->
# 1. fire the EMR cluster 
# 2. get the job number or the cluster id in a variable
# 3. Use that variable to keep polling the cluster status
# 4. Once the status changes to "TERMINATED", we end the loop and download the output/* and log/* files into a local folder
# 5. These output files are in a location that is known to the R script, and along with the output files from the pseudo mode,
# we can generate graphs

# based on the os, please uncomment one of the two lines to install jq, the tool that greps JSON data that is
# returned by the polling (may require sudo)
# please enter the password whenever prompted if the install commands are executed

# for linux: 
# sudo apt-get install jq

# for OS X
# brew install jq

# args for the java program are::  
# args[0] : path to downloaded folder
# args[1] : mean/median/fastmedian

downloadpath=$1
echo $downloadpath

bucketname=$2
echo $bucketname
aws s3 ls s3://$bucketname
echo $PATH
#cluster_id="j-1L5ZOXYTPB54G"
i=0
locpath="$downloadpath/output_aws_lin"
rm -rf $locpath
#args1 are -mean, -median or -fastMedian
#args1=$1
#echo $args1
#$1="/Users/rohanjoshi/Documents/output_aws4/"
#eval job_id=`aws emr create-cluster --name "CLI-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Name="ClusterAnalysis",ActionOnFailure=CONTINUE,Jar=s3://clusteranalysis/ClusterAnalysis.jar,MainClass=ClusterAnalysis,Args=[s3://clusteranalysis/input/all,s3://clusteranalysis/output/] --auto-terminate --log-uri s3://clusteranalysis/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging | jq ".ClusterId"`
eval job_id=`aws emr create-cluster --name "LIN-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=m1.large InstanceGroupType=CORE,InstanceCount=2,InstanceType=m1.large --steps Type=CUSTOM_JAR,Name="LinearRegression",ActionOnFailure=CONTINUE,Jar=s3://$bucketname/LinearRegression.jar,MainClass=LinearRegression,Args=[-emr,s3://$bucketname/input/all,s3://$bucketname/output/] --auto-terminate --log-uri s3://$bucketname/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging | jq ".ClusterId"`

echo "now removing the folder output_aws_lin"

#rm -rf /Users/rohanjoshi/Documents/output_aws_lin

echo $job_id
echo "deleting the log directory in the S3 bucket"
aws s3 rm s3://$bucketname/log --recursive

echo "deleting the output directory in the S3 bucket"
aws s3 rm s3://$bucketname/output --recursive

status=`aws emr describe-cluster --cluster-id "$job_id" | jq ".Cluster.InstanceGroups[$i].Status.State"`
echo $status
flag="False"
while [ $flag == "False" ]
do
	sleep 5
	status=`aws emr describe-cluster --cluster-id "$job_id" | jq ".Cluster.InstanceGroups[$i].Status.State"`
	echo $status
	if [ $status = '"TERMINATED"' ]; then
		flag="True"
	else
		flag="False"
	fi
done
aws s3 sync s3://$bucketname/output $locpath

echo "The job has terminated and all the output files downloaded..."
echo "The folder where the part files are located is the downloadpath variable that you specified in the script.cfg file"

echo "completed if uptil here"