#!/bin/bash

# authors Rohan Joshi, Vedant Naik

# The flow of the program ->
# 1. fire the EMR cluster 
# 2. get the job number or the cluster id in a variable
# 3. Use that variable to keep polling the cluster status
# 4. Once the status changes to "TERMINATED", we end the loop and download the output/* and log/* files into a local folder
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
bucketname=$1
echo $bucketname
aws s3 mb $bucketname
aws s3 cp all/ s3://$bucketname/input/all/ --recursive
aws s3 cp missed-connections_2.10-0.1-SNAPSHOT.jar s3://$bucketname
aws s3 ls s3://$bucketname
echo $PATH
#cluster_id="j-1L5ZOXYTPB54G"
i=0
rm -rf /Users/rohanjoshi/Documents/output_aws_lin
#args1 are -mean, -median or -fastMedian
#args1=$1
#echo $args1
#$1="/Users/rohanjoshi/Documents/output_aws4/"
#eval job_id=`aws emr create-cluster --name "CLI-Cluster" --release-label emr-4.3.0 --instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Name="ClusterAnalysis",ActionOnFailure=CONTINUE,Jar=s3://clusteranalysis/ClusterAnalysis.jar,MainClass=ClusterAnalysis,Args=[s3://clusteranalysis/input/all,s3://clusteranalysis/output/] --auto-terminate --log-uri s3://clusteranalysis/log --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging | jq ".ClusterId"`
eval job_id=`aws emr create-cluster --name "Spark Cluster" --ami-version 3.11.0 --applications Name=Spark \
 --ec2-attributes --instance-type m3.xlarge --log-uri s3://misdconsparkb/logs --service-role EMR_DefaultRole \
 --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a --enable-debugging --instance-count 5 --auto-terminate | jq ".ClusterId"`

aws emr add-steps --cluster-id $job_id --steps Type=Spark,Name="Spark Step",Args=[--deploy-mode,cluster,--class,MissedConnections,--verbose,s3://misdconsparkb/mc.jar,s3://misdconsparkb/input/all,s3://misdconsparkb/output]

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
finpath='outputAws'
aws s3 sync s3://$bucketname/output $finpath

# uncomment this line if you want to locally download the logs
#mkdir output_aws_lin
#mkdir output_aws_lin/log
#aws s3 sync s3://$bucketname/log/$job_id/steps/ output_aws_lin/log/

#cat /Users/rohanjoshi/Documents/output_aws_lin/* > output_aws.txt
#cd /Users/rohanjoshi/Documents/workspace_exp/Read/src
echo "completed if uptil here"