setwd("~/sem4/mr/gitRepo/CS6240MapReduceSpring2016/LinearRegression")

require(dplyr)
require(ggplot2)
library(R.utils)

# Read data folder name from command line arguments
args <- commandArgs(trailingOnly = TRUE)
outputFolder <- "outputPseudo"

files <- dir(outputFolder, pattern='part-r-', full.names = TRUE)
#files <- lapply(Filter(function(x) countLines(x)==0, files), unlink)

partOutputs <- lapply(files, read.csv, header=FALSE, sep="\t")
data <- bind_rows(partOutputs)

# Set headers
names(data) <- c("Carrier", "Distance", "Time", "AvgPrice")

predValues <- data.frame()

testDistVal <- median(date$Distance)
testTimeVal <- median(data$Time)

for(carrier in unique(data$Carrier)){
  dfForCarrier <- data[which(data$Carrier == carrier),]
  
  lrDistance <- lm(dfForCarrier$AvgPrice ~ dfForCarrier$Distance)
  lrTime <- lm(dfForCarrier$AvgPrice ~ dfForCarrier$Time)
  
  jpeg(paste(carrier,".jpeg"))
  par(mfrow = c(2,1))
  
  plot(dfForCarrier$Distance, dfForCarrier$AvgPrice,ylab="Average Ticket Price",
       xlab=paste("Distance   ","Intercept :", lrDistance$coefficients[1], "  Slope: ", 
                  lrDistance$coefficients[2]))
  abline(lrDistance, col='green')
  
  plot(dfForCarrier$Time, dfForCarrier$AvgPrice,ylab="Average Ticket Price",
       xlab=paste("Distance   ","Intercept :", lrTime$coefficients[1], "  Slope: ", 
                  lrTime$coefficients[2]))
  abline(lrTime, col='red')
  
  dev.off()
  
  predDistancePrice = lrDistance$coefficients[1] + lrDistance$coefficients[2] * testDistVal
  predTimePrice = lrTime$coefficients[1] + lrTime$coefficients[2] * testTimeVal
  
  carrier = carrier
  
  predValues <- rbind(predValues, c(carrier, predDistancePrice, predTimePrice))
}

names(predValues) <- c("Carrier", "DistPrice", "TimePrice")

predValues
