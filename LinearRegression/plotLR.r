require(dplyr)
require(ggplot2)
library(R.utils)

# Read data folder name from command line arguments
args <- commandArgs(trailingOnly = TRUE)
outputFolder <- args[1]

files <- dir(outputFolder, pattern='part-r-', full.names = TRUE)
partOutputs <- lapply(files, read.csv, header=FALSE, sep="\t")
data <- bind_rows(partOutputs)
# Set headers
names(data) <- c("Carrier", "Distance", "Time", "AvgPrice")

for(carrier in unique(data$Carrier)){
  dfForCarrier <- data[which(data$Carrier == carrier),]
  
  lrDistance <- lm(dfForCarrier$AvgPrice ~ dfForCarrier$Distance)

  lrTime <- lm(dfForCarrier$AvgPrice ~ dfForCarrier$Time)

  jpeg(paste(carrier,sep = ".","jpg"))
  
  par(mfrow = c(2,1))
  
  plot(dfForCarrier$Distance, dfForCarrier$AvgPrice,ylab="Average Ticket Price",
       xlab=paste("Distance   ","Intercept :", lrDistance$coefficients[[1]], 
                  "  Slope: ", lrDistance$coefficients[[2]]))
  abline(lrDistance, col='green')
  
  plot(dfForCarrier$Time, dfForCarrier$AvgPrice,ylab="Average Ticket Price",
       xlab=paste("Distance   ","Intercept :", lrTime$coefficients[[1]], 
                  "  Slope: ", lrTime$coefficients[[2]]))
  abline(lrTime, col='red')
  
  dev.off()
}
