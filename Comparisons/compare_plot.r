# Load the libraries
require(ggplot2)
require(dplyr)
library(ggplot2)

args <- commandArgs(TRUE)
input <- args[1]
out <- args[2]

results <- dir(input, pattern=".csv", full.names=TRUE)
merged <- lapply(results , read.csv, header=FALSE)
result <- bind_rows(merged)
names(result) <- c("RunType","ValueType", "Value")

finalplot <- ggplot(data=result, aes(x=RunType, y=Value,color=ValueType,group=ValueType)) + geom_line() + geom_point(size=2)

ggsave(filename = out, plot = finalplot)
