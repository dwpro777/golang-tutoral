#! /usr/bin/Rscript --vanilla 

args <- commandArgs(TRUE)
resRoastLogPacket <- read.csv(file=sprintf("csvDir/roastOutput%s.csv",args[1]),header=TRUE,sep="\t")
png(sprintf("images/roast%s.png",args[1]))
plot(x = resRoastLogPacket$rTime, y = resRoastLogPacket$RoasterTempInDegrees, type = "o")
graphics.off()
quit()
