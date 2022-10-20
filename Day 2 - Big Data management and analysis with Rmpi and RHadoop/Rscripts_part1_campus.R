

#******************************
# create name of folder + git pull
#******************************
work_dir=paste("/home", Sys.getenv("USER"),"resources", sep="/")
data_dir=paste(work_dir,"data", sep="/")

#unlink(work_dir, recursive = TRUE)  

setwd(work_dir)
system("git pull")

#system(paste0("cp -R /home/campus01/Rscripts/AutumnSchool_2021/*.R /home/",Sys.getenv("USER"),"/Rscripts/AutumnSchool_2021"))
dir()
dir("data/")


#******************************
# Create and save simple data file
#******************************


N=1000;
set.seed(2021)
Data=data.frame(group=character(N),ints=numeric(N),reals=numeric(N))
Data$group=sample(c("a","b","c"), 1000, replace=TRUE);
Data$ints=rbinom(N,10,0.5);
Data$reals=rnorm(N);

head(Data)
Data


write.table(Data, file='data/Data_Ex_1.txt', append = FALSE, dec = ".",col.names = TRUE)
#file.remove('Data_Ex_1.txt')

ls()
rm(list = ls())


#******************************
# Load and analyse the data
#******************************
Data_read<-read.table(file='data/Data_Ex_1.txt',header = TRUE)
# first few rows
head(Data_read)

#10 th row
Data_read[10,]
# column group
Data_read$group
Data_read[,1]

Group_lev=levels(Data_read$group)

Tab_summary=data.frame(group=character(3),count_ints=integer(3),mean_ints=numeric(3))
Tab_summary$group<-Group_lev
for (i in c(1:3)){
  sub_data = subset(Data_read,group==Group_lev[i])
  Tab_summary$count_ints[i]<-nrow(sub_data)
  Tab_summary$mean_ints[i]<-mean(sub_data$ints)
}

#******************************
# summary using dplyr and magrittr
#******************************

library(dplyr)
library(magrittr)
Tab_summary1<-group_by(Data_read,group) %>% dplyr::summarise(count_ints=n(),mean_ints=mean(ints))

# other operations on rows and columns
Data_read_group_ints<-Data_read %>% select(group,ints)
# add new variable reals/ints
Data_read<-mutate(Data_read,ratio=reals/ints)
Data_read<-Data_read %>% mutate(ratio1=ints/reals)
#arrange
#sort accordind to increasing group
Data_read<-Data_read %>% arrange(desc(group))
Data_read<-Data_read %>% arrange(group)


#******************************
# using aggregate and split
#******************************
#require(tidyr)
s <- split(Data_read, Data_read$group)
Tab_summary1<-t(sapply(s, function(x) return(c(mean(x$ints),length(x$group)) )))

Tab_summary2<-cbind(aggregate(ints~group,data = Data_read,FUN=length),aggregate(ints~group,data = Data_read,FUN=mean))
Tab_summary2<-Tab_summary2[,-3]


