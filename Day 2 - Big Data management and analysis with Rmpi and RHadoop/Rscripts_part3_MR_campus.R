library(rmr2)
library(rhdfs)
hdfs.init()


hdfs.ls("/user/jpovh")
hdfs.ls("/public/")
hdfs.ls("/tmp/")
hdfs.ls("/")
system("hdfs dfs -ls")

system("hdfs fsck /tmp/CEnetBig")

system('curl https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/639388c2cbc2120a14dcf466e85730eb8be498bb/iris.csv | hadoop fs -appendToFile - /tmp/iris_new.csv')
system('curl file:///home/jpovh/R/data/iris.csv | hadoop fs -appendToFile - /tmp/iris1.csv')
system('hadoop fs -copyFromLocal  /home/jpovh/R/data/iris.csv /tmp/iris2.csv')


hdfs.rm("/tmp/iris2.csv")

myDir=paste("/home", Sys.getenv("USER"),'Rscripts/resources', sep="/")
#campus
myDir=paste("/home", Sys.getenv("USER"),'resources', sep="/")
myDir_Data=paste(myDir,"data", sep="/")
dir(myDir_Data)

Data<-read.table(file=paste0(myDir_Data,'/Data_Ex_1.txt'),header = TRUE)

myFile=paste("/user", Sys.getenv("USER"), "OurSmallData", sep="/")
myDir=paste("/user", Sys.getenv("USER"), sep="/")

hdfs.rm(myFile)
OurSmallData=to.dfs(Data, myFile,format="native")
hdfs.ls(myDir)
SmallData1_DFS=from.dfs(OurSmallData)

CEnetBIG<-from.dfs("/tmp/CEnetBig")

iris_dfs<-from.dfs("/tmp/iris1.csv",format = "csv")

mapper = function (., X) {
  M=max(X[,2]);
  keyval(1,M)
}


mapper1 = function (., X) {
  keyval(1,1)
}


mapper2 = function (., X) {
  M=max(X[,2]);
  keyval(1:3,list(1,M,dim(X)[1]))
}


reducer = function(k, A) {
  keyval(k, list(Reduce("max", A))) # take maximum of maxima
}


reducer1 = function(k, A) {
  keyval(k, list(Reduce("+", A))) # take maximum of maxima
}



reducer2 = function(k, A) {
  if(k==1){
    keyval(k, list(Reduce("+", A))) # take sum of maxima
  } else if (k==2) {
    keyval(k, list(Reduce("max", A))) # take maximum of maxima
  } else {
    keyval(k, A)
  }
}


GlobalMaxMR = from.dfs(
  mapreduce(
    input = "/tmp/CEnetBig",
    map = mapper,
    reduce = reducer
  )
)   


#  Finding maximum, number of map calls and block sizes
GlobalMaxNumMR = from.dfs(
  mapreduce(
    input = "/tmp/CEnetBig",
    map = mapper2,
    reduce = reducer2
  )
)   


#second example
mapper_mean = function (., X) {
  n=nrow(X);
  mi=sum(X[,2]);
  keyval(1:2,list(n,mi));
}	


reducer_mean = function(k, A) {
  keyval(k,list(Reduce('+', A)))
}

Block_means <-   from.dfs(
  mapreduce(
    input = "/tmp/CEnetBig",
    map = mapper_mean,
    reduce = reducer_mean
  )
)

GlobalMean=Block_means$val[[2]]/Block_means$val[[1]]


#third  example
mapper_var = function (., X) {
  n=nrow(X);
  mi=sum(X[,2]);
  si=sum(X[,2]^2);
  keyval(1:3,list(n,mi,si));
}	


reducer_var = function(k, A) {
  keyval(k,list(Reduce('+', A)))
}

Block_var <-   from.dfs(
  mapreduce(
    input = "/tmp/CEnetBig",
    map = mapper_var,
    reduce = reducer_var
  )
)


globalVar=Block_var$val[[3]]/Block_var$val[[1]]-(Block_var$val[[2]]/Block_var$val[[1]])^2

CEnetBigloc<-from.dfs("/CEnetBig")
to.dfs(CEnetBigloc)
mean(CEnetBigloc$val[,2])
var(CEnetBigloc$val[,2])
rm(CEnetBigloc)


#plyrmr
#system("curl -o  /home/campus04/R/Rlibs/plyrmr_0.6.0.tar.gz https://github.com/RevolutionAnalytics/plyrmr/releases/download/0.6.0/plyrmr_0.6.0.tar.gz")
#dir("/home/campus04/R/Rlibs")
#install.packages("/home/campus04/R/Rlibs/plyrmr_0.6.0.tar.gz",repos = NULL)


#challenge
mapper_FIND = function (., X) {
  r_sum=rowSums(X[,2:13]);
  ind_s=sum(r_sum>30000);
  keyval(1,list(ind_s));
}	


reducer_FIND = function(k, A) {
  keyval(k,list(Reduce('+', A)))
}

Count_large <-from.dfs(
  mapreduce(
    input = "/tmp/CEnetBig",
    map = mapper_FIND,
    reduce = reducer_FIND
  )
)


#fourth example Covariance matrix
mapperSS = function (., X) {
  ni=nrow(X);
  si=colSums(X[,2:13]);
  SSi=t(X[,2:13])%*%X[,2:13];
  keyval(1:3,list(ni,si,SSi));
}

reducerSS = function(k, A) {
  keyval(k,list(Reduce('+', A)))
}

CovMatrixRaw <-   from.dfs(
  mapreduce(
    input = "/tmp/CEnetBig",
    map = mapperSS,
    reduce = reducerSS
  )
)

meanVec <- CovMatrixRaw$val[[2]]/CovMatrixRaw$val[[1]]  
CovMat <- CovMatrixRaw$val[[3]]/CovMatrixRaw$val[[1]] -outer(meanVec,meanVec)
