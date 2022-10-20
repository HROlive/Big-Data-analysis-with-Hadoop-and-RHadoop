#************************************************
#*pepare framework
#************************************************

myDir=paste("/home", Sys.getenv("USER"),'resources', sep="/")
myDir_Data=paste(myDir,"data", sep="/")

ifelse(!dir.exists(myDir), dir.create(myDir), FALSE)
ifelse(!dir.exists(myDir_Data), dir.create(myDir_Data), FALSE)
setwd(myDir)
dir()


#************************************************
# apply
#************************************************
Data_read<-read.table(file='data/Data_Ex_1.txt',header = TRUE)

Data_col_means_1 <- colMeans(Data_read[,-1])
Data_col_means_2 <- apply(Data_read[,-1],2,FUN =mean)

Data_row_means_1 <- rowMeans(Data_read[,-1])
Data_row_means_2 <- apply(Data_read[,-1],1,FUN =mean)

Data_both_squares <- apply(Data_read[,-1],c(1,2),FUN = function(x) return(x^2))


#************************************************
# lapply
#************************************************


Data_col_sums_1 <- apply(Data_read[,-1],2,FUN =sum)
Data_col_sums_2 <- lapply(Data_read[,-1],FUN =sum)

typeof(Data_col_sums_1)  
typeof(Data_col_sums_2)  


Data_abs <- lapply(Data_read[,-1],FUN =abs)
Data_sq <- lapply(Data_read[,-1],FUN = function(x){x^2})

typeof(Data_abs)
length(Data_abs)


typeof(Data_sq)
length(Data_sq)


#************************************************
# sapply
#************************************************


Data_col_sums_1 <- apply(Data_read[,-1],2,FUN =sum)
Data_col_sums_2 <- lapply(Data_read[,-1],FUN =sum)
Data_col_sums_3 <- sapply(Data_read[,-1],FUN =sum)

typeof(Data_col_sums_1)  
typeof(Data_col_sums_2)  
typeof(Data_col_sums_3)  


Data_col_sums_4 <- lapply(list(Data_read$ints,Data_read$reals),FUN =sum)
Data_col_sums_5 <- sapply(list(Data_read$ints,Data_read$reals),FUN =sum)
Data_col_len_1 <- lapply(list(Data_read$ints,Data_read$reals),FUN =length)
Data_col_len_2 <- sapply(list(Data_read$ints,Data_read$reals),FUN =length)

#************************************************
#*packages for parallel and rand
#************************************************

needed.packages <- c("foreach", "doParallel","parallel","tictoc","pracma","doMC")
new.packages <- needed.packages[!(needed.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library(foreach)
library(doParallel)
library(parallel)
library(tictoc)
library(pracma)
library(doMC)
#************************************************
# for loop
#************************************************
N=300
K=30
set.seed(2021)
sum_rand=rep(0,K-1);
tic()
for (i in c(1:K)){
  A=rand(N,N)
  sum_rand[i]=sum(A)
}
time_for=toc()


#************************************************
#  foreach do
#************************************************
set.seed(2021)
sum_rand=rep(0,K-1);
tic()
foreach (i = c(1:K)) %do% {
  A=rand(N,N)
  sum_rand[i]=sum(A)
}
time_foreach=toc()

#************************************************
#  foreach dopar - no cluster
#************************************************
set.seed(2021)
sum_rand=rep(0,K-1);
tic()
print("for each-dopar (no cluster)")
foreach (i = c(1:K)) %dopar% {
  library(pracma)
  A= rand(N,N)
  sum_rand[i]=sum(A)
}
time_foreach_dopar=toc()



#************************************************
#  foreach dopar - with cluster
#************************************************
set.seed(2021)
clust <- makeCluster(12, type="PSOCK")  
registerDoParallel(clust,cores=12)  # use multicore, set to the number of our cores - needed for foerach dopar

sum_rand=rep(0,K-1);
tic()
print("for each-dopar (cluster allocated)")
foreach (i = c(1:K)) %dopar% {
  library(pracma)
  A=rand(N)
  sum_rand[i]=sum(A)
}
time_foreach_dopar_1=toc()
registerDoSEQ()

#************************************************
#  apply and parallel apply
#************************************************
mat_sum<-function(x){
  library(pracma)
  A=rand(x)
  return(sum(A))
}

time_lapply<-system.time({
  set.seed(2021)
  sum_rand_lapply=lapply(rep(N,K),FUN=mat_sum)
})

time_sapply<-system.time({
  set.seed(2021)
  sum_rand_sapply=sapply(rep(N,K),FUN=mat_sum)
})

# forking
time_mcLapply<-system.time({
  set.seed(2021)
  sum_rand_mcLapply=mclapply(X=rep(N,K),FUN=mat_sum,mc.cores = 12)
})

#forking with foreach dopar
time_foreach_dopar_fork<-system.time({registerDoMC(cores = 12) # make a fork cluster
  sum_rand=c()
  foreach (i=1:20, .combine = 'c') %dopar% {
    A=rand(N,N)
    sum_rand[i]=sum(A)}
  registerDoSEQ()
}
) # time the fork cluster



# socketing
time_parLapply<-system.time({
  clust <- makeCluster(12, type="PSOCK")  
  set.seed(2021)
  sum_rand_parLapply=parLapply(clust,rep(N,K),fun=mat_sum)
  stopCluster(clust)
})



time_parSapply<-system.time({
  clust <- makeCluster(12, type="PSOCK")  
  set.seed(2021)
  sum_rand_parSapply=parSapply(clust,rep(N,K),FUN=mat_sum)
  stopCluster(clust)
})


times_apply<-rbind(time_lapply,time_sapply,time_parLapply,time_parSapply,time_mcLapply,time_foreach_dopar_fork) 
print(times_apply[,1:3])

