#load sources

library(Rmpi)
N=100
n=10
#

size <- Rmpi::mpi.comm.size(0)
rank <- Rmpi::mpi.comm.rank(0)
host <- Rmpi::mpi.get.processor.name()
if (rank == 0){
	print('I am the master')
} else {
  where=getwd()
  A=matrix(rnorm(N),nrow=n)
  A=A+t(A)
  a = max(eigen(A)$values)
  print(paste("I am", rank, "of", size, "running on", host,"and the frist eigenvalue of my matrix is",a,"with pwd =",where))
}