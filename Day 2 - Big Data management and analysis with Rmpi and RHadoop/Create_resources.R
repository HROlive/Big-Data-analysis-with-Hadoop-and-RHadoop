work_dir=paste("/home", Sys.getenv("USER"),"resources", sep="/")

if (file.exists(work_dir)){
  unlink(work_dir,recursive=TRUE)
}
setwd(paste("/home", Sys.getenv("USER"), sep="/"))
system("git clone https://bitbucket.org/bdtrain/resources.git")
setwd(work_dir)
system("git pull")



dir()
dir( "data/" )
