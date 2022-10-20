library(readr)
library(rmr2)
library(rhdfs)
hdfs.init()

rmr.options(backend = "local")
rmr.options(backend = "hadoop")


# Word count --------------------------------------------------------------

ebookLocation <- "/home/jpovh/Rscripts/big-data-training/data/ullyses.txt"
ebookLocation_output <- "/home/jpovh/Rscripts/big-data-training/data/ullyses_output"
system('hadoop fs -copyFromLocal /home/jpovh/Rscripts/big-data-training/data/ullyses.txt /tmp/ullyses.txt')
hdfs.ls("/user/jpovh/")
hdfs.ls("/tmp/")
ebookLocation_hdfs <- "/tmp/ullyses.txt"
wikiLocation_hdfs <- "/public/wiki_1k_lines"

file.info(ebookLocation)$size
file.remove(ebookLocation_output)



m <- mapreduce(input = ebookLocation_hdfs,
#               output = ebookLocation_hdfs,
               input.format  =  "text",
               
               map = function(k, v){
                 words <- unlist(strsplit(v, split = "[[:space:][:punct:]]"))
                 words <- tolower(words)
                 words <- gsub("[0-9]", "", words)
                 words <- words[words != ""]
                 wordcount <- table(words)
                 keyval(
                   key = names(wordcount),
                   val = as.numeric(wordcount)
                 )
               },
               
               reduce = function(k, counts){
                 keyval(key = k,
                        val = sum(counts))
               }
)



# Retrieve results and prepare to plot ------------------------------------


x <- from.dfs(m)
dat <- data.frame(
  word  = keys(x),
  count = values(x)
)
dat <- dat[order(dat$count, decreasing=TRUE), ]
head(dat, 50)

