#!/bin/sh

docker system prune

docker run -it \
     --name sparkmaster \
     spark \
     spark-submit --master local --deploy-mode client --class nl.javadb.LoadData --name RunSpark --driver-memory 1024M --driver-cores 1 --executor-memory 1G ./target/scala-2.12/schipholairport_2.12-0.1.jar
