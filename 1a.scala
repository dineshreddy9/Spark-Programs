// Databricks notebook source
val reviewfile = sc.textFile("/FileStore/tables/ratings.csv")
val movieidRating = reviewfile.map(line=>(line.split(",")(1),line.split(",")(2))).filter(line=>(line._2!="rating")).map(line=>(line._1,line._2.toFloat)).groupByKey().map(line=>(line._1,line._2.toList)).map(line=>(line._1+"\t"+line._2.sum/line._2.length))
println("Movieid"+"\t"+"AvgRating")
movieidRating.collect.foreach(println)
movieidRating.coalesce(1).saveAsTextFile("/FileStore/dinesh/1aout.txt")

// COMMAND ----------


