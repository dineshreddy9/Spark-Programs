// Databricks notebook source
val reviewfile = sc.textFile("/FileStore/tables/ratings.csv")
val movieidRating = reviewfile.map(line=>(line.split(",")(1),line.split(",")(2))).filter(line=>(line._2!="rating")).map(line=>(line._1,line._2.toFloat)).groupByKey().map(line=>(line._1,line._2.toList)).map(line=>(line._1,line._2.sum/line._2.length))
val tagsfile = sc.textFile("/FileStore/tables/tags.csv").map(line=>(line.split(",")(1),line.split(",")(2))).filter(line=>(line._2=="action"))
val moviesfile = sqlContext.read.format("csv").option("header","true").load("/FileStore/tables/movies.csv")
val mov = moviesfile.select("movieId","title")
val mov2 = mov.rdd.map(row=>row.mkString("_")).map(line=>(line.split("_")(0),line.split("_")(1)))
val result1 = movieidRating.join(tagsfile).map(line=>(line._1,line._2._1))
val result2 = result1.join(mov2).map(line=>(line._1+"\t"+line._2._1+"\t"+line._2._2))
println("movieid"+"\t"+"avgrating"+"\t"+"Title")
result2.collect.foreach(println)
result2.coalesce(1).saveAsTextFile("/FileStore/dinesh/q32output.txt")

// COMMAND ----------


