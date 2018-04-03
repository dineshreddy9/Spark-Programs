// Databricks notebook source
val reviewfile = sc.textFile("/FileStore/tables/ratings.csv")
val movieidRating = reviewfile.map(line=>(line.split(",")(1),line.split(",")(2))).filter(line=>(line._2!="rating")).map(line=>(line._1,line._2.toFloat)).groupByKey().map(line=>(line._1,line._2.toList)).map(line=>(line._1,line._2.sum/line._2.length))
val tagsfile = sc.textFile("/FileStore/tables/tags.csv").map(line=>(line.split(",")(1),line.split(",")(2))).filter(line=>(line._2=="action"))
val moviesfile = sqlContext.read.format("csv").option("header","true").load("/FileStore/tables/movies.csv")
val mov = moviesfile.select("movieId","title","genres");
val mov2 = mov.rdd.map(row=>row.mkString("_")).filter(line=>(line.split("_")(2).contains("Thrill"))).map(line=>(line.split("_")(0),line.split("_")(1)+"\t"+line.split("_")(2)))
val result = tagsfile.join(mov2)
val result2 = movieidRating.join(result)
val result3 = result2.map(line=>(line._2._1+"\t"+line._2._2._2.split("\t")(0)))
println("AvgRating"+"\t"+"Title")
result3.collect.foreach(println)

// COMMAND ----------


