// Databricks notebook source
//0       1,2,3,4,..
var adjFile = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val tabSep = adjFile.map(line=>line.split("\t")).filter(line=>(line.size==2))
val commaSep = tabSep.map(line=>(line(0),line(1).split(",")))
val friendPair = commaSep.flatMap(t=>(t._2.map(s=>((t._1.toInt,s.toInt), t._2.flatMap(y=>y.split(" "))))))
val sortedPair = friendPair.map(t=>(sort(t._1._1, t._1._2), t._2))
def sort(x:Int, y:Int): (Int, Int) = {
  if(y>x){
    return (x,y)
  }
  else{
    return (y,x)
  }
}
val redSortedPair = sortedPair.groupByKey()
val mutFriends = redSortedPair.map(t=>(t._1,t._2.toList(1).toList.intersect(t._2.toList(0).toList)))
val sortedMutualFriends = mutFriends.sortBy(_._2.size, ascending = false).map(t=>(t._1._1+","+t._1._2+"\t"+t._2.size))
val userdata = sc.textFile("/FileStore/tables/userdata.txt")
val firstSet = sortedMutualFriends.map(line=>line.split(",")(0)).take(10).toSet
val secondSet = sortedMutualFriends.map(line=>line.split(",")(1).split("\t")(0)).take(10).toSet
val userdata1 = userdata.map(line=>line.split(",")).filter(line=>firstSet(line(0))).map(line=>line.mkString(","))
val userdata2 = userdata.map(line=>line.split(",")).filter(line=>secondSet(line(0))).map(line=>line.mkString(","))
val topTenMutual = sortedMutualFriends.take(10)
val final_output = topTenMutual.map(t=>((t.split(",")(1).split("\t")(1))+"\t"+(userdata1.map(x=>(x.split(",")(0), (x.split(",")(1)+"\t"+x.split(",")(2)+"\t"+x.split(",")(3)))).filter(y=>(y._1==t.split(",")(0))).map(z=>z._2.mkString).take(1).mkString) +"\t"+(userdata2.map(x=>(x.split(",")(0),(x.split(",")(1)+"\t"+x.split(",")(2)+"\t"+x.split(",")(3))))).filter(y=>(y._1==t.split(",")(1).split("\t")(0))).map(z=>z._2.mkString).take(1).mkString))
println("Mutno"+"\t"+"FNuserA"+"\t"+"LNuserA"+"\t"+"addressA"+"\t"+"\t"+"FNuserB"+"\t"+"LNuserB"+"\t"+"addressB")
final_output.take(10).foreach(println)

// COMMAND ----------


