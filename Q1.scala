// Databricks notebook source
//0	   1,2,3,4,..
var adjFile = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val tabSep = adjFile.map(line=>line.split("\t")).filter(line=>(line.size==2))
val commaSep = tabSep.map(line=>(line(0),line(1).split(",")))
val friendPair = commaSep.flatMap(t=>(t._2.map(s=>((t._1.toInt,s.toInt), t._2.flatMap(y=>y.split(" "))))))
val sortedPair = friendPair.map(t=>(sort(t._1._1, t._1._2), t._2))
def sort(x:Int, y:Int): (Int,Int) = {
  if(y>x){
    return (x,y)
  }
  else{
    return (y,x)
  }
}
val redSortedPair = sortedPair.groupByKey()
val mutFriends = redSortedPair.map(t=>(t._1,t._2.toList(1).toList.intersect(t._2.toList(0).toList)))
val filtmutFriends = mutFriends.filter(t=>(t._1._1==0&&t._1._2==4||t._1._1==20&&t._1._2==22939||t._1._1==1&&t._1._2==29826||t._1._1==6222&&t._1._2==19272||t._1._1==28041&&t._1._2==28056))
val mutualFriends = filtmutFriends.map(t=>(t._1._1+"\t"+t._1._2+"\t"+t._2))
mutualFriends.collect().foreach(println)

// COMMAND ----------


