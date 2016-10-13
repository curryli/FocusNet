import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable 
import scala.io.Source  


object FocusNet {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("FocusNet") 
    val sc = new SparkContext(conf)
     
//    // 读入时指定编码  
    val vFile = sc.textFile("xrli/FocusNet/AllCardDicts.txt") 
    
    val verticeRDD = vFile.map { line=>
        val lineArray = line.split("\\s+")
        val card = lineArray(0)
        val vid = lineArray(1).toLong
        (vid,card)      //这里设置顶点编号就是前面的卡编号
    }
    

    val TransRDD = sc.textFile("xrli/FocusNet/MapedTrans.txt") 
    val TransPair = TransRDD.map{line =>
      val lineArray = line.split("\\s+")
        val srcId = lineArray(0).toLong
        val dstId = lineArray(1).toLong
        val amount = lineArray(2).toDouble
        val region = lineArray(3).toInt
        //val time = lineArray(4).toInt  暂时不用
        ((srcId, dstId), (amount,region))
    }
    
    
    val tempSet: mutable.HashSet[Int] = new mutable.HashSet()
      
    val regionReduce = TransPair.map{case((srcId, dstId), (amount,region)) => 
      if(tempSet.contains(region))
         ((srcId, dstId), (amount,0 ,1))
         else{
           tempSet.add(region)
           ((srcId, dstId), (amount,1 ,1))
         }     
    }
    
   val eFile =  regionReduce.reduceByKey((x,y)=> ((x._1 + y._1),(x._2 + y._2),(x._3 + y._3)))
    
//	val CounttoLongeFile = regionReduce.combineByKey(
//        (v: (Double, Int)) => (v._1, v._2.toLong),                                             //createCombiner: V => C, 
//        (c: (Double, Long), v: (Double, Int)) => ((c._1+ v._1, c._2+ v._2.toLong)),            //mergeValue: (C, V) => C,  
//        (c1: (Double, Long), c2: (Double, Long)) => ((c1._1+ c2._1, c1._2+ c2._2))          //mergeCombiners: (C, C) => C                                           
//    )
	
//  edgeAmountFile.collect().foreach(println)  
    
  
   val edgeRDD = eFile.map {case((srcId, dstId), (amount, regionCount, transCount)) =>
          
       Edge(srcId, dstId, (amount,regionCount, transCount))
    }
   
    // 定义一个默认用户，避免有不存在用户的关系  
    val graph = Graph(verticeRDD, edgeRDD) 
    
    
    println("vertices :")
    graph.vertices.saveAsTextFile("xrli/FocusNet/vertices03")
//graph.vertices.collect().foreach(println)
    
    println("edges :")
    graph.edges.saveAsTextFile("xrli/FocusNet/edges03")
    

    //println("找出图中交易次数大于4的边：")
    graph.edges.filter{e => 
      val transCount = e.attr._3
      transCount > 4
    }saveAsTextFile("xrli/FocusNet/frequentEdge03")         //.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    

    val inDegrees: VertexRDD[Int] = graph.inDegrees
    //inDegrees.collect().foreach(v => println("vid: " + v._1 + " inDeg: " + v._2))
    
    val DegInGraph = graph.outerJoinVertices(graph.inDegrees){
      (vid, card, inDegOpt) => (card, inDegOpt.getOrElse(0))}
  
//    println("Graph attr: card and indeg:")
//    DegInGraph.vertices.collect.foreach(v => println("vid: " + v._1 + " card: " + v._2._1 + " inDeg: " + v._2._2))
    
    val DegInOutGraph = DegInGraph.outerJoinVertices(graph.outDegrees){
      (vid, p, outDegOpt) => (p._1, p._2, outDegOpt.getOrElse(0))}
    
    val result =  DegInOutGraph.vertices.map(v => "vid: " + v._1 + " card: " + v._2._1 + " inDeg: " + v._2._2 + " OutDeg: " + v._2._3)
    result.saveAsTextFile("xrli/FocusNet/DegInout")
    
    sc.stop()
  }
}