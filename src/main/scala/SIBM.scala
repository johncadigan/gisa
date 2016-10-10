package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import scala.io.Source
import java.io.BufferedWriter
import java.io.BufferedReader
import java.io.File
import java.io.FileWriter
import java.io.FileReader
import org.apache.spark.HashPartitioner

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.MutableList
import scala.collection.mutable.HashMap
import scala.collection.parallel._

object IBM1Helper{
  
    // maps a sentence combinations to contexts of a target word and its possible source words
    def context_mapper(pair : Tuple2[Array[Int], Array[Int]], combo_map : Map[Tuple2[Int,Int], Int]): Array[Tuple2[Int, Array[Int]]] = {
        val contexts = Array.ofDim[Tuple2[Int, Array[Int]]](pair._2.size) //target word contexts
        var i = 0
        var c = 0
        for(t <- pair._2){
           i = 0
           val context = Array.ofDim[Int](pair._1.size)
           for(s <- pair._1){
               context(i) = combo_map(s,t)
               i+=1
           }
           contexts(c) = Tuple2(t, context)
           c += 1
        }
        return contexts
    }
    //used for counting the c(e,f) expectation
    def expectation_counter(input: Array[Int] , prob: Array[Double]) : Array[Tuple2[Int,Double]] = {          
          
          var res = Array.ofDim[Tuple2[Int,Double]](input.size)
          var index = 0;
          var delta = 0.0
          for(i <- input){
               delta += prob(i)
          }
          for(i <-input){
             res(index) = (i, prob(i) / delta)
             index+=1
          }
          return res;
    }
    
    def normalize(f_counts : Array[Tuple2[Int,Double]]) : Array[Tuple2[Int,Double]] = {
    var total = 0.0
    for(x <- f_counts){
       total += x._2
    }
    
    for(idx <-Range(0, f_counts.size)){
       f_counts(idx) = (f_counts(idx)._1, f_counts(idx)._2/total)
   }  
   return f_counts;
   }


    def alignments(combined : Tuple2[Array[Int], Array[Int]],  combo_map : Map[Tuple2[Int,Int], Int], prob: Array[Double], null_num : Int): Array[Tuple2[Int,Int]] = {
          
          
             val alignments = scala.collection.mutable.ArrayBuffer[Tuple2[Int,Int]]()
             val source = combined._1
             val target = combined._2
             var max = 0.0
             var tup = (-1,-1)
             
             var prev_e_state = -1
             for(y<- Range(0, target.size)){
               max = -1.0
               tup = (-1,-1)
               for (x<- Range(0, source.size)){
                   var score = prob(combo_map((source(x.toInt),target(y.toInt))))
                    
                   if(score > max) {   
                     max = score
                     tup =  (x.toInt,y.toInt)
                   }
               }
               if (source(tup._1)!=null_num) alignments append tup
             }
          return alignments.toArray
     }
}






object SIBM {

  
  def main(args: Array[String]) {
    
    val logFile = "runtime.txt" // Should be some file on your system
    val file = new File(logFile)
    println("System test 2")
    var systemText = ""
    val conf = new SparkConf().setAppName("Spark IBM Model1")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryoserializer.buffer.mb", ".128")
    //conf.set("spark.kryoserializer.buffer.max.mb", "1024")
    conf.set("spark.driver.maxResultSize", "3g")
    conf.set("spark.akka.frameSize", "512") 

    conf.set("spark.default.parallelism", args(4))
    //conf.set("spark.storage.memoryFraction", args(5))
    //conf.set("spark.shuffle.memoryFraction", args(6))
    conf.set("spark.executor.heartbeatInterval", "20")
    //conf.set("spark.akka.timeout", "300") 
    //conf.set("spark.storage.blockManagerHeartBeatMs", "300000")
    conf.set("spark.akka.threads", "8")    
    conf.set("spark.network.timeout", "600")
    conf.set("spark.cores.max", "60")    
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///home2/jcadigan/tmp")
    val sc = new SparkContext(conf)		
    val start = System.currentTimeMillis()
       
    val fs = sc.textFile((args(0)))
    val ft = sc.textFile((args(1)))
    val null_value = "NULL#!@"
    val source = fs.map(x=>x.stripLineEnd.split("\\s+").filter(x=>x.size > 0):+null_value)
    val source_words = source.flatMap(x=>x).distinct().collect()
    val source_map = source_words.zipWithIndex.toMap
    val source_sents = source.map(x=>x.map(y=>source_map(y))).collect()
    source.unpersist()
    fs.unpersist() 
    //val null_value = "NULL#!@"
    val target = ft.map(x=>x.stripLineEnd.split("\\s+").filter(x=>x.size > 0))
    val target_words = target.flatMap(x=>x).distinct().collect()
    ft.unpersist()
    target.unpersist()    
    val target_map = target_words.zipWithIndex.toMap
    val target_sents = target.map(x=>x.map(y=>target_map(y))).collect() 
    val map_target = target_map.map(_.swap)
    
        
    //For local file reading if there are issues with the Hadoop file system
    /*
    val fs = Source.fromFile((args(0))).getLines()
    val ft = Source.fromFile((args(1))).getLines()  
    
    val null_value = "NULL#!@"
    val source = fs.toArray.par.map(x=>x.stripLineEnd.split("\\s+").filter(x=>x.size > 0):+null_value)
    val source_words = source.flatMap(x=>x).distinct.seq.toArray
    val source_map = source_words.zipWithIndex.toMap
    val source_sents = source.map(x=>x.map(y=>source_map(y))).seq.toArray  
    
    val target = ft.toArray.par.map(x=>x.stripLineEnd.split("\\s+").filter(x=>x.size > 0))
    val target_words = target.flatMap(x=>x).distinct.seq.toArray
    val target_map = target_words.zipWithIndex.toMap
    val target_sents = target.map(x=>x.map(y=>target_map(y))).seq.toArray
    val map_target = target_map.map(_.swap)
    */
    var curtime = System.currentTimeMillis()
    systemText += "Numeric mapping complete seconds: %s\n".format((curtime-start)/1000)
    var lasttime = curtime
    
    println("Starting mapping")
    val combined = sc.parallelize(source_sents.zip(target_sents))
    val comb_set = combined.flatMap(sents=>sents._1
         .flatMap(source_w=>sents._2.map(target_w=>Tuple2(source_w,target_w))))
         .distinct().collect().toArray
    combined.unpersist()
    curtime = System.currentTimeMillis()
    systemText += "%s combinations enumerated: %s seconds\n".format(comb_set.size,(curtime-lasttime)/1000)
    lasttime = curtime
    val combo_map = comb_set.toArray.zipWithIndex.toMap
    val combo_source_map = Array.ofDim[Int](comb_set.size)
    comb_set.foreach(x=>(combo_source_map(combo_map(x))=x._1))
    val combo_source_map_d = sc.broadcast(combo_source_map)
    curtime = System.currentTimeMillis()
    systemText += "Pair-combination mapping created: %s seconds\n".format((curtime-lasttime)/1000)
    lasttime = curtime
    
    /// every combo is mapped to to an index in an array of probabilities
    /// they are grouped by their target word to ease the calculation of delta
    /// [ Array(  (haus,house), (das,house), ..)
    val comb = source_sents.zip(target_sents).par
    // For larger corpora it is necessary to restrict the number of threads
    // on the driver node because of memory issues with the map
    //comb.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(8))
 
    val combined_m = comb.flatMap(x=>IBM1Helper.context_mapper(x, combo_map)).seq
    
    curtime = System.currentTimeMillis()
    systemText += "Contexts created: %s seconds\n".format((curtime-lasttime)/1000)
    lasttime = curtime 
    println("Mapping complete, starting Spark")    
    val combined_mapped = sc.parallelize(combined_m).partitionBy(new HashPartitioner(args(4).toInt)).cache()//.persist(StorageLevel.MEMORY_AND_DISK)

    curtime = System.currentTimeMillis()
    systemText += "Transformed and and created partitions: %s seconds\n".format((curtime-lasttime)/1000)
    lasttime = curtime    
    
    var probs = Array.fill(combo_map.size)(1.0)
    curtime = System.currentTimeMillis()

    systemText += "Ready to start iterations: %s seconds\n".format((curtime-lasttime)/1000)
    lasttime = curtime
    val mw = new BufferedWriter(new FileWriter(new File(args(3))))
    for(it <- Range(1, args(2).toInt+1)){          
        
        val EMstart = System.currentTimeMillis()
         
        var expectation = combined_mapped
	.flatMap(pair=>IBM1Helper.expectation_counter(pair._2, probs))//E-step
	.reduceByKey(_+_)//M-step 1
        .groupBy(x=>combo_source_map_d.value(x._1))//prepare for M2 and norm
	.flatMap(x=>(IBM1Helper.normalize(x._2.toArray))).collect()//M2 and norm

        expectation.foreach(i=>probs(i._1)=i._2)        
        
        val Mend = System.currentTimeMillis()
        
        systemText +="EM iteration complete %s seconds\n".format((Mend-EMstart)/1000)
 
    }
    val model1 = new BufferedWriter(new FileWriter(new File(args(3))))
    val null_num = source_map(null_value)
    comb.map(pair=>IBM1Helper.alignments(pair, combo_map, probs, null_num)).seq.toArray.foreach(a=>{
        model1.write(a.map(x=>"%s-%s".format(x._1.toString(), x._2.toString())).mkString(" ")+"\n")
    })

    model1.close()
    
    val end = System.currentTimeMillis()
    val sysw = new BufferedWriter(new FileWriter(file))
    systemText += "Total running time: %s seconds\n".format((end-start)/1000)
    sysw.write(systemText)
    sysw.close()
  }
}
