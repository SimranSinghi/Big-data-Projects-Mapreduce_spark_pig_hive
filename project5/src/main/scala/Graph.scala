import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5
 
  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val graph: RDD[(Int,Int)]
         = sc.textFile(args(0))
             .map( line => { val split = line.split(",")
        (split(1).toInt,split(0).toInt) } )                // create a graph edge (i,j), where i follows j

    var R: RDD[(Int,Int)]             // initial shortest distances
         = graph.groupByKey()
                .map{ 
        case(keyval,valueval)=> 
          // for(i <- keyval){
            if(keyval == start_id || keyval == 1){
              (keyval,0)
            }else{
              (keyval,max_int)
            }
          // }; 
      }            // starting point has distance 0, while the others max_int

    for (i <- 0 until iterations) {
        R = R.join(graph)
      
            // .map{
            //   case(key,value)=>
            //     (key,value._1)
            //     }
          .flatMap{
              case(key,value)=>
                val distance = value._1
                if (distance < max_int){
                  List((key,distance),(value._2,distance+1))
                }else{
                  List((key,distance))
                }
            }        // calculate distance alternatives
            .reduceByKey((a, b) => if (a < b) a else b) // for each node, find the shortest distance
    }  
    

    R.filter{case(key,value)=> value < max_int}                 // keep only the vertices that can be reached
     .sortByKey()
     .collect()
     .foreach(println)
    
  }
}
