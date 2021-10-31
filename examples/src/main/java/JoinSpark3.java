import java.io.*;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

class Emp implements Serializable {
    private static final long serialVersionUID = 53L;
    public String name;
    public int dno;
    public String address;

    Emp ( String n, int d, String a ) {
        name = n; dno = d; address = a;
    }
}

class Dept implements Serializable {
    private static final long serialVersionUID = 54L;
    public String name;
    public int dno;

    Dept ( String n, int d ) {
        name = n; dno = d;
    }
}

public class JoinSpark3 {

    public static void main ( String[] args ) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Join");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Emp> e = sc.textFile(args[0])
            .map(new Function<String,Emp>() {
                    public Emp call ( String line ) {
                        Scanner s = new Scanner(line.toString()).useDelimiter(",");
                        return new Emp(s.next(),s.nextInt(),s.next());
                    }
                });
        JavaRDD<Dept> d = sc.textFile(args[1])
            .map(new Function<String,Dept>() {
                    public Dept call ( String line ) {
                        Scanner s = new Scanner(line.toString()).useDelimiter(",");
                        return new Dept(s.next(),s.nextInt());
                    }
                });
        JavaRDD<String> res = e.mapToPair(new PairFunction<Emp,Integer,Emp>() {
                    public Tuple2<Integer,Emp> call ( Emp e ) {
                        return new Tuple2<Integer,Emp>(e.dno,e);
                    }
                }).join(d.mapToPair(new PairFunction<Dept,Integer,Dept>() {
                        public Tuple2<Integer,Dept> call ( Dept d )  {
                            return new Tuple2<Integer,Dept>(d.dno,d);
                        }
                    })).values()
            .map(new Function<Tuple2<Emp,Dept>,String>() {
                            public String call ( Tuple2<Emp,Dept> x ) {
                                return x._1.name+"  "+x._2.name;
                            }
                });
        res.saveAsTextFile(args[2]);
        sc.stop();
    }
}
