import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class AggregateVisitNumbers {
    private static CassandraConnector client = new CassandraConnector();
    private static Map<String, Integer> resultMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("My application").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Session session = client.getSession();
        ResultSet countryCodesSet = session.execute("select countryCode from uservisitsrecords.user_visits");
        List<String> countryCodes = new ArrayList<String>(10000);
        for(Row countryCode: countryCodesSet) {
            countryCodes.add(countryCode.toString());
        }
        System.out.println(countryCodes.size());
        JavaRDD<String> cassandraRdd = sc.parallelize(countryCodes);

        JavaPairRDD<String,Integer> tuples  = cassandraRdd.mapToPair(countryCode -> {
            return new Tuple2<>(countryCode, 1);
        }).reduceByKey((v1, v2) -> (Integer)v1 + (Integer) v2).sortByKey(false);
        tuples.foreach(data -> {
            System.out.println("model="+data._1() + " label=" + data._2());
        });
        JavaPairRDD<Integer,String> swappedTuples = tuples.mapToPair(Tuple2::swap).sortByKey(false);
        //swappedTuples.sortByKey().top(10);

        List<Integer> top10Counters = new ArrayList<>();
        List<String> top10Countries = new ArrayList<>();
        System.out.println(swappedTuples);


        IntStream.range(0, 10).forEach(i -> top10Counters.add(swappedTuples.collect().get(i)._1));
        IntStream.range(0, 10).forEach(i -> top10Countries.add(swappedTuples.collect().get(i)._2));

       for(int i=0; i<10; i++)
           System.out.println(top10Countries.get(i)+":"+top10Counters.get(i)+" times");

        sc.close();
    }

}
