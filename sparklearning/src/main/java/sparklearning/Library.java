package sparklearning;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.*;

public class Library {
	
	public static void main(String[] args) throws IOException {
		Logger log = LoggerFactory.getLogger(Library.class);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> intRdd = sc.textFile("pathToYourFile/IntegerDataFile.txt").flatMap(line -> {
			List<Integer> list = new ArrayList<>();
			for (String str : line.split(",")) {
				list.add(Integer.parseInt(str.trim()));
			}
			return list.iterator();
		});
		log.info("IntegerRdd: {}", intRdd.collect().toString());

		JavaRDD<Integer> squaredRdd = intRdd.map(x -> x * x);
		log.info("SquaredRdd: {}", squaredRdd.collect().toString());

		JavaRDD<Integer> unionRdd = intRdd.union(squaredRdd);
		log.info("UnionRdd: {}", unionRdd.collect().toString());

		JavaRDD<Integer> intersectionRdd = unionRdd.intersection(intRdd);
		log.info("IntersectionRdd: {}", intersectionRdd.collect().toString());

		JavaRDD<Integer> filteredRdd = squaredRdd.filter(x -> x > 200);
		log.info("FilteredRdd: {}", filteredRdd.collect().toString());

		JavaRDD<Integer> distinctRdd = filteredRdd.distinct();
		log.info("DistinctRdd: {}", distinctRdd.collect().toString());

		int sum = filteredRdd.reduce((a, b) -> a + b);
		log.info("Sum: {}", sum);

		long count = filteredRdd.count();
		log.info("Count: {}", count);

		List<Integer> arrayOf5Elements = filteredRdd.take(5);
		log.info("ArrayOf5Elements: {}", arrayOf5Elements.toString());

		int firstElement = filteredRdd.first();
		log.info("FirstElement: {}", firstElement);

		sc.close();

	}

}
