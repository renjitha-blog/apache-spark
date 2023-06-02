package sparklearning;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import java.io.*;

public class Library {

	public static void main(String[] args) throws IOException {
		Logger log = LoggerFactory.getLogger(Library.class);

		SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
		Dataset<Row> df = spark.read().option("multiline", "true").json("src/main/resources/StockPurchaseData.json");
		//rddExample(log);
		sqlOperations(spark, df, log);
		spark.close();
	}

	/**
	 * Covers Filtering,grouping,ordering, aggregation, formatting using SQL
	 * expressions
	 * 
	 * @param spark
	 * @param df
	 * @param log
	 */
	private static void sqlOperations(SparkSession spark, Dataset<Row> df, Logger log) {
		// Displays the content of Stock Name column to console
		df.select("Stock Name").show();
		log.info("No:of rows:{}", df.count());

		// Filter with expressions
		Dataset<Row> filteredDataWithExpressions = df.filter("Quantity > 20 AND `Purchase Price` < 30.0");
		filteredDataWithExpressions.show();
		// Filter with lambdas
		Dataset<Row> filteredDataWithLambdas = df.filter((FilterFunction<Row>) row -> (Long) row.getAs("Quantity") > 20
				&& (Double) row.getAs("Purchase Price") < 30.0);
		filteredDataWithLambdas.show();
		// Filter using columns
		Dataset<Row> filteredDataWithColumns = df.filter(col("Quantity").gt(20).and(col("Purchase Price").lt(30.0)));
		filteredDataWithColumns.show();

		df.createOrReplaceTempView("financial_data"); // Create a temporary view for the DataFrame
		// Grouping
		Dataset<Row> groupedData = spark
				.sql("SELECT `Stock Name`, SUM(Quantity) as TotalQuantity FROM financial_data GROUP BY `Stock Name`");
		groupedData.show();
		// Aggregation
		Dataset<Row> aggregatedData = spark.sql(
				"SELECT AVG(`Purchase Price`) as AveragePrice, MAX(`Purchase Price`) as MaxPrice FROM financial_data");
		// Date formatting
		Dataset<Row> formattedData = spark.sql(
				"SELECT `Stock Name`, DATE_FORMAT(`Purchase Date`, 'yyyy-MM-dd') as DateofPurchase FROM financial_data");
		// Ordering
		Dataset<Row> orderedData = spark.sql("SELECT * FROM financial_data ORDER BY `Purchase Price` DESC");
		// Grouping and Aggregation: Calculate purchase count and total quantity for
		// each stock and Oder by purchase count
		Dataset<Row> stockSummary = spark.sql(
				"SELECT `Stock Name`, COUNT(*) as PurchaseCount, SUM(Quantity) as TotalQuantity FROM financial_data GROUP BY `Stock Name` ORDER BY `PurchaseCount` DESC");
		stockSummary.show();
	}

	/**
	 * Example for an RDD and basic operations
	 * 
	 * @param log
	 */
	private static void rddExample(Logger log) {
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
