package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;


public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		Dataset<Row> inputDF = ss.read().format("csv")
				.option("delimiter", "\\t")
				.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
				.option("header", true)
				.option("inferSchema", true)
				.load(inputPath);
		
		inputDF.createOrReplaceTempView("register");
				
		ss.udf().register("DayOfTheWeek", (java.sql.Timestamp t) -> DateTool.DayOfTheWeek(t), DataTypes.StringType);
		
		Dataset<Row> filtered = ss.sql(""
				+ "SELECT r.station, DayOfTheWeek(r.timestamp) AS day, hour(r.timestamp) AS hour, r.used_slots, r.free_slots "
				+ "FROM register r "
				+ "WHERE r.used_slots!=0 OR r.free_slots!=0");
				
		filtered.createOrReplaceTempView("filtered");

		Dataset<Row> full = ss.sql(""
				+ "SELECT f.station, f.day, f.hour, COUNT(*) AS full_count "
				+ "FROM filtered f "
				+ "WHERE f.free_slots=0 "
				+ "GROUP BY f.station, f.day, f.hour");
		
		Dataset<Row> tot = ss.sql(""
				+ "SELECT f.station, f.day, f.hour, COUNT(*) AS tot_count "
				+ "FROM filtered f "
				+ "GROUP BY f.station, f.day, f.hour");
		
		full.createOrReplaceTempView("full");
		tot.createOrReplaceTempView("tot");
		
		Dataset<Row> criticality_table = ss.sql(""
				+ "SELECT f.station, f.day, f.hour, f.full_count/t.tot_count AS criticality "
				+ "FROM full f, tot t "
				+ "WHERE f.hour = t.hour AND f.day = t.day AND f.station = t.station");
				
		criticality_table.createOrReplaceTempView("criticality_table");
		
		Dataset<Row> criticality_filtered = ss.sql(""
				+ "SELECT * "
				+ "FROM criticality_table "
				+ "WHERE criticality>="+threshold);
				
		criticality_filtered.createOrReplaceTempView("criticality_filtered");
			
		Dataset<Row> inputDF2 = ss.read().format("csv")
				.option("delimiter", "\\t")
				.option("header", true)
				.option("inferSchema", true)
				.load(inputPath2);
		
		inputDF2.createOrReplaceTempView("stations");
		
		Dataset<Row> join = ss.sql(""
				+ "SELECT c.station, c.day, c.hour, c.criticality, s.latitude, s.longitude "
				+ "FROM stations s, criticality_filtered c "
				+ "WHERE s.id = c.station "
				+ "ORDER BY criticality DESC, station ASC, day ASC, hour ASC");
				
		join.write().format("csv").option("header", true).save(outputFolder);

		// Close the Spark session
		ss.stop();
	}
}
