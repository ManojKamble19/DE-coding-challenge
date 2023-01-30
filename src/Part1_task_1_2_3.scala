package pack
import org.apache.spark._
import scala.io.Source._
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.io.Source
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import java.net.URL
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.window
import scala.collection.immutable.ListMap

object Part1_task_1_2_3 {
	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setMaster("local[*]").setAppName("first")

					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder.getOrCreate()
					import spark.implicits._

					//Part 1 -
					//Task 1

					val url = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"
					sc.addFile(url)
					val dataRDD = sc.textFile(SparkFiles.get("groceries.csv"))
					val data1 = dataRDD.map(x => x.split(","))
					val data = data1.take(5)
					//data.foreach(x=>println(x.mkString(",")))


					//Task 2 (a)
					val distinctRDD = dataRDD.flatMap(x => x.split(","))
					val distinctRDD1 = distinctRDD.distinct
					//distinctRDD1.coalesce(1).saveAsTextFile("File:///D://out/out_1_2a.txt")

					// (b)
					val productRDD = distinctRDD.count()
					//sc.parallelize(Seq(productRDD)).coalesce(1).saveAsTextFile("File:///D://out/out_1_2b.txt")


					//Task 3
					val productIndividualCount = distinctRDD.countByValue()

					//sorting products by their count
					val sortedProducts = ListMap(productIndividualCount.toSeq.sortWith(_._2 > _._2): _*)
					//Top 5 purchased products
					val res = sortedProducts.take(5)

					//sc.parallelize(Seq(res)).coalesce(1).saveAsTextFile("File:///D://out/out_1_3.txt")
	}

}