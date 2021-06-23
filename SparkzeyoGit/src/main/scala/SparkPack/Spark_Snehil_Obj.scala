package SparkPack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import sys.process._

object Spark_Snehil_Obj {
  
  def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					val df1 = spark.read.format("csv").option("header","true").load("file:///D:/data/joindata/j1.csv")
					df1.show()

					val df2 = spark.read.format("csv").option("header","true").load("file:///D:/data/joindata/j2.csv")
					df2.show()
					
					val joindfinner = df1.join(df2,df1("txnno")===df2("txn_number"),"inner")
					println("====================inner join===========================================")
					joindfinner.show(5)
					
					val joindfleft = df1.join(df2,df1("txnno")===df2("txn_number"),"left")
					println("====================left join===========================================")
					joindfleft.show(5)
					
					val joindfright = df1.join(df2,df1("txnno")===df2("txn_number"),"right")
					println("====================right join===========================================")
					joindfright.show(5)
					
					val joindfouter = df1.join(df2,df1("txnnumber")===df2("txnno"),"outer")
					println("====================outer join===========================================")
					joindfouter.show(5)
  
}
}