package com.challenge.spark

import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}



object main {
  //Part 1
  def part_1(spark: SparkSession, show: Boolean): DataFrame ={
    val content = spark.sparkContext.textFile("googleplaystore_user_reviews.csv")

    //remove the first line of the file
    val header = content.first()
    val lines = content.filter(row => row != header)

    //get values(App,Sentiment_Polarity) of each line
    val rdd = lines.map(parsLinePart_1)

    //calculate average Sentiment_Polarity for each app
    val result = rdd.groupBy(_._1).mapValues(x => x.map(_._2).sum / x.map(_._2).size)

    //create dataframe
    val df_1 = spark.createDataFrame(result).toDF("App", "Average_Sentiment_Polarity")

    if(show) {
      df_1.printSchema()
      df_1.show()
    }

    (df_1)
  }

  //Part 2
  def part_2(spark: SparkSession): Unit ={
    //read content from file as dataframe
    val content = spark.read.option("header",true).csv("googleplaystore.csv")

    //filter info with rating >= 4.0 && <= 5.0 and order in descending order
    val df_2 = content.filter(content("Rating") >= 4.0 && content("Rating") <= 5.0 && content("Rating") != null && !content("Rating").isNaN).orderBy(content("Rating").desc)

    df_2.printSchema()
    df_2.show(false)

    //write file
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path("Part2/"+ "best_apps.csv"))) {
      //save as .csv
      df_2.coalesce(1).write.csv("Part2/")
      //change filename
      changeFileName(spark, fs, "Part2/", "best_apps.csv")
    }
  }

  //Part 3
  def part_3(spark: SparkSession, show: Boolean): DataFrame ={

    //read content from file as dataframe
    val content = spark.read.option("header",true).csv("googleplaystore.csv")
    val w = Window.partitionBy(col("App")).orderBy("Reviews")

    val df_3 = content.withColumnRenamed("Category","Categories")
      .withColumnRenamed("Content Rating","Content_Rating")
      .withColumnRenamed("Current Ver","Current_Version")
      .withColumnRenamed("Android Ver","Minimum_Android_Version")
      .withColumnRenamed("Last Updated","Last_Updated")
      .withColumn("Genres", split(col("Genres"),";"))
      .withColumn("Rating", col("Rating").cast(DoubleType))
      .withColumn("Reviews", col("Reviews").cast(LongType))
      .withColumn("Price", round(functions.regexp_replace(col("Price"),"\\$","").cast(DoubleType)*0.9))
      .withColumn("Last_Updated", to_date(unix_timestamp(col("Last_Updated"),"MMMMM dd, yyyy").cast("timestamp"), "yyyy-MM-dd hh:mm:ss"))
      .dropDuplicates("App","Categories") // drops duplicated lines with same App and Categories
      .withColumn("Categories", collect_list("Categories").over(w))
      .groupBy("App")
      .agg(max(col("Categories")).as("Categories"), max(col("Rating")).as("Rating")
        ,max(col("Reviews")).as("Reviews"),max(col("Size")).as("Size"),max(col("Installs")).as("Installs"),max(col("Type")).as("Type"),max(col("Price")).as("Price")
        ,max(col("Content_Rating")).as("Content_Rating"),max(col("Genres")).as("Genres"),max(col("Last_Updated")).as("Last_Updated"),max(col("Current_Version")).as("Current_Version")
        ,max(col("Minimum_Android_Version")).as("Minimum_Android_Version"))
      .withColumn("Size",when(col("Size").rlike("k"), round(regexp_replace(col("Size"),"[a-z A-Z]","").cast(DoubleType) * 0.0009765625, 2)).otherwise(functions.regexp_replace(col("Size"),"[a-z A-Z]","").cast(DoubleType)))

    if(show) {
      df_3.printSchema()
      df_3.show()
    }

    (df_3)
  }

  def part_4(spark: SparkSession, show: Boolean): DataFrame ={

    //get dataframes from part 1 and part 3
    val df_1 = part_1(spark,false)
    val df_3 = part_3(spark,false)

    //join dataframes info
    val df_4 = df_3.join(df_1, df_3.col("App") ===  df_1.col("App"))
        .select(df_3.col("App").as("App"), (col("Categories")).as("Categories"), (col("Rating")).as("Rating")
          ,(col("Reviews")).as("Reviews"),(col("Size")).as("Size"),(col("Installs")).as("Installs"),(col("Type")).as("Type"),(col("Price")).as("Price")
          ,(col("Content_Rating")).as("Content_Rating"),(col("Genres")).as("Genres"),(col("Last_Updated")).as("Last_Updated"),(col("Current_Version")).as("Current_Version")
          ,(col("Minimum_Android_Version")).as("Minimum_Android_Version"), df_1.col("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity"))

    if(show) {
      df_4.printSchema()
      df_4.show()
    }

    //write file
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path("Part4/"+ "googleplaystore_cleaned.gz.parquet"))) {
      //save with gzip compression
      df_4.coalesce(1).write.option("compression","gzip").parquet("Part4")
      //change filename
      changeFileName(spark, fs, "Part4/", "googleplaystore_cleaned.gz.parquet")
    }

    (df_4)
  }

  def part_5(spark: SparkSession): Unit={
    //get dataframe from part 4
    val df_4 = part_4(spark,false)

    val df_tmp = df_4.select(col("App"), explode(col("Genres")).alias("Genre"), col("Rating"), col("Average_Sentiment_Polarity"))
      .withColumn("Rating", when(col("Rating").cast(StringType) === ("NaN"), null).otherwise(col("Rating")))
    df_tmp.createOrReplaceTempView("df_tmp")

    val df_5 = spark.sql("SELECT Genre, count(App) as `Count`, round(avg(Rating),2) as `Average_Rating`, round(avg(Average_Sentiment_Polarity),2) as `Average_Sentiment_Polarity` FROM df_tmp group by Genre")
    df_5.printSchema()
    df_5.show()

    //write file
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path("Part5/"+ "googleplaystore_metrics.gz.parquet"))) {
      //save with gzip compression
      df_4.coalesce(1).write.option("compression","gzip").parquet("Part5")
      //change filename
      changeFileName(spark, fs, "Part5/", "googleplaystore_metrics.gz.parquet")
    }
  }

  def parsLinePart_1(line: String): (String, Double) = {
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)

    val name = fields(0).toString()
    val sent_polarity = if (fields(3) == null || fields(3) == "nan") 0 else fields(3).toDouble
    (name,sent_polarity)
  }


  def changeFileName(spark: SparkSession, fs: FileSystem,  path: String, filename: String) : Unit = {
    val file = fs.globStatus(new Path(path+"part*"))(0).getPath().getName()
    fs.rename(new Path(path + file), new Path(path + filename))
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel((Level.ERROR));
    val spark = SparkSession.builder()
      .appName("main")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()

    //To test each part of the challenge
    //part_1(spark,true)
    //part_2(spark)
    //part_3(spark,true)
    //part_4(spark,true)
    //part_5(spark)
  }
}
