package com.mastercan.statics
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)
case class Rating(val uid:Int,val mid:Int,val score:Double,val timestamp:Int)
case class MongoConfig(val uri:String,val db:String)
case class Recommendation(mid :Int,score:Double)
case class GenresRecommendation(genres:String,recs:Seq[Recommendation])

object Statistics {
    val MONGODB_RATING_COLLECTION="Rating"
    val MONGODB_MOVIE_COLLECTION="Movie"

    val RATE_MORE_MOVIES="RateMoreMovies"
    val RATE_MORE_RECENTLY_MOVIES="RateMoreRecentlyMovies"
    val AVERAGE_MOVIES="AverageMovies"
    val GENES_TOP_MOVIES="GenesTopMovies"

    def main(args: Array[String]): Unit = {
        val config=Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender"
        )

        val sparkConf = new SparkConf().setAppName("Statistics").setMaster(config("spark.cores"))
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        import spark.implicits._

        val ratingDF=spark
            .read
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Rating]
            .toDF()

        val movieDF=spark
            .read
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Movie]
            .toDF()

        ratingDF.createOrReplaceTempView("ratings")



        //历史热门电影统计
        val rateMoreMoviesDF=spark.sql("select mid,count(mid) as count from ratings group by mid")

        rateMoreMoviesDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",RATE_MORE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


        //最近热门电影统计
        val simpleDateFormat=new SimpleDateFormat("yyyyMM")

        spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x*1000L)).toInt)

        val ratingOfYearMonth = spark.sql("select mid,score,changeDate(timestamp) as yearmonth from ratings")

        ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

        val rateMoreRecentlyMovies = spark.sql("select mid,count(mid) as count,yearmonth from ratingOfMonth group by yearmonth,mid")

        rateMoreRecentlyMovies
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",RATE_MORE_RECENTLY_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


        //统计每个电影的平均评分
        val averageMoviesDF=spark.sql("select mid,avg(score) as avf from ratings group by mid")

        averageMoviesDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",AVERAGE_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql").save()


        //每个类别优质电影的统计
        val movieWithScore = movieDF.join(averageMoviesDF,Seq("mid","mid"))

        val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy",
            "Foreign","History","Horror","Music","Mystery","Romance","Science","Tv","Thriller","War","Western")

        val genresRDD=spark.sparkContext.makeRDD(genres)

        val genrenTopMovies=genresRDD.cartesian(movieWithScore.rdd)
                .filter{
                    case (genres,row) =>
                        row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
                }
                .map{
                    case (genres,row) =>{
                        (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avf")))
                    }
                }.groupByKey()
                .map{
                    case (genres,items) => GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2).take(10)
                        .map(item =>Recommendation(item._1,item._2)))
                }.toDF()

        genrenTopMovies
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",GENES_TOP_MOVIES)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


        spark.stop()
    }







}
