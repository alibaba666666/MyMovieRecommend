package com.mastercan.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


case class Movie(val mid:Int,val name:String,val descri:String,val timeLong:String,val issue:String,
                 val shoot:String,val language:String,val genres:String,val actors:String,val directors:String)

case class Rating(val uid:Int,val mid: Int,val score:Double,val timestamp:Int)

case class Tag(val uid:Int,val mid:Int,val tag:String,val timestamp:Int)

case class MongoConfig(val uri:String,val db:String)

case class ESConfig(httpHosts:String,transportHosts:String,index:String,clustername:String)

object DataLoader {
    val MOVIE_DATA_PATH="D:\\MyWork\\MyMovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH="D:\\MyWork\\MyMovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
    val TAG_DATA_PATH="D:\\MyWork\\MyMovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

    val MONGODB_MOVIE_COLLECTION="Movie"
    val MONGODB_RATING_COLLECTION="Rating"
    val MONGODB_TAG_COLLECTION="Tag"

    val ES_MOVIE_INDEX="Movie"

    def main(args: Array[String]): Unit = {
        val config =Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender",
            "es.httpHosts" -> "hadoop101:9200",
            "es.transportHosts" -> "hadoop101:9300",
            "es.index" -> "recommender",
            "es.cluster.name" -> "es-cluster"
        )

        val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        val movieRDD=spark.sparkContext.textFile(MOVIE_DATA_PATH)

        val movieDF = movieRDD.map(item =>{
            val attr = item.split("\\^")
            Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,
                attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
        }).toDF()
        movieDF.show(2)


        val ratingRDD=spark.sparkContext.textFile(RATING_DATA_PATH)
        val ratingDF = ratingRDD.map(item => {
            val attr = item.split(",")
            Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
        }).toDF()
        ratingDF.show(2)


        val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
        //将tagRDD装换为DataFrame
        val tagDF = tagRDD.map(item => {
            val attr = item.split(",")
            Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)
        }).toDF()
        tagDF.show(2)


        implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)
        storeDataInMongoDB(movieDF,ratingDF,tagDF)

        import org.apache.spark.sql.functions._
        val newTag = tagDF.groupBy($"mid")
            .agg(concat_ws("|",collect_set($"tag"))
            .as("tags"))
            .select("mid","tags")

        val movieWithTagsDF = movieDF.join(newTag,Seq("mid","mid"),"left")
        movieWithTagsDF.show(5)

        // 声明了一个ES配置的隐式参数
        implicit  val esConfig = ESConfig(config.get("es.httpHosts").get,
            config.get("es.transportHosts").get,
            config.get("es.index").get,
            config.get("es.cluster.name").get)
        // 需要将新的Movie数据保存到ES中
        storeDataInES(movieWithTagsDF)

        spark.stop()

    }
    def storeDataInMongoDB(movieDF: DataFrame, ratingDF:DataFrame, tagDF:DataFrame)
                          (implicit mongoConfig: MongoConfig): Unit = {

        //新建一个到MongoDB的连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
        //如果MongoDB中有对应的数据库，那么应该删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

        //将当前数据写入到MongoDB
        movieDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_MOVIE_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        ratingDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
        tagDF
            .write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_TAG_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        //对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        //关闭MongoDB的连接
        mongoClient.close()
    }


    def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESConfig): Unit = {
        //新建一个配置
        val settings:Settings = Settings.builder()
            .put("cluster.name",eSConfig.clustername).build()
        //新建一个ES的客户端
        val esClient = new PreBuiltTransportClient(settings)
        //需要将TransportHosts添加到esClient中
        val REGEX_HOST_PORT = "(.+):(\\d+)".r
        eSConfig.transportHosts.split(",").foreach{
            case REGEX_HOST_PORT(host:String,port:String) => {
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
            }
        }
        //需要清除掉ES中遗留的数据
        if(esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
            esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
        }
        esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

        //将数据写入到ES中
        movieDF
            .write
            .option("es.nodes",eSConfig.httpHosts)
            .option("es.http.timeout","100m")
            .option("es.mapping.id","mid")
            .mode("overwrite")
            .format("org.elasticsearch.spark.sql")
            .save(eSConfig.index+"/"+ES_MOVIE_INDEX)
    }

}
