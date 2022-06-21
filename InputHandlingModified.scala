import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InputHandling {
  def readInput(spark_session: SparkSession): RDD[(String, (String, String, String))] = { //(String, (String, String, String))
/*
    // load dataset ratings
    //val ratings_path = "gs://scc_bucket_1/ratings.csv" // cloud
    val ratings_path = "dataset/ratings.csv" // local
    //val ratings_path = "dataset/rating.csv" // local AND big
    var ds_ratings = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load(ratings_path)
    // load dataset movies


    //val movies_path = "gs://scc_bucket_1/movies.csv" // cloud
    val movies_path = "dataset/movies.csv" // local
    //val movies_path = "dataset/movie.csv" // local AND big
    var ds_movies = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load(movies_path)

    // clean data and join
    ds_ratings = ds_ratings.drop("timestamp")
    ds_ratings = ds_ratings.na.drop()
    ds_movies = ds_movies.drop( "genres")
    ds_movies = ds_movies.na.drop()
    var ds_full = ds_ratings.join(ds_movies, "movieId")
    ds_full = ds_full.dropDuplicates()

    /*+--------+------+------+
      | movieId|userId|rating|
      +--------+------+------+
      |  110   |  1   |  4.0 |
      +--------+------+------+*/

    // filter only ratings >= 3.0
    ds_full = ds_full.filter(ds_full("rating") >= 3)
    def rdd = ds_full.rdd.repartition(8)
    println("partition " + rdd.getNumPartitions)    //durata: 6.951704152 secondi
    rdd.collect().foreach(println)
    rdd

    //result  = [6,1,4.0]
    //[1,1,4.0]
    //durata: 8.501904541 secondi


    //ds_full.rdd.foreach(println)
    //ds_full.write.format("com.databricks.spark.csv").save("myFile.csv")


    //e se la pulizia la facessimo nell'rdd??
   // val rdd = spark_session.sparkContext.textFile("myFile.csv",8)
    //rdd.collect().foreach(println)
*/

    // load dataset ratings
    //val ratings_path = "gs://scc_bucket_1/ratings.csv" // cloud
    val ratings_path = "dataset/ratings.csv" // local
    //val ratings_path = "dataset/rating.csv" // local AND big
    var ds_ratings = spark_session.sparkContext.textFile(ratings_path)
    val header = ds_ratings.first() //extract header
    val ratings_rdd = ds_ratings.filter(line => line != header).map(f => {
      f.split(",")
    }).map(p => (p(1), (p(0), p(2)))) //RDD Pairs
    //val movies_path = "gs://scc_bucket_1/movies.csv" // cloud
    val movies_path = "dataset/movies.csv" // local

    //val movies_path = "dataset/movie.csv" // local AND big
    def ds_movies_rdd = spark_session.sparkContext.textFile(movies_path)

    def movies_rdd = ds_movies_rdd.map(f => {
      f.split(",")
    }).map(p => (p(0), (p(1), p(2)))) //RDD Pairs

    val moviesRddPartitioned: RDD[(String, (String, String))] = movies_rdd.partitionBy(new HashPartitioner(8))
    val ratingsRddPartitioned: RDD[(String, (String, String))] = ratings_rdd.partitionBy(new HashPartitioner(8))
    //(p._1 -> key, (p._2._1._1 ->title, p._2._1._2->genre, p._2._2._1-> userID, p._2._2._2->rating))
    val ds_full = moviesRddPartitioned.join(ratingsRddPartitioned).map(p => (p._1, (p._2._2._1, p._2._2._2, p._2._1._1))).filter(p => (p._2._2.toFloat >= 3))
    // println("partition " + moviesRddPartitioned.getNumPartitions)
    println("partition " + ds_full.getNumPartitions) //2.771935272 secondi

    ds_full.collect().foreach(println)
    //ds_movies.collect().foreach(println)   //6.824895984 secondi

    ds_full
  }
}

//result = (1,(Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy,1,4.0))
//(6,(Heat (1995),Action|Crime|Thriller,1,4.0))
//durata: 3.894648616 secondi