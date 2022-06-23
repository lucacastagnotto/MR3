import org.apache.spark.sql.{DataFrame, SparkSession}

object InputHandling {
  def readInput(spark_session: SparkSession): DataFrame = {
    // load dataset ratings
    //val ratings_path = "gs://scc_bucket_1/ratings.csv" // cloud
    val ratings_path = "datasets/25M/ratings.csv" // local
    //val ratings_path = "big_dataset/rating.csv" // local AND big
    var ds_ratings = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load(ratings_path)
    // load dataset movies
    //val movies_path = "gs://scc_bucket_1/movies.csv" // cloud
    val movies_path = "datasets/25M/movies.csv" // local
    //val movies_path = "big_dataset/movie.csv" // local AND big
    var ds_movies = spark_session.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load(movies_path)

    // clean data and join
    ds_ratings = ds_ratings.drop("timestamp")
    ds_ratings = ds_ratings.na.drop()
    ds_movies = ds_movies.drop("title", "genres")
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

    ds_full
  }
}