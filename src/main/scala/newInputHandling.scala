import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}

object newInputHandling {
  def readInput(spark_session: SparkSession, ratings_path: String, movies_path: String): RDD[(String, String)] = {
  //def readInput(spark_session: SparkSession, ratings_path: String, movies_path: String): RDD[(String, (String, String, String))] = {
    // load dataset ratings
    val ds_ratings = spark_session.sparkContext.textFile(ratings_path)
    val header = ds_ratings.first() //extract header
    val ratings_rdd = ds_ratings.filter(line => line != header).map(f => {
      f.split(",")
    }).map(p => (p(1), (p(0), p(2)))).filter(p => p._2._2.toFloat >= 3) //RDD Pairs ("movieId": ("userId", "rating"))
    // load movies ratings
    def ds_movies_rdd = spark_session.sparkContext.textFile(movies_path)
    def movies_rdd = ds_movies_rdd.map(f => {
      f.split(",")
    }).map(p => (p(0), (p(1), p(2)))) //RDD Pairs ("movieId": ("title", "genres"))
    val moviesRddPartitioned: RDD[(String, (String, String))] = movies_rdd.partitionBy(new HashPartitioner(8))
    val ratingsRddPartitioned: RDD[(String, (String, String))] = ratings_rdd.partitionBy(new HashPartitioner(8))
    //val ds_full = moviesRddPartitioned.join(ratingsRddPartitioned).map(p => (p._1, (p._2._2._1, p._2._2._2, p._2._1._1))).filter(p => p._2._2.toFloat >= 3)
    // join returns ("movieId": (("title", "genres") , ("userId", "rating")))
    val ds_full = moviesRddPartitioned.join(ratingsRddPartitioned).map(p => (p._2._2._1, p._1))
    ds_full
  }
}
