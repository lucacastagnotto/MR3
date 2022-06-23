import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer

object oldMainApp {
  def sort[A : Ordering](coll: Seq[Iterable[A]]): Seq[Iterable[A]] = coll.sorted

  def candidates_generation(last_frequent_itemsets: List[List[Int]], k : Int) : Set[Set[Int]] = {
    val it1 = last_frequent_itemsets.iterator
    var candidates = Set[Set[Int]]()
    while (it1.hasNext) {
      val item1 = it1.next()
      val it2 = last_frequent_itemsets.iterator
      while(it2.hasNext) {
        val lNext = it2.next()
        if(item1.take(k - 2) == lNext.take(k - 2) && item1 != lNext && item1.last < lNext.last) {
          val l = List(item1 :+ lNext.last)
          val lF = l.flatten
          candidates += lF.toSet
        }
      }
    }
    // pruning
    val last_freq_itemset_Set = last_frequent_itemsets.map(_.toSet).toSet
    if(k > 2) {
      for(candidate <- candidates) {
        for(subset <- candidate.subsets(k - 1)) {
          if(!last_freq_itemset_Set.contains(subset)) {
            candidates -= candidate
          }
        }
      }
    }
    candidates
  }

  def main(args: Array[String]): Unit = {

    def filter_candidates(transactions: Iterator[Iterable[Int]], candidates: Set[Set[Int]], k: Int) : Iterator[(List[Int], Int)] = {
      var transactionSet = new ListBuffer[Set[Int]]()
      for(transaction <- transactions){
        transactionSet += transaction.toSet
      }
      val filteredItemsets = candidates.map{
        itemset => (itemset, transactionSet.count(transaction => itemset.subsetOf(transaction)))
      }.filter(x => x._2 > 0).map(x => (x._1.toList, x._2))
      filteredItemsets.iterator
    }

    val t1 = System.nanoTime // execution duration
    // SparkSession
    val spark_session = SparkSessionBuilder.create_SparkSession()
    // Input Reading
    val t0 = System.nanoTime // opening csv files duration
    val ratings_path = args(0)
    val movies_path = args(1)
    val ds_full = InputHandling.readInput(spark_session)
    val reading_duration = (System.nanoTime - t0) / 1e9d
    // Initial variables
    val transactions = ds_full.rdd.map(row => (row(1), row(0))).groupByKey().map(t => t._2.map(_.toString.toInt))
    val totalTransactions = transactions.count().toInt
    val min_support_coef = 0.25
    val min_support = min_support_coef * totalTransactions
    //transactions.collect().foreach(println)
    val frequent_singleton = transactions.flatMap(transaction => transaction.map(movieId => (movieId, 1))).reduceByKey((x, y) => x + y).filter(x => x._2 >= min_support).map(_._1).collect()
    var l1 = ListBuffer[List[Int]]()
    for(movieId <- frequent_singleton){
      val item_as_list = List[Int](movieId)
      l1 += item_as_list
    }
    var last_frequent_itemsets = sort(l1.toList).toList.map(_.toList)
    var frequent_itemsets = List[List[Int]]()
    frequent_itemsets ++= last_frequent_itemsets
    // Apriori
    var k = 2
    while(last_frequent_itemsets.nonEmpty) {
      val candidates = candidates_generation(last_frequent_itemsets, k)
      val candidatesPartitions = transactions.mapPartitions(x => filter_candidates(x, candidates, k))
      val new_frequent_itemsets = candidatesPartitions.reduceByKey((x,y) => x + y).filter(z => z._2 >= min_support)
      last_frequent_itemsets = sort(new_frequent_itemsets.map(x => x._1.sorted).collect().toList).toList.map(_.toList)
      frequent_itemsets ++= last_frequent_itemsets
      k = k + 1
    }
    val duration = (System.nanoTime - t1) / 1e9d
    // Make some prints
    /*
    println("min_support_coef: " + min_support_coef.toString + "\n")
    println("min_support: " + min_support.toString + "\n")
    println("durata: " + duration.toString + " secondi\n")
    println("durata apertura files: " + reading_duration.toString + " secondi\n")
    println("Frequent Itemsets:\n")
    for(itemset <- frequent_itemsets) {
      println(itemset.toString() + "\n")
    }
    */
    // Save file with frequent itemsets
    // val file = new File("gs://scc_bucket_1") // cloud
    val currentDirectory = new java.io.File(".").getCanonicalPath
    println("currentDirectory: " + currentDirectory)
    val file = new File("outputFile") // local
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("min_support_coef: " + min_support_coef.toString + "\n")
    bw.write("min_support: " + min_support.toString + "\n")
    bw.write("totalTransactions: " + totalTransactions + "\n")
    bw.write("durata: " + duration.toString + " secondi\n")
    bw.write("durata apertura files: " + reading_duration.toString + " secondi\n")
    bw.write("Frequent itemsets: \n")
    for (itemset <- frequent_itemsets) {
      bw.write(itemset.toString() + "\n")
    }
    bw.close()

  }
}