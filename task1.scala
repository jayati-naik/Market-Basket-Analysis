import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import scala.collection.immutable.ListMap
import scala.collection.{SortedSet, mutable}
import scala.collection.mutable.ListBuffer
import scala.math.Ordering.Implicits.seqDerivedOrdering

object task1 {

  def get_frequent_itemsets(data_chunk: Iterator[List[String]], candidates: Array[Set[String]]): Iterator[Tuple2[Set[String], Int]] = {
    val final_itemsets = new ListBuffer[Tuple2[Set[String], Int]]()
    val counter = mutable.Map.empty[Set[String], Int]

    for (i <- data_chunk) {
      for (c <- candidates) {
        if (c.subsetOf(i.toSet)) {
          if (!counter.contains(c)) {
            counter += (c -> 1)
          } else {
            counter(c) += 1
          }
        }
      }
    }

    for (c <- counter) {
      if (!final_itemsets.contains(c)) {
        final_itemsets.append((c._1, c._2))
      }
    }

    final_itemsets.toIterator
  }

  def generate_powersets(baskets: ListBuffer[Set[String]], prev_candidate_set: ListBuffer[Set[String]], counter: Int, support: Int): ListBuffer[Set[String]] = {

    var powersets = mutable.Map.empty[Set[String], Int]
    val current_pairs = new ListBuffer[Set[String]]()
    var pair = mutable.Set.empty[Set[String]]

    for (i <- prev_candidate_set) {
      for (j <- prev_candidate_set) {
        if (i.toList.sorted.init == i.toList.sorted.init) {
          val c = (i).union((j))
          if (c.size == counter) {
            pair += c.toList.sorted.toSet
          }
        }
      }
    }

    for (i <- pair) {
      for (j <- baskets) {
        if (i.subsetOf(j)) {
          if (!powersets.contains(i)) {
            powersets += (i -> 1)
          } else {
            powersets(i) += 1
          }
        }
      }
    }

    for (i <- powersets) {
      if (i._2 >= support) {
        current_pairs.append(i._1)
      }
    }

    current_pairs
  }

  def a_priori(data_chunk: Iterator[List[String]], support: Int): Iterator[Set[String]] = {
    val final_iterator = new ListBuffer[Set[String]]()
    val baskets = new ListBuffer[Set[String]]()
    var singletons = mutable.Map.empty[String, Int]
    var prev_candidate_set = new ListBuffer[Set[String]]()
    val result = new ListBuffer[Set[String]]()

    while (data_chunk.hasNext) {
      val b = data_chunk.next()
      baskets += b.toSet
      for (j <- b) {
        if (!singletons.contains(j))
          singletons += (j -> 1)
        else {
          singletons(j) += 1
        }
      }
    }

    for (k <- singletons) {
      if (k._2.toInt >= support) {
        prev_candidate_set.append(Set(k._1.toString))
        result.append(Set(k._1.toString))
      }
    }

    var counter = 2
    while (prev_candidate_set.size != 0) {
      val curr_candidate_set = generate_powersets(baskets, prev_candidate_set, counter, support)
      prev_candidate_set = curr_candidate_set
      if (prev_candidate_set.size != 0) {
        for (i <- curr_candidate_set) {
          result.append(i)
        }
        counter += 1
      }
    }

    for (i <- result) {
      if (!final_iterator.contains(i)) {
        final_iterator.append(i)
      }
    }

    final_iterator.toIterator
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("task1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel("Error")

    // Read Input
    val case_number = args(0)
    val support = args(1)
    val input_file_path = args(2)
    val output_file_path = args(3)

    val partition_count = 2
    val start_time = System.currentTimeMillis()

    val user_business_data = sc.textFile(input_file_path, partition_count)
    var data: RDD[List[String]] = null
    if (case_number == "1") {
      data = user_business_data.map(line => (line.split(',')))
        .filter(line => !(line.contains("user_id")))
        .map(line => (line(0), line(1))).groupByKey().map(_._2.toList)
    } else {
      data = user_business_data.map(line => (line.split(',')))
        .filter(line => !(line.contains("user_id")))
        .map(line => (line(1), line(0))).groupByKey().map(_._2.toList)
    }

    val candidates = data.mapPartitions(x => a_priori(x, support.toInt / partition_count)).distinct().collect()

    val frequent_itemsets = data.mapPartitions(x => get_frequent_itemsets(x, candidates))
      .reduceByKey(_ + _)
      .filter(_._2 >= support.toInt)
      .map(x => x._1)
      .collect()

    val candidate_formatted = mutable.Map.empty[Int, ListBuffer[SortedSet[String]]]
    for (c <- candidates) {
      if (!candidate_formatted.contains(c.size)) {
        candidate_formatted += (c.size -> ListBuffer(sortSet(c)))
      } else {
        candidate_formatted(c.size) += sortSet(c)
      }
    }

    val frequent_itemsets_formatted = mutable.Map.empty[Int, ListBuffer[SortedSet[String]]]
    for (f <- frequent_itemsets) {
      if (!frequent_itemsets_formatted.contains(f.size)) {
        frequent_itemsets_formatted += (f.size -> ListBuffer(sortSet(f)))
      } else {
        frequent_itemsets_formatted(f.size) += sortSet(f)
      }
    }

    var final_candiadates_to_be_written = ""
    val w = new PrintWriter(new File(output_file_path))
    w.write("Candidates:\n")

    val c_sorted = ListMap(candidate_formatted.toSeq.sortBy(_._1): _*)

    for (c1 <- c_sorted) {
      var counter = c1._2.size
      for (c2 <- c1._2.toList.sortBy(x => x.toList)) {
        var t1 = ""
        for (c3 <- c2.toList.sortBy(x => x.toList)) {
          t1 = t1.concat("'" + c3 + "',")
        }
        final_candiadates_to_be_written = final_candiadates_to_be_written + "(" + t1 + ")"
        counter = counter - 1
        if (counter == 0) {
          final_candiadates_to_be_written = final_candiadates_to_be_written + "\n\n"
        } else {
          final_candiadates_to_be_written = final_candiadates_to_be_written + ","
        }
      }
    }

    w.write(final_candiadates_to_be_written.replace(",)", ")"))

    w.write("Frequent Itemsets:\n")
    val f_sorted = ListMap(frequent_itemsets_formatted.toSeq.sortBy(_._1): _*)
    var final_cfrequent_itemsets_to_be_written = ""

    for (f1 <- f_sorted) {
      var counter = f1._2.size
      for (f2 <- f1._2.toList.sortBy(x => x.toList)) {
        var t1 = ""
        for (f3 <- f2.toList.sortBy(x => x.toList)) {
          t1 = t1.concat("'" + f3 + "',")
        }
        final_cfrequent_itemsets_to_be_written = final_cfrequent_itemsets_to_be_written + "(" + t1 + ")"
        counter = counter - 1
        if (counter == 0) {
          final_cfrequent_itemsets_to_be_written = final_cfrequent_itemsets_to_be_written + "\n\n"
        } else {
          final_cfrequent_itemsets_to_be_written = final_cfrequent_itemsets_to_be_written + ","
        }
      }
    }

    w.write(final_cfrequent_itemsets_to_be_written.replace(",)", ")"))

    w.close()

    val end_time = System.currentTimeMillis()
    println("Duration: " + (end_time - start_time) / 1000)
  }

  def sortSet[A](unsortedSet: Set[A])(implicit ordering: Ordering[A]): SortedSet[A] =
    SortedSet.empty[A] ++ unsortedSet

}
