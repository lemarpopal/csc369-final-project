import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.ListBuffer



case class Data(idx: Long, name: String, totalFunding : Double, rounds : Double, seed : Double, venture : Double, roundA: Double, roundB: Double)
case class DataEncode(idx: Long, totalFunding : Int, rounds : Int, seed : Int, venture : Int, roundA: Int, roundB: Int,
                      crowdFunding: Int, angel: Int, privateEquity: Int)
case class Classification(idx: Long, status : String) {
  override def toString : String = status
}

class KNN(var neighbors: Int) extends Serializable {
  // Fit a K-Nearest-Neighbors model with Spark RDD's
  def fit(X: RDD[Data], y : RDD[Classification], Xtest: RDD[Data]) : Array[(Data, String)] = {
    val y_indexed = y.map(line => (line.idx, line))
    val XY_indexed = X.map(line => (line.idx,line)).join(y_indexed)
    Xtest.cartesian(XY_indexed)
      .map({case (a, b) => (a, (b._2._2.status, distance(a, b._2._1)))}) // Get each row's distance to every other row
      .sortBy({case (_, b) => b._2}) // Sort by lowest distance first
      .groupByKey() // Group by row
      .mapValues(v => v.take(neighbors)) // Get the N nearest neighbor categories
      .map({case (a, b) => (a, get_category(b.toList))}) // Get the most likely category for each row
      .collect()
  }

  // Gets the most likely category from a list of category-distance pairs
  def get_category(distances : List[(String, Double)]) : String = {
    distances.map({case (category, _) => category}) // Get the category of each category-distance pair
      .groupBy(identity) // Get all possible categories for each row
      .mapValues(_.size) // Get the frequency of each possible category
      .maxBy(_._2)._1 // Get the category that is most frequent for this row
  }

  // Calculate the euclidean distance between two observations
  def distance(a : Data, b : Data) : Double = {
    var dist = 0.0;
    dist += Math.pow(a.totalFunding - b.totalFunding, 2.0)
    dist += Math.pow(a.rounds - b.rounds, 2.0)
    dist += Math.pow(a.seed - b.seed, 2.0)
    dist += Math.pow(a.venture - b.venture, 2.0)
    dist += Math.pow(a.roundA - b.roundA, 2.0)
    dist += Math.pow(a.roundB - b.roundB, 2.0)
    Math.sqrt(dist)
  }
}

class normalizer() {
  // Normalize data with minmax technique
  val scaler_info = ListBuffer[(Double, Double)]()
  def fit(data: RDD[Data]) : Unit = {
    val maxFunding = data.takeOrdered(1)(Ordering[Double].reverse.on(_.totalFunding))(0).totalFunding
    val minFunding = data.takeOrdered(1)(Ordering[Double].on(_.totalFunding))(0).totalFunding
    val maxRounds = data.takeOrdered(1)(Ordering[Double].reverse.on(_.rounds))(0).rounds
    val minRounds = data.takeOrdered(1)(Ordering[Double].on(_.rounds))(0).rounds
    val maxSeed = data.takeOrdered(1)(Ordering[Double].reverse.on(_.seed))(0).seed
    val minSeed = data.takeOrdered(1)(Ordering[Double].on(_.seed))(0).seed
    val maxVenture = data.takeOrdered(1)(Ordering[Double].reverse.on(_.venture))(0).venture
    val minVenture = data.takeOrdered(1)(Ordering[Double].on(_.venture))(0).venture
    val maxA = data.takeOrdered(1)(Ordering[Double].reverse.on(_.roundA))(0).roundA
    val minA = data.takeOrdered(1)(Ordering[Double].on(_.roundA))(0).roundA
    val maxB = data.takeOrdered(1)(Ordering[Double].reverse.on(_.roundB))(0).roundB
    val minB = data.takeOrdered(1)(Ordering[Double].on(_.roundB))(0).roundB
    val funding_tuple = (maxFunding,minFunding)
    val round_tuple = (maxRounds,minRounds)
    val seed_tuple = (maxSeed,minSeed)
    val venture_tuple = (maxVenture,minVenture)
    val a_tuple = (maxA,minA)
    val b_tuple = (maxB, minB)

    scaler_info += funding_tuple
    scaler_info += round_tuple
    scaler_info += seed_tuple
    scaler_info += venture_tuple
    scaler_info += a_tuple
    scaler_info += b_tuple

  }
  def normalize(data: RDD[Data]) : RDD[Data] = {
    val maxFunding = scaler_info(0)._1
    val minFunding = scaler_info(0)._2
    val maxRounds = scaler_info(1)._1
    val minRounds = scaler_info(1)._2
    val maxSeed = scaler_info(2)._1
    val minSeed = scaler_info(2)._2
    val maxVenture = scaler_info(3)._1
    val minVenture = scaler_info(3)._2
    val maxA = scaler_info(4)._1
    val minA = scaler_info(4)._2
    val maxB = scaler_info(5)._1
    val minB = scaler_info(5)._2

    data.map(rec => Data(rec.idx, rec.name,
      (rec.totalFunding - minFunding) / (maxFunding - minFunding),
      (rec.rounds - minRounds) / (maxRounds - minRounds),
      (rec.seed - minSeed) / (maxSeed - minSeed),
      (rec.venture - minVenture) / (maxVenture - minVenture),
      (rec.roundA - minA) / (maxA - minA),
      (rec.roundB - minB) / (maxB - minB)))
  }


}

object KNN {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("KNN").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("./data/investments.csv")
      .map(_.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")).zipWithIndex()
      .map({case(line, idx) => Data(idx, line(1), getTotalFunding(line(5).trim()), line(11).toDouble, line(18).toDouble,
        line(19).toDouble, line(32).toDouble, line(33).toDouble)}).sample(false, 0.01)

    val data_idx = data.map(line => (line.idx, line))

    val categories = sc.textFile("./data/investments.csv")
      .map(_.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")).zipWithIndex()
      .map({case(line,idx) => Classification(idx, line(6))}).map(line => (line.idx,line))
      .join(data_idx).map({case (_, pair) => pair._1})



    var model = new KNN(5)

    val split_data = train_test_split(data, categories, 0.5)

    val Xtrain = split_data._1
    val ytrain = split_data._2
    val Xtest = split_data._3
    val ytest = split_data._4

    val normalizer = new normalizer()
    normalizer.fit(Xtrain)
    val Xtrain_norm = normalizer.normalize(Xtrain)
    val Xtest_norm = normalizer.normalize(Xtest)


    val result = model.fit(Xtrain_norm, ytrain, Xtest_norm)

    result.foreach(println)

    // accuracy
    println(accuracy(result, ytest.collect()))

    // precision
    println(precision(result, ytest.collect()))

    // recall
    println(recall(result, ytest.collect()))

    //f1
    println(f1_score(result, ytest.collect()))

    //cross_validate
    val losses = List("recall","precision","f1")
    for (loss <- losses){
      println(loss)
      for (neighbors <- 1 to 10) {
        println(neighbors)
        model = new KNN(neighbors)
        println(cross_validate(model, data, categories, 5, loss))
      }
    }

  }
  def extractDouble(x: Any): Option[Double] = x match {
    case n: java.lang.Number => Some(n.doubleValue())
    case _ => None
  }

  // Get the total funding from a weirdly formatted string
  def getTotalFunding(funding: String) : Double = {
    if (funding == "-") {
      return 0
    }
    funding.replaceAll(",", "")
      .replaceAll("^\"+|\"+$", "")
      .trim().toDouble
  }

  def train_test_split(X: RDD[Data], y: RDD[Classification], frac: Double):
  (RDD[Data], RDD[Classification], RDD[Data], RDD[Classification]) = {
    val Xtrain = X.sample(false, frac);
    val Xtrain_idx = Xtrain.map(row => (row.idx, row))
    val ytrain = y.map(row => (row.idx, row)).join(Xtrain_idx).map({case (_, pair) => pair._1})
    val Xtest = X.subtract(Xtrain)
    val ytest = y.subtract(ytrain)

    (Xtrain, ytrain, Xtest, ytest)
  }

  def cross_validate(model: KNN, X: RDD[Data], y: RDD[Classification], folds: Int, loss: String): Map[String, Double] = {
    // Make (K, V) rows for convenient index slicing
    val X_idx = X.map(line => (line.idx, line))
    val y_idx = y.map(line => (line.idx, line))

    // Get Partition size for eah fold
    val partition_size = X.count()/folds
    val losses = ListBuffer[(String, Double)]()

    // For each fold compute the losses for each class and add to losses list
    for (i <- 0 until folds){
      // In this case I make the fold data the test data and the rest the training data
      var X_fold_data = X_idx.filter({case(idx, line) => ((0 + partition_size * i) <= idx) && (idx <= (0 + partition_size * (i + 1)))}).map(tuple => tuple._2)
      var X_non_fold_data = X_idx.filter({case(idx, line) => ((0 + partition_size * i) > idx) || (idx > (0 + partition_size * (i + 1)))}).map(tuple => tuple._2)

      var y_fold_data = y_idx.filter({case(idx, line) => ((0 + partition_size * i) <= idx) && (idx <= (0 + partition_size * (i + 1)))}).map(tuple => tuple._2)
      var y_non_fold_data = y_idx.filter({case(idx, line) => ((0 + partition_size * i) > idx) || (idx > (0 + partition_size * (i + 1)))}).map(tuple => tuple._2)

      //ACCURACY IS RETURNING ANY FOR SOME REAONON >:-(
      /*
      if (loss == "accuracy"){
        var result = model.fit(X_non_fold_data, y_non_fold_data, X_fold_data)
        var accuracy = accuracy(result, y_fold_data.collect())
        for (j <- categories.map(_.toString).distinct){
          losses += ((j,accuracy))
        }
        losses += loss_val
      }
      */
      var normalizer = new normalizer()
      normalizer.fit(X_non_fold_data)
      var X_non_fold_data_norm = normalizer.normalize(X_non_fold_data)
      var X_fold_data_norm = normalizer.normalize(X_fold_data)
      if (loss == "recall"){
        var result = model.fit(X_non_fold_data_norm, y_non_fold_data, X_fold_data_norm)
        var loss_val = recall(result, y_fold_data.collect())
        // Make the map into a Sequence of tuples (Class, Loss)
        var loss_holder = loss_val.map(e => (e._1,e._2))
        loss_val.map(e => (e._1,e._2)).foreach(e => losses += e)
      }
      else if (loss == "precision"){
        var result = model.fit(X_non_fold_data, y_non_fold_data, X_fold_data)
        // Make the map into a Sequence of tuples (Class, Loss)
        var loss_val = precision(result, y_fold_data.collect())
        loss_val.map(e => (e._1,e._2)).foreach(e => losses += e)
      }
      else if (loss == "f1"){
        var result = model.fit(X_non_fold_data, y_non_fold_data, X_fold_data)
        // Make the map into a Sequence of tuples (Class, Loss)
        var loss_val = f1_score(result, y_fold_data.collect())
        loss_val.map(e => (e._1,e._2)).foreach(e => losses += e)
      }
    }
    // For each class return the mean from all the folds losses
    return losses.groupBy(tuple => tuple._1).mapValues(map_val => map_val.map(value => value._2)).mapValues(loss_list => loss_list.foldLeft(0.0)(_ + _) / loss_list.length)
  }

  def accuracy(result : Array[(Data, String)], ytest : Array[Classification]) : Double = {
    val ypred_tuple = result.map({case( data, pred) => (data.idx, pred)})
    val ytest_tuple = ytest.map(line => (line.idx, line.status))
    (ypred_tuple ++ ytest_tuple)
      .groupBy(_._1)
      .values.foreach(x=>println(x.mkString(" ")))
    val ypred_ytest = (ypred_tuple ++ ytest_tuple)
      .groupBy(_._1)
      .values
      .map(tuple_pair => (tuple_pair(0), tuple_pair(1)))

    val correct_classifications = ypred_ytest.count({
      case(pred, actual) => pred._2 == actual._2
    })

    val total_classifications = result.length

    var accuracy = correct_classifications * 1.0 / total_classifications
    if (accuracy.isNaN){accuracy = 0}

    accuracy
  }

  def precision(result : Array[(Data, String)], ytest : Array[Classification]) : scala.collection.mutable.Map[String, Double] = {
    val categories = ytest.map(pred => pred.status).distinct
    val precision_map = scala.collection.mutable.Map[String, Double]()
    val category_strings = categories.map(_.toString).distinct

    val ypred_tuple = result.map({case( data, pred) => (data.idx, pred)})
    val ytest_tuple = ytest.map(line => (line.idx, line.status))
    val ypred_ytest = (ypred_tuple ++ ytest_tuple)
      .groupBy(_._1)
      .values
      .map(tuple_pair => (tuple_pair(0), tuple_pair(1)))

    for (cat <- category_strings) {
      // filter results for those PREDICTED to be in this category
      val filtered = ypred_ytest.filter({
        case (pred, _) => pred._2 == cat
      })

      // filter results where prediction for this category was correct (True Positive)
      val true_positives = filtered.filter({
        case (pred, actual) => pred._2 == actual._2
      })


      var precision = true_positives.size * 1.0 / filtered.size
      if (precision.isNaN){precision = 0}

      precision_map += (cat -> precision)
    }

    precision_map
  }

  def recall(result : Array[(Data, String)], ytest : Array[Classification]) : scala.collection.mutable.Map[String, Double] = {
    val categories = ytest.map(pred => pred.status).distinct
    val recall_map = scala.collection.mutable.Map[String, Double]()
    val category_strings = categories.map(_.toString).distinct

    val ypred_tuple = result.map({case( data, pred) => (data.idx, pred)})
    val ytest_tuple = ytest.map(line => (line.idx, line.status))
    val ypred_ytest = (ypred_tuple ++ ytest_tuple).
      groupBy(_._1)
      .values
      .map(tuple_pair => (tuple_pair(0), tuple_pair(1)))

    for (cat <- category_strings) {
      // filter results for those PREDICTED to be in this category
      val filtered = ypred_ytest.filter({
        case (_, actual) => actual._2 == cat
      })

      // filter results where prediction for this category was correct (True Positive)
      val true_positives = filtered.filter({
        case (pred, actual) => pred._2 == actual._2
      })


      var recall_val = true_positives.size * 1.0 / filtered.size

      if (recall_val.isNaN){recall_val = 0}

      recall_map += (cat -> recall_val)
    }
    recall_map
  }

  def f1_score(result : Array[(Data, String)], ytest : Array[Classification]) : scala.collection.mutable.Map[String, Double] = {
    val precisionScore = precision(result, ytest)
    val recallScore = recall(result, ytest)
    val categories = ytest.map(pred => pred.status).distinct
    val category_strings = categories.map(_.toString).distinct
    val f1_map = scala.collection.mutable.Map[String, Double]()
    for (cat <- category_strings) {
      f1_map += (cat -> (2*precisionScore(cat)*recallScore(cat))/(precisionScore(cat)+recallScore(cat)))
    }

    return f1_map
  }
}