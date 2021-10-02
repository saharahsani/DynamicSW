package clustream

/**
 * Created by omar on 9/25/15.
 */

import breeze.linalg.{sum, _}
import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, sql}

import java.io._
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * Class that contains the offline methods for the CluStream
 * method. It can be initialized with a CluStreamOnline model to
 * facilitate the use of it at the same time the online process
 * is running.
 *
 * */

@Experimental
class CluStream(
                 val model: dynamicCluWithPca)
  extends Logging with Serializable {


  def this() = this(null)

  val spark: SparkSession = SparkSession.builder.getOrCreate()

  /**
   * Method that samples values from a given distribution.
   *
   * @param dist : this is a map containing values and their weights in
   *             the distributions. Weights must add to 1.
   *             Example. {A -> 0.5, B -> 0.3, C -> 0.2 }
   * @return A: sample value A
   * */

  private def sample0[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var bool = false
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p) {
        return item
      }
    }


    sys.error(f"this should never happen") // needed so it will compile
  }

  private def sample[A](dist: Map[A, Double]): Any = {
    val p = scala.util.Random.nextDouble
    val it = dist.toArray
    var u = false
    var accum = 0.0
    for (m <- it) {
      val (item, itemProb) = m
      accum += itemProb
      if (accum >= p) {
        u = true
        return item
      }
    }
    if (!u) {
      return it(0)._1
    }
    if (it.size == 0)
      sys.error(f"this should never happen") // needed so it will compile
  }

  private def sample1[A](dist: Map[A, Double]): (A, Double) = {
    val p = scala.util.Random.nextDouble
    val it = dist.toArray
    var u = false
    var accum = 0.0
    for (m <- it) {
      val (item, itemProb) = m
      accum += itemProb
      if (accum >= p) {
        u = true
        return (item, itemProb)
      }
    }
    if (!u) {
      return it(0)
    }

    sys.error(f"this should never happen") // needed so it will compile
  }

  /**
   * Method that saves a snapshot to disk using the pyramidal time
   * scheme to a given directory.
   *
   * @param dir   : directory to save the snapshot
   * @param tc    : time clock unit to save
   * @param alpha : alpha parameter of the pyramidal time scheme
   * @param l     : l modifier of the pyramidal time scheme
   * */

  def saveSnapShotsToDisk(dir: String = "", tc: Long, alpha: Int = 2, l: Int = 2): Unit = {

    var write = false
    var delete = false
    var order = 0
    val mcs = model.getMicroClusters
    val mcSW = model.getMicroClusterSW


    val exp = (scala.math.log(tc) / scala.math.log(alpha)).toInt

    for (i <- 0 to exp) {
      if (tc % scala.math.pow(alpha, i + 1) != 0 && tc % scala.math.pow(alpha, i) == 0) {
        order = i
        write = true
      }
    }

    val tcBye = tc - ((scala.math.pow(alpha, l) + 1) * scala.math.pow(alpha, order + 1)).toInt

    if (tcBye > 0) delete = true

    if (write) {
      val out = new ObjectOutputStream(new FileOutputStream(dir + "/" + tc))
       val out1 = new ObjectOutputStream((new FileOutputStream(dir + "/" + tc + "SW")))

      try {
        out.writeObject(mcs)
          out1.writeObject(mcSW)
       // println(mcs.map(x => x.getN).mkString(","))
        //  println(mcs.size)
        // println("mc size: "+mcs.size)
        // println(model.getMicroClustersInfo.map(x=>(x._1.rmsd)).mkString(","))
        // println(mcSW.map(x=>x.getN).mkString(","))

      }
      catch {
        case ex: IOException => println("Exception while writing file " + ex)
      }
      finally {
        out.close()
        //  out1.close()

      }
    }

    if (delete) {
      try {
        new File(dir + "/" + tcBye).delete()
      }
      catch {
        case ex: IOException => println("Exception while deleting file " + ex);
      }
    }
  }

  /**
   * Method that gets the snapshots to use for a given time and horizon in a
   * given file directory.
   *
   * @param dir : directory to save the snapshot
   * @param tc  : time clock unit to save
   * @param h   : time horizon
   * @return (Long,Long): tuple of the first and second snapshots to use.
   * */

  def getSnapShots(dir: String = "", tc: Long, h: Long): (Long, Long) = {

    var tcReal = tc
    while (!Files.exists(Paths.get(dir + "/" + tcReal)) && tcReal >= 0) tcReal = tcReal - 1
    var tcH = tcReal - h
    while (!Files.exists(Paths.get(dir + "/" + tcH)) && tcH >= 0) tcH = tcH - 1
    if (tcH < 0) while (!Files.exists(Paths.get(dir + "/" + tcH))) tcH = tcH + 1

    if (tcReal == -1L) tcH = -1L
    (tcReal, tcH)
  }

  /**
   * Method that returns the microclusters from the snapshots for a given time and horizon in a
   * given file directory. Subtracts the features of the first one with the second one.
   *
   * @param dir : directory to save the snapshot
   * @param tc  : time clock unit to save
   * @param h   : time horizon
   * @return Array[MicroCluster]: computed array of microclusters
   * */

  def getMCsFromSnapshots(dir: String = "", tc: Long, h: Long): ListBuffer[MicroCluster] = {
    val (t1, t2) = getSnapShots(dir, tc, h)

    try {
      val in1 = new ObjectInputStream(new FileInputStream(dir + "/" + t1))
      val snap1 = in1.readObject().asInstanceOf[ListBuffer[MicroCluster]]

      val in2 = new ObjectInputStream(new FileInputStream(dir + "/" + t2))
      val snap2 = in2.readObject().asInstanceOf[ListBuffer[MicroCluster]]

      in2.close()
      in1.close()

      val arrs1 = snap1.map(_.getIds)
      val arrs2 = snap2.map(_.getIds)
      val relatingMCs = snap1 zip arrs1.map(a => arrs2.zipWithIndex.map(b => if (b._1.toSet.intersect(a.toSet).nonEmpty) b._2; else -1))
      relatingMCs.map { mc =>
        if (!mc._2.forall(_ == -1) && t1 - h >= t2) {

          for (id <- mc._2) if (id != -1) {
            val numCount = mc._1.getN - snap2(id).getN
            if (numCount >= 0) {
              mc._1.setCf2x(mc._1.getCf2x - snap2(id).getCf2x)
              mc._1.setCf1x(mc._1.getCf1x - snap2(id).getCf1x)
              mc._1.setCf2t(mc._1.getCf2t - snap2(id).getCf2t)
              mc._1.setCf1t(mc._1.getCf1t - snap2(id).getCf1t)
              mc._1.setN(mc._1.getN - snap2(id).getN)
              mc._1.setIds(mc._1.getIds.toSet.diff(snap2(id).getIds.toSet).toArray)
            }
          }
          mc._1
        } else mc._1

      }
    }
    catch {
      case ex: IOException => println("Exception while reading files " + ex)
        null
    }

  }

  /**
   * Method that returns the centrois of the microclusters.
   *
   * @param mcs : array of microclusters
   * @return Array[Vector]: computed array of centroids
   * */

  def getCentersFromMC(mcs: ListBuffer[MicroCluster]): ListBuffer[Vector[Double]] = {
    mcs.filter(_.getN > 0).map(mc => mc.getCf1x / mc.getN.toDouble)
  }

  def getMCsFromSnapshotSW(dir: String = "", tc: Long): ListBuffer[MicroCluster] = {
    //model.getMicroClusters
      try {
        val in1 = new ObjectInputStream(new FileInputStream(dir + "/" + tc))
        val snap1 = in1.readObject().asInstanceOf[ListBuffer[MicroCluster]]
        in1.close()
        snap1
      } catch {
        case ex: IOException => println("Exception while reading files " + ex)
          null
      }

  }


  /**
   * Method that returns the weights of the microclusters from the number of points.
   *
   * @param mcs : array of microclusters
   * @return Array[Double]: computed array of weights
   * */

  def getWeightsFromMC(mcs: ListBuffer[MicroCluster]): ListBuffer[Double] = {
    var arr: ListBuffer[Double] = mcs.map(_.getN.toDouble).filter(_ > 0)
    val sum: Double = arr.sum
    arr.map(value => value / sum)
  }

  /**
   * expiring phase
   * DELETE file with related swFiles
   *
   * @param tc
   * @param windowTime
   * @param dir
   * */
  def expiringPhase(tc: Long, windowTime: Int, dir: String): Boolean = {
    val threshold = tc - windowTime + 1
    val directory = new File(dir)
    try {
      if (directory.exists && directory.isDirectory && directory.listFiles().length > 0) {
        val expiredFiles = directory.listFiles().filter(x => Try(x.getName.toInt).isSuccess && x.getName.toInt < threshold)
        if (expiredFiles.length > 0) {
          val headFile = expiredFiles.head
          headFile.delete()
          val swFile = dir + "/" + headFile.getName + "SW"
          if (Files.exists(Paths.get(swFile))) {
            Files.delete(Paths.get(swFile))
          }
          return true
        }
      }
    } catch {
      case ex: IOException => println("Exception files not found or not delete! " + ex)
    }
    return false
  }

  /**
   * Method that returns a computed KMeansModel. It runs a modified version
   * of the KMeans algorithm in Spark from sampling the microclusters given
   * its weights.
   *
   * @param sc  : spark context where KMeans will run
   * @param k   : number of clusters
   * @param mcs : array of microclusters
   * @return org.apache.spark.mllib.clustering.KMeansModel: computed KMeansModel
   * */

  def fakeKMeans(sc: SparkContext, k: Int, numPoints: Int, mcs: ListBuffer[MicroCluster]) = {

    val kmeans = new KMeans()
    var centers = getCentersFromMC(mcs).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    val weights = getWeightsFromMC(mcs)
    val map = (centers zip weights).toMap
    val points = Array.fill(numPoints)(Vectors.dense(sample(map).toString
      .replace("]", " ").replace("[", " ").split(",").map(_.toDouble)))


    kmeans.setMaxIterations(20)
    kmeans.setK(k)
    kmeans.setInitialModel(new org.apache.spark.mllib.clustering.KMeansModel(Array.fill(k)(Vectors.dense(sample(map).toString.replace("[", " ").replace("]", " ").split(",").map(_.toDouble)))))
    val trainingSet = sc.parallelize(points)
    val clusters = kmeans.run(trainingSet)
    trainingSet.unpersist(blocking = false)
    clusters

  }

  def streamingKmeans(sc: SparkContext, k: Int, numPoints: Int, mcs: ListBuffer[MicroCluster]) = {

    val strKmeans = new StreamingKMeans()
    var centers = getCentersFromMC(mcs).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    val weights = getWeightsFromMC(mcs)
    val map = (centers zip weights).toMap
    val points = Array.fill(numPoints)(sample1(map)._1)

    val initialize = Array.fill(k)(sample1(map))
    strKmeans.setK(k).setInitialCenters(initialize.map(x => x._1), initialize.map(x => x._2))
    val trainingSet = sc.parallelize(points)
    val clusters = strKmeans.latestModel().update(trainingSet, 1.0, "batches")
    clusters
  }

  def bisectingKmeans(sc: SparkContext, k: Int, numPoints: Int, mcs: ListBuffer[MicroCluster]) = {

    val bkm = new BisectingKMeans().setK(k)

    var centers = getCentersFromMC(mcs).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    val weights = getWeightsFromMC(mcs)
    val map = (centers zip weights).toMap
    val points = Array.fill(numPoints)(sample1(map)._1)
    val trainingSet = sc.parallelize(points)
    val model = bkm.run(trainingSet)
    model

  }

  def getNearestClusterRdd(point: (MicroCluster, Int), microClusters: ListBuffer[(MicroCluster, Int)]) = {
    var minDist = Double.PositiveInfinity
    var nearIdx: Int = -1
    val pointCenters = point._1.getCf1x / point._1.getN.toDouble
    for (idx2 <- microClusters.length - 1 to 0 by -1) if (idx2 > -1 && microClusters(idx2)._2 != point._2) {
      val mcCenter = microClusters(idx2)._1.getCf1x / microClusters(idx2)._1.getN.toDouble
      val dist = squaredDistance(mcCenter, pointCenters)
      if (dist < minDist) {
        minDist = dist
        nearIdx = microClusters(idx2)._2
      }
    }
    (minDist, nearIdx)
  }

  /*def getNearestCluster(point: (MicroCluster, Int), microClusters: ListBuffer[(MicroCluster, Int)]) = {
    var minDist = Double.PositiveInfinity
    var nearIdx: Int = -1
    val pointCenters = point._1.getCf1x / point._1.getN.toDouble
    for (idx2 <- microClusters.length - 1 to 0 by -1) if (idx2 > -1 && microClusters(idx2)._2 != point._2) {
      val mcCenter = microClusters(idx2)._1.getCf1x / microClusters(idx2)._1.getN.toDouble
      val dist = squaredDistance(mcCenter, pointCenters)
      if (dist < minDist) {
        minDist = dist
        nearIdx = idx2
      }
    }
    (minDist, nearIdx)
  }

*/


  def distanceNearestMC(mc: MicroCluster, mcs: ListBuffer[MicroCluster]): Double = {
    val vec = mc.getCf1x / mc.getN.toDouble
    var minDist = Double.PositiveInfinity
    var i = 0
    for (mc <- mcs) {
      val mcCenter = mc.getCf1x / mc.getN.toDouble
      val dist = squaredDistance(vec, mcCenter)
      if (dist != 0.0 && dist < minDist) minDist = dist
      i += 1
    }
    scala.math.sqrt(minDist)
  }

  def setRMSDToMc(microClusters: ListBuffer[MicroCluster]): ListBuffer[MicroCluster] = {
    for (mc <- microClusters) {
      if (mc.getN > 1) {
        mc.setRMSD(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
      } else {
        mc.setRMSD(distanceNearestMC(mc, microClusters))
      }
    }
    microClusters
  }

  def getFinalClusteringRdd(sc: SparkContext, k: Int, numPoints: Int, mcs: ListBuffer[MicroCluster]): Array[org.apache.spark.ml.linalg.Vector] = {

    val lMcClusters = mcs.filter(_.getN > 0)
    //var centers = getCentersFromMC(mcs).map(v => (v))
    //val weights = getWeightsFromMC(mcs)
   // val map = (centers zip weights).toMap
    //val points = ListBuffer.fill(numPoints)(sample1(map)._1)
    val LMc = setRMSDToMc(lMcClusters)
    val microClusters = LMc zip (0 until (numPoints))
println("microClusters befor: "+ microClusters.size)
    val someSchema = List(
      StructField("cf1x", VectorType),
      StructField("N", LongType),
      StructField("mcId", IntegerType)
    )
    val list = ListBuffer[Row]()
    microClusters.foreach(x =>
      list.append(Row(org.apache.spark.ml.linalg.Vectors.dense(x._1.getCf1x.toArray), x._1.getN, x._2))
    )
    val listRdd = sc.parallelize(list)
    val microClustersDf = spark.createDataFrame(listRdd, StructType(someSchema))
    // get nearestId by udf
    val findNearestId = (mcId: Int, cf1x: org.apache.spark.ml.linalg.Vector, N: Long) => {
      var minDist = Double.PositiveInfinity
      var nearIdx: Int = -1
      if (microClusters.size > 1) {
        val pointCenters = DenseVector(cf1x.toArray) / N.toDouble
        for (idx2 <- microClusters.length - 1 to 0 by -1) if (idx2 > -1 && microClusters(idx2)._2 != mcId) {
          val mcCenter = microClusters(idx2)._1.getCf1x / microClusters(idx2)._1.getN.toDouble
          val dist = squaredDistance(mcCenter, pointCenters)
          if (dist < minDist) {
            minDist = dist
            nearIdx = idx2
          }
        }
        else -1
        if (scala.math.sqrt(minDist) <= microClusters(nearIdx)._1.rmSD) nearIdx
        else -1
      }else -1
    }
    val getNearIdUdf = udf(findNearestId)
    // find nearId
    val mcWithNearestDF = microClustersDf.withColumn("nearId", getNearIdUdf(col("mcId"), col("cf1x"), col("N")))
    val dataDf = mcWithNearestDF.where(col("nearId").notEqual(-1)).select(col("mcId"), col("nearId"))
    var joinDF= spark.emptyDataFrame
    if(dataDf.head(1).isEmpty){
      joinDF=microClustersDf.withColumn("new_cluster_Id",col("mcId"))
        .sort("new_cluster_Id")
      // joinDF.show(50)
    }else{
    // compute graph for nearest ids
    val rows = dataDf.collectAsList()
    val setNewClusterIdDF = AdjacencyMatrix.computeGraph(rows, spark)
    joinDF = setNewClusterIdDF.join(microClustersDf, setNewClusterIdDF.col("CLUSTER_NUMBER").equalTo(microClustersDf.col("mcId")), "right")
      .select(when(col("NEW_CLUSTER_NUMBER").isNull, col("mcId")).otherwise(col("NEW_CLUSTER_NUMBER")).as("new_cluster_id"),
        col("cf1x"), col("N"), col("mcId"))
      .sort(col("NEW_CLUSTER_NUMBER"))
    //  joinDF.show(50
  }
    // group near cluster
    val groupDF = joinDF.groupBy(col("new_cluster_id")).agg(
      sql.functions.sum(col("N")).as("N"),
      Summarizer.sum(col("cf1x")).as("cf1x")
    )
    //groupDF.show(50)
    // get centroid
    val getCentroid = (cflx: org.apache.spark.ml.linalg.Vector, n: Long) => {
      val centroid = DenseVector(cflx.toArray).toVector / n.toDouble
      org.apache.spark.ml.linalg.Vectors.dense(centroid.toArray)

    }
    val getCentroidUdf = udf(getCentroid)
    val centroidDf = groupDF.withColumn("centroid", getCentroidUdf(col("cf1x"), (col("N"))))
    //centroidDf.show(50)
    val lCentroid = centroidDf.select("centroid").collect().map(x =>
      x.getAs("centroid").asInstanceOf[org.apache.spark.ml.linalg.Vector]
    )
    println("microClusters after mrg: "+ lCentroid.size)

    lCentroid

  }


  /**
   * Method that allows to initialize the online clustering from this class
   *
   * @param initPathFile
   * @return
   */
  /*def StartInitialize(snapPath: String, sc: SparkContext, initPathFile: String): Boolean = {
    val bool = model.initializeClusters(sc, initPathFile)
    if (bool) {
      saveSnapShotsToDisk(snapPath, 1, 2, 10)
      println("tc = " + 1 + ", n = " + model.minInitPoints)
    }
    return bool
  }
*/

  /**
   * Method that allows to run the online process from this class.
   *
   * @param data : data that comes from the stream
   *
   * */

  def startOnline(data: DStream[org.apache.spark.mllib.linalg.Vector]): Unit = {
    model.run(data)
  }


}
