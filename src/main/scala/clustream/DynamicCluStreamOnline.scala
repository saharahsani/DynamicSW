
package clustream

/**
 * Created by omar on 9/25/15.
 */

/*
/**
 * CluStreamOnline is a class that contains all the necessary
 * procedures to initialize and maintain the microclusters
 * required by the CluStream method. This approach is adapted
 * to work with batches of data to match the way Spark Streaming
 * works; meaning that every batch of data is considered to have
 * to have the same time stamp.
 *
 * @param q             : the number of microclusters to use. Normally 10 * k is a good choice,
 *                      where k is the number of macro clusters
 * @param numDimensions : this sets the number of attributes of the data
 * @param minInitPoints : minimum number of points to use for the initialization
 *                      of the microclusters. If set to 0 then initRand is used
 *                      insted of initKmeans
 * */

@Experimental
class DynamicCluStreamOnline(
                              val q: Int,
                              val numDimensions: Int,
                              val minInitPoints: Int)
  extends Logging with Serializable {


  /**
   * Easy timer function for blocks
   * */

  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
     logInfo(s"Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  private var mLastPoints = 500
  private var delta = 20
  private var tFactor = 2.0
  private var recursiveOutliersRMSDCheck = true
  private var removeSW = false
  private var time: Long = 0L
  private var N: Long = 0L
  private var currentN: Long = 0L
  //var windowTime: Int = 5
  var durationStep: Long = 0L
  private var microClusters: ListBuffer[MicroCluster] = ListBuffer() //Array.fill(q)(new MicroCluster(Vector(Array.fill[Double](numDimensions)(0.0)), Vector(Array.fill[Double](numDimensions)(0.0)), 0L, 0L, 0L))
  private var microClusterSW: ListBuffer[MicroCluster] = ListBuffer[MicroCluster]()
  private var mcInfo: ListBuffer[(MicroClusterInfo, Int)] = null
  private var mrgArray: ListBuffer[MicroCluster] = ListBuffer()
  private var broadcastQ: Broadcast[Int] = null
  private var broadcastMCInfo: Broadcast[ListBuffer[(MicroClusterInfo, Int)]] = null

  var initialized = false

  private var useNormalKMeans = false
  private var strKmeans: StreamingKMeans = null


  private var initArr: Array[breeze.linalg.Vector[Double]] = Array()

  /**
   * Random initialization of the q microclusters
   *
   * @param rdd : rdd in use from the incoming DStream
   * */

  private def initRand(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    mcInfo = ListBuffer.fill(q)(new MicroClusterInfo(Vector(Array.fill[Double](numDimensions)(rand())), 0.0, 0L)) zip (0 until q)

    val assignations = assignToMicroCluster(rdd, mcInfo)
    updateMicroClusters(assignations)
    var i = 0
    for (mc <- microClusters) {
      mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
      if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x / mc.n.toDouble)
      mcInfo(i)._1.setN(mc.getN)
      if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
      i += 1
    }
    for (mc <- mcInfo) {
      if (mc._1.n == 1)
        mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
    }

    broadcastMCInfo = rdd.context.broadcast(mcInfo)
    initialized = true
  }

  /**
   * Initialization of the q microclusters using the K-Means algorithm
   *
   * @param rdd : rdd in use from the incoming DStream
   * */

  private def initKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    initArr = initArr ++ rdd.collect
    if (initArr.length >= minInitPoints) {
      val tempRDD = rdd.context.parallelize(initArr)
      val trainingSet = tempRDD.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
      val kmeans = new KMeans();
      kmeans.setMaxIterations(10)
      kmeans.setK(q)
      //  kmeans.setSeed(1)
      val clusters = kmeans.run(trainingSet)
      //   val clusters = KMeans.train(trainingSet, q, 10)

      mcInfo = ListBuffer.fill(q)(new MicroClusterInfo(Vector(Array.fill[Double](numDimensions)(0)), 0.0, 0L)) zip (0 until q)
      for (i <- clusters.clusterCenters.indices) mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))

      val assignations = assignToMicroCluster(tempRDD, mcInfo)
      updateMicroClusters(assignations)
      //log.info("befor mcInfo: "+mcInfo.map(x=>x._2).mkString(","))
      microClusters = microClusters.sortBy(x => x.getIds(0))
      var i = 0
      for (mc <- microClusters) {
        mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
        if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x / mc.n.toDouble)
        mcInfo(i)._1.setN(mc.getN)
        if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
        i += 1
      }
      for (mc <- mcInfo) {
        if (mc._1.n == 1)
          mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
      }
      broadcastMCInfo = rdd.context.broadcast(mcInfo)

      initialized = true
    }
  }

  private def initStreamingKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {

    if (strKmeans == null) strKmeans = new StreamingKMeans().setK(q).setRandomCenters(numDimensions, 0.0)
    val trainingSet = rdd.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))

    val clusters = strKmeans.latestModel().update(trainingSet, 1.0, "batches")
    if (currentN >= minInitPoints) {

      mcInfo = ListBuffer.fill(q)(new MicroClusterInfo(Vector(Array.fill[Double](numDimensions)(0)), 0.0, 0L)) zip (0 until q)
      for (i <- clusters.clusterCenters.indices) mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))

      val assignations = assignToMicroCluster(rdd, mcInfo)
      updateMicroClusters(assignations)

      microClusters = microClusters.sortBy(x => x.getIds(0))
      var i = 0
      for (mc <- microClusters) {
        mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
        if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x / mc.n.toDouble)
        mcInfo(i)._1.setN(mc.getN)
        if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
        i += 1
      }
      for (mc <- mcInfo) {
        if (mc._1.n == 1)
          mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
      }

      broadcastMCInfo = rdd.context.broadcast(mcInfo)
      initialized = true
    }

  }

  /**
   * remove old data that expired their time of sliding window
   * update microclusters and mcInfo
   *
   * @return
   */
  def removeOldData(rdd: RDD[Vector[Double]], time: Long) = {
    try {
      val dir = "src/test/resources/snaps"
      if (Files.exists(Paths.get(dir + "/" + time + "SW"))) {
        val snap1: ListBuffer[MicroCluster] = microClusters
        val in = new ObjectInputStream(new FileInputStream(dir + "/" + time + "SW"))
        val snap2 = in.readObject().asInstanceOf[ListBuffer[MicroCluster]]
        in.close()

        // microClusters befor change
        // println("before change: " + snap1.map(x => x.getN).mkString(","))

        /* val arrs1 = snap1.map(_.getIds)
         val arrs2 = snap2.map(_.getIds)
         val relatingMCs = snap1 zip arrs1.map(a => arrs2.zipWithIndex.map(b => if (b._1.toSet.intersect(a.toSet).nonEmpty) b._2; else -1))
         val mcs = relatingMCs.map { mc =>

           if (!mc._2.forall(_ == -1)) {
             for (id <- mc._2) if (id != -1) {
               mc._1.setCf2x(mc._1.getCf2x :- snap2(id).getCf2x)
               mc._1.setCf1x(mc._1.getCf1x :- snap2(id).getCf1x)
               mc._1.setCf2t(mc._1.getCf2t - snap2(id).getCf2t)
               mc._1.setCf1t(mc._1.getCf1t - snap2(id).getCf1t)
               mc._1.setN(mc._1.getN - snap2(id).getN)
               /*  if(mc._1.getIds.toSet.diff(snap2(id).getIds.toSet).nonEmpty) {
                   mc._1.setIds(mc._1.getIds.toSet.diff(snap2(id).getIds.toSet).toArray)
                 }*/
             }
             mc._1
           } else mc._1

         }*/
        // println("snap2 "+ snap2.map(x=>x.getIds(0)).sorted.mkString(","))
        val ddf = microClusters.map(x => x.getN).sum
        log.info("mc before remove item: " + microClusters.size + " count: " + ddf)
        log.info("mcInfo before remove item: " + mcInfo.size)
        val mcs = snap1.map { mc =>
          for (mc2 <- snap2) {
            // mc.getIds.intersect(mc2.getIds).nonEmpty
            if (mc.getIds.contains(mc2.getIds(0))) {
              //println("intersect: "+ mc.getIds.intersect(mc2.getIds).map(x=>x).mkString(","))
              val numCount = mc.getN - mc2.getN
              if (numCount == 0) {
               // mcInfo(microClusters.indexOf(mc)) = (null, -1)
                mc.setN(0)
                 microClusters.remove(microClusters.indexOf(mc))
               /* mc.setCf2x(Vector(Array.fill[Double](numDimensions)(0.0)))
                mc.setCf1x(Vector(Array.fill[Double](numDimensions)(0.0)))
                mc.setCf2t(0L)
                mc.setCf1t(0L)*/


              }
              if(numCount>0) {
                mc.setCf2x(mc.getCf2x - mc2.getCf2x)
                mc.setCf1x(mc.getCf1x - mc2.getCf1x)
                mc.setCf2t(mc.getCf2t - mc2.getCf2t)
                mc.setCf1t(mc.getCf1t - mc2.getCf1t)
                mc.setN(mc.getN - mc2.getN)
                /*  val ss=  mc.ids.toSet.diff(mc2.ids.toSet)
             println("diff ids: "+ss.map(x=>x).mkString(","))*/
              }
            }
          }
          mc
        }

        this.microClusters = mcs.filter(x => x.getN != 0)
        this.mcInfo = mcInfo.filter(x => x._2 != -1)


        // microClusters after change
        // println("after change: " + mcs.map(x => x.getN).mkString(","))
        val df = microClusters.map(x => x.getN).sum
        log.info("mcSW after: " + microClusters.size + " count: " + df)
        log.info("infoSW after: " + mcInfo.size)

        var i = 0
        for (mc <- this.microClusters) {
          mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
          if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x / mc.n.toDouble)
          mcInfo(i)._1.setN(mc.getN)
          if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
          i += 1
        }
        for (mc <- mcInfo) {
          if (mc._1.n == 1)
            mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
        }
        broadcastMCInfo = rdd.context.broadcast(mcInfo)

        //println("mcs ids: "+ snap1.map(x=>x.ids(0)).mkString(","))
        //  println("file " + time + "SW ids: " + snap2.map(x => x.ids(0)).mkString(","))
        // println("file " + time + "SW n: " + snap2.map(x => x.n).mkString(","))


      }
    }
    catch {
      case ex: IOException => println("Exception while reading files " + ex)
        null
    }
  }

  /**
   * method that initialized clusters
   *
   * @param initPathFile
   * @return
   */
  def initializeClusters(sc: SparkContext, initPathFile: String): Boolean = {
    try {
      if (Files.exists(Paths.get(initPathFile + 0))) {
        val rddFile = sc.textFile(initPathFile)
        val rddData = rddFile.map(x => x.split(",").map(_.toDouble)).map(Vector(_))
        currentN = minInitPoints
        minInitPoints match {
          case 0 => initRand(rddData)
          case _ => if (useNormalKMeans) initKmeans(rddData) else initStreamingKmeans(rddData)
        }
        this.time += 1
        this.N += currentN
        return true
      }
      return false
    } catch {
      case ex: IOException => println("Exception while reading files " + ex)
        return false
    }
  }

  /**
   * Main method that runs the entire algorithm. This is called every time the
   * Streaming context handles a batch.
   *
   * @param data : data coming from the stream. Each entry has to be parsed as
   *             breeze.linalg.Vector[Double]
   * */

  def run(data: DStream[breeze.linalg.Vector[Double]]): Unit = {
    data.foreachRDD { (rdd, timeS) =>
      //if(getCurrentTime==250) System.exit(0)
      currentN = rdd.count()
      if (currentN != 0) {
        this.time += 1
        this.N += currentN

        // scaled data before process

        val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(rdd.map(x => Vectors.dense(x.toArray)))
        val data_std = rdd.map(x => scaler2.transform(Vectors.dense(x.toArray)))
        val rdd_std = data_std.map(x => x.toArray).map(DenseVector(_).toVector)

        if (initialized) {
          // val t0=Instant.now()
          if (this.getRemoveExpireSW) {
            val tc = this.getCurrentTime //+ 1
            if (tc > this.delta) {
              val time = tc - this.delta //+ 1
              // check if time < ((tc-wt+1),tc)

              if (time > 0) {
                //  println(s"currentTime: ${this.getCurrentTime + 1}, windowTime: ${this.windowTime}, diffTime: ${time}}")
                removeOldData(rdd_std, time)
              }
            }
          }
          val assignations = assignToMicroCluster(rdd_std)
          updateMicroClusters(assignations)

          var i = 0
          for (mc <- microClusters) {
            mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
            if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x / mc.n.toDouble)
            mcInfo(i)._1.setN(mc.getN)
            if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
            i += 1
          }
          for (mc <- mcInfo) {
            if (mc._1.n == 1)
              mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
          }
          broadcastMCInfo = rdd.context.broadcast(mcInfo)

        } else {
          //val t0=Instant.now
          minInitPoints match {
            case 0 => initRand(rdd_std)
            case _ => if (useNormalKMeans) initKmeans(rdd_std) else initStreamingKmeans(rdd_std)
          }

        }
      }

    }
  }

  /**
   * Method that returns the current array of microclusters.
   *
   * @return Array[MicroCluster]: current array of microclusters
   * */

  def getMicroClusters: ListBuffer[MicroCluster] = {
    this.microClusters
  }

  def getMicroClusterSW: ListBuffer[MicroCluster] = {
    this.microClusterSW
  }

  def getMicroClustersInfo: ListBuffer[(MicroClusterInfo, Int)] = {
    this.mcInfo
  }

  /**
   * Method that returns current time clock unit in the stream.
   *
   * @return Long: current time in stream
   * */

  def getCurrentTime: Long = {
    this.time
  }

  /**
   * Method that returns the total number of points processed so far in
   * the stream.
   *
   * @return Long: total number of points processed
   * */

  def getTotalPoints: Long = {
    this.N
  }

  /**
   * Method that sets if the newly created microclusters due to
   * outliers are able to absorb other outlier points. This is done recursively
   * for all new microclusters, thus disabling these increases slightly the
   * speed of the algorithm but also allows to create overlaping microclusters
   * at this stage.
   *
   * @param ans : true or false
   * @return Class: current class
   * */

  def setRecursiveOutliersRMSDCheck(ans: Boolean): this.type = {
    this.recursiveOutliersRMSDCheck = ans
    this
  }

  /**
   * Changes the K-Means method to use from StreamingKmeans to
   * normal K-Means for the initialization. StreamingKMeans is much
   * faster but in some cases normal K-Means could deliver more
   * accurate initialization.
   *
   * @param ans : true or false
   * @return Class: current class
   * */

  def setInitNormalKMeans(ans: Boolean): this.type = {
    this.useNormalKMeans = ans
    this
  }


  /**
   * Method that sets the m last number of points in a microcluster
   * used to approximate its timestamp (recency value).
   *
   * @param m : m last points
   * @return Class: current class
   * */

  def setM(m: Int): this.type = {
    this.mLastPoints = m
    this
  }

  /**
   * Method that sets the threshold d, used to determine whether a
   * microcluster is safe to delete or not (Tc - d < recency).
   *
   * @param d : threshold
   * @return Class: current class
   * */

  def setDelta(d: Int): this.type = {
    this.delta = d
    this
  }

  /**
   * for sliding window
   *
   * @param bool
   * @return
   */
  def removeExpiredSW(bool: Boolean): this.type = {
    this.removeSW = bool
    this
  }

  def getRemoveExpireSW: Boolean = {
    this.removeSW
  }

  /**
   * Method that sets the factor t of RMSDs. A point whose distance to
   * its nearest microcluster is greater than t*RMSD is considered an
   * outlier.
   *
   * @param t : t factor
   * @return Class: current class
   * */

  def setTFactor(t: Double): this.type = {
    this.tFactor = t
    this
  }

  /**
   * Computes the distance of a point to its nearest microcluster.
   *
   * @param vec : the point
   * @param mcs : Array of microcluster information
   * @return Double: the distance
   * */

  private def distanceNearestMC(vec: breeze.linalg.Vector[Double], mcs: ListBuffer[(MicroClusterInfo, Int)]): Double = {

    var minDist = Double.PositiveInfinity
    var i = 0
    for (mc <- mcs) {
      val dist = squaredDistance(vec, mc._1.centroid)
      if (dist != 0.0 && dist < minDist) minDist = dist
      i += 1
    }
    scala.math.sqrt(minDist)
  }

  /**
   * Computes the squared distance of two microclusters.
   *
   * @param idx1 : local index of one microcluster in the array
   * @param idx2 : local index of another microcluster in the array
   * @return Double: the squared distance
   * */

  private def squaredDistTwoMCArrIdx(idx1: Int, idx2: Int): Double = {
    squaredDistance(microClusters(idx1).getCf1x / microClusters(idx1).getN.toDouble, microClusters(idx2).getCf1x / microClusters(idx2).getN.toDouble)
  }

  /**
   * Computes the squared distance of one microcluster to a point.
   *
   * @param idx1  : local index of the microcluster in the array
   * @param point : the point
   * @return Double: the squared distance
   * */

  private def squaredDistPointToMCArrIdx(idx1: Int, point: Vector[Double]): Double = {
    squaredDistance(microClusters(idx1).getCf1x / microClusters(idx1).getN.toDouble, point)
  }

  /**
   * Returns the local index of a microcluster for a given ID
   *
   * @param idx0 : ID of the microcluster
   * @return Int: local index of the microcluster
   * */

  private def getArrIdxMC(idx0: Int): Int = {
    var id = -1
    var i = 0
    for (mc <- microClusters) {
      if (mc.getIds(0) == idx0) id = i
      i += 1
    }
    id
  }

  /**
   * Merges two microclusters adding all its features.
   *
   * @param idx1 : local index of one microcluster in the array
   * @param idx2 : local index of one microcluster in the array
   *
   * */

  private def mergeMicroClusters(idx1: Int, idx2: Int): Unit = {
    val cf1x = microClusters(idx2).getCf1x
    val cf2x = microClusters(idx2).getCf2x
    val cf1t = microClusters(idx2).getCf1t
    val cf2t = microClusters(idx2).getCf2t
    val N = microClusters(idx2).getN
    val ids = microClusters(idx2).getIds
    val mcc = new MicroCluster(cf2x, cf1x, cf2t, cf1t, N, ids)
    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x + mcc.getCf1x)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x + mcc.getCf2x)
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + mcc.getCf1t)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + mcc.cf2t)
    microClusters(idx1).setN(microClusters(idx1).getN + mcc.getN)
    microClusters(idx1).setIds(microClusters(idx1).getIds ++ mcc.ids)
    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x / microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

  }


  /**
   * Adds one point to a microcluster adding all its features.
   *
   * @param idx1  : local index of the microcluster in the array
   * @param point : the point
   *
   * */

  private def addPointMicroClusters(idx1: Int, point: (Int, Vector[Double])): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x + point._2)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x + (point._2 * point._2))
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + this.time)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + (this.time * this.time))
    microClusters(idx1).setN(microClusters(idx1).getN + 1)
    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x / microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

    //----------------- for each of data -----
    if (microClusterSW.nonEmpty) {
      val mc = microClusterSW.find(x => x.getIds(0) == microClusters(idx1).getIds(0)).orNull
      if (mc != null) {
        mc.setCf1x(mc.getCf1x + point._2)
        mc.setCf2x(mc.getCf2x + (point._2 * point._2))
        mc.setCf1t(mc.getCf1t + this.time)
        mc.setCf2t(mc.getCf2t + (this.time * this.time))
        mc.setN(mc.getN + 1)
      }
      else {
        microClusterSW.append(new MicroCluster(point._2 * point._2, point._2, this.time * this.time, this.time, 1, Array(microClusters(idx1).getIds(0))))
      }
    }
    /*microClusterSW(idx1).setCf1x(microClusterSW(idx1).getCf1x :+ point._2)
    microClusterSW(idx1).setCf2x(microClusterSW(idx1).getCf2x :+ (point._2 :* point._2))
    microClusterSW(idx1).setCf1t(microClusterSW(idx1).getCf1t + this.time)
    microClusterSW(idx1).setCf2t(microClusterSW(idx1).getCf2t + (this.time * this.time))
    microClusterSW(idx1).setN(microClusterSW(idx1).getN + 1)
    microClusterSW(idx1).setIds(microClusterSW(idx1).getIds :+ point._1)*/
  }


  /**
   * Deletes one microcluster and replaces it locally with a new point.
   *
   * @param idx   : local index of the microcluster in the array
   * @param point : the point
   *
   * */

  private def createMicroCluster(point: (Int, Vector[Double])): Int = {
    var mc = new MicroCluster(point._2 * point._2, point._2, this.time * this.time, this.time, 1L)
    microClusters = microClusters :+ mc
    var info = new MicroClusterInfo()
    mcInfo = mcInfo :+ (info, mc.getIds(0))
    info.setCentroid(point._2)
    info.setRmsd(distanceNearestMC(info.centroid, mcInfo))
    info.setN(1L)
    //------------for each data-----------------
    microClusterSW.append(new MicroCluster(point._2 * point._2, point._2, this.time * this.time, this.time, 1L, mc.getIds))
    microClusters.indexOf(mc)
  }


  /**
   * Finds the nearest microcluster for all entries of an RDD.
   *
   * @param rdd    : RDD with points
   * @param mcInfo : Array containing microclusters information
   * @return RDD[(Int, Vector[Double])]: RDD that contains a tuple of the ID of the
   *         nearest microcluster and the point itself.
   *
   * */

  private def assignToMicroCluster(rdd: RDD[Vector[Double]], mcInfo: ListBuffer[(MicroClusterInfo, Int)]): RDD[(Int, Vector[Double])] = {
    rdd.map { a =>
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      var i = 0
      for (mc <- mcInfo) {
        val dist = squaredDistance(a, mc._1.centroid)
        if (dist < minDist) {
          minDist = dist
          minIndex = mc._2
        }
        i += 1
      }
      (minIndex, a)
    }
  }

  /**
   * Finds the nearest microcluster for all entries of an RDD, uses broadcast variable.
   *
   * @param rdd : RDD with points
   * @return RDD[(Int, Vector[Double])]: RDD that contains a tuple of the ID of the
   *         nearest microcluster and the point itself.
   *
   * */
  private def assignToMicroCluster(rdd: RDD[Vector[Double]]) = {
    val gg = rdd.map { a =>
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      var i = 0
      for (mc <- broadcastMCInfo.value) {
        val dist = squaredDistance(a, mc._1.centroid)
        if (dist < minDist) {
          minDist = dist
          minIndex = mc._2
        }
        i += 1
      }
      (minIndex, a)
    }
    // val oo=  gg.collect()
    // println(oo.map(x=>x._1).mkString(","))
    gg
  }

  def getNearestCluster(mc: MicroCluster) = {
    var minDist = Double.PositiveInfinity
    var nearMc: (MicroClusterInfo, Int) = null

    val micro = mcInfo.find(x => x._2 == mc.getIds(0)).orNull
    if (micro != null) {
      for (info <- mcInfo) {
        if (info._2 != micro._2) {
          val dist = squaredDistance(micro._1.centroid, info._1.centroid)
          if (dist < minDist) {
            minDist = dist
            nearMc = info
          }
        }
      }
      (minDist, nearMc)
    }
    else null
  }

  /**
   * Performs all the operations to maintain the microclusters. Assign points that
   * belong to a microclusters, detects outliers and deals with them.
   *
   * @param assignations : RDD that contains a tuple of the ID of the
   *                     nearest microcluster and the point itself.
   *
   * */

  private def updateMicroClusters(assignations: RDD[(Int, Vector[Double])]): Unit = {

    var dataInAndOut: RDD[(Int, (Int, Vector[Double]))] = null
    var dataIn: RDD[(Int, Vector[Double])] = null
    var dataOut: RDD[((Int, Vector[Double]))] = null


    // Calculate RMSD : برای تعیین حداکثر مرز ,برای خوشه هایی که بیش از یک نقطه دارند
    if (initialized) {
      var rmsd = 0.0
      dataInAndOut = assignations.map { a =>
        val nearMCInfo = broadcastMCInfo.value.find(id => id._2 == a._1)
        val nearDistance = scala.math.sqrt(breeze.linalg.squaredDistance(a._2, nearMCInfo.get._1.centroid))
        // tFactor * nearMCInfo.rmsd : تعیین حداکثر مرز به عنوان عاملی از انحراف rms
        if (nearMCInfo.get._1.n == 1) rmsd = 2 * nearMCInfo.get._1.rmsd
        else rmsd = tFactor * nearMCInfo.get._1.rmsd
        //  if(nearMCInfo.get._1.rmsd==0.0) println("rmsd is zero! "+nearMCInfo.get._2)
        if (nearDistance <= rmsd) { //&& tFactor * nearMCInfo.rmsd < 1
          (1, a)
        } //If the data point falls within the maximum boundary of the micro-cluster
        else {
          (0, a)
        }
      }
    }

    // Separate data
    if (dataInAndOut != null) {
      dataIn = dataInAndOut.filter(_._1 == 1).map(a => a._2)
      dataOut = dataInAndOut.filter(_._1 == 0).map(a => a._2)
    } else dataIn = assignations
    //  println("dataIn: " + dataIn.count())
    // if (dataOut != null) println("dataOut: " + dataOut.count())

    // Compute sums, sums of squares and count points... all by key
    // logInfo(s"Processing points")

    // sumsAndSumsSquares -> (key: Int, (sum: Vector[Double], sumSquares: Vector[Double], count: Long ) )
    val sumsAndSumsSquares = timer {
      val aggregateFuntion = (aa: (Vector[Double], Vector[Double], Long), bb: (Vector[Double], Vector[Double], Long)) => (aa._1 + bb._1, aa._2 + bb._2, aa._3 + bb._3)
      dataIn.mapValues(a => (a, a * a, 1L)).reduceByKey(aggregateFuntion).collect()
    }


    var totalIn = 0L
    if (microClusterSW.nonEmpty) {
      microClusterSW.clear()
    }
    for (ss <- sumsAndSumsSquares) {
      var mc: MicroCluster = null
      mc = microClusters.find(x => x.getIds(0) == ss._1).orNull
      if (mc == null) {
        //new
        mc = new MicroCluster()
        mc.setIds(Array(ss._1))
        microClusters = microClusters :+ mc
      }
      mc.setCf1x(mc.cf1x + ss._2._1)
      mc.setCf2x(mc.cf2x + ss._2._2)
      mc.setN(mc.n + ss._2._3)
      mc.setCf1t(mc.cf1t + ss._2._3 * this.time)
      mc.setCf2t(mc.cf2t + ss._2._3 * (this.time * this.time))
      // for SW
      var mm = new MicroCluster(ss._2._2,
        ss._2._1,
        ss._2._3 * (this.time * this.time),
        ss._2._3 * this.time,
        ss._2._3,
        Array(ss._1))
      microClusterSW.append(mm)

      totalIn += ss._2._3
    }
    logInfo(s"Processing " + (currentN - totalIn) + " outliers")
    timer {
      if (dataOut != null && currentN - totalIn != 0) {

        var newMC: Array[Int] = Array()
        for (point <- dataOut.collect()) {

          var minDist = Double.PositiveInfinity
          var idMinDist = 0
          if (recursiveOutliersRMSDCheck) for (idx <- newMC) {
            val dist = squaredDistPointToMCArrIdx(idx, point._2)
            if (dist < minDist) {
              minDist = dist
              idMinDist = idx
            }
          }
          var rmsThereshol = 2
          if (mcInfo.size > 50) rmsThereshol = 4
          if (scala.math.sqrt(minDist) <= rmsThereshol * mcInfo(idMinDist)._1.rmsd) addPointMicroClusters(idMinDist, point)
          else {
            val idx = createMicroCluster(point)
            newMC = newMC :+ idx
          }

        }

        // ..........pruning clusters ...........
        // - -- --  -- -- -   --  --  - - -- -- -

        // remove zero point
        for (mc <- microClusters) {
          if (mc.getN == 0) {
            microClusters = microClusters.filterNot(z => z.getIds(0) == mc.getIds(0))
            mcInfo = mcInfo.filterNot(x => x._2 == mc.getIds(0))
            println("trigred zero point!")
          }
        }
        var mrgCount = 0
       //  if (this.time % this.delta == 0) {
           var mTimeStamp: Double = 0.0
           val recencyThreshold = this.time - delta

           var i = 0
           for (mc <- microClusters) {
             val meanTimeStamp = if (mc.getN > 0) mc.getCf1t.toDouble / mc.getN.toDouble else 0
             val sdTimeStamp = scala.math.sqrt(mc.getCf2t.toDouble / mc.getN.toDouble - meanTimeStamp * meanTimeStamp)

             if (mc.getN < 2 * mLastPoints) mTimeStamp = meanTimeStamp
             else mTimeStamp = Gaussian(meanTimeStamp, sdTimeStamp).inverseCdf(1 - mLastPoints / (2 * mc.getN.toDouble))

             if (mTimeStamp < recencyThreshold || mc.getN == 0) {
               //remove recency mc
               i = i + 1
               mcInfo.remove(microClusters.indexOf(mc))
               microClusters.remove(microClusters.indexOf(mc))

               // microClusters = microClusters.filterNot(z => z.getIds(0) == mc.getIds(0))
               // mcInfo = mcInfo.filterNot(x => x._2 == mc.getIds(0))
             }
           }

         //  logInfo("remove Count ---------> : " + i)

        //   logInfo("mcLen Before: " + microClusters.length + " count: " + microClusters.map(x => x.getN).sum)
        // }

        // merge nearest mc
        var minDist = Double.PositiveInfinity
        var a = 0
        var b = 0
        for (idx1 <- microClusters.length - 1 to 0 by -1) {
          // for (a <- microClusters.indices){
          breakable {
            //for (b <- (0 + a) until microClusters.length) {
            for (idx2 <- (idx1 - 1) to 0 by -1) {
              if (microClusters(idx1) != microClusters(idx2)) {
                val dist = squaredDistance(mcInfo(idx1)._1.centroid, mcInfo(idx2)._1.centroid)
                if (math.sqrt(dist) <= mcInfo(idx2)._1.rmsd) {
                  minDist = dist
                  // mrg in idx b and remove idx a
                  mergeMicroClusters(idx2, idx1)
                  //  mrgArray.append(microClusters(idx2))
                  microClusters.remove(idx1)
                  mcInfo.remove(idx1)
                  //  microClusters = microClusters.filterNot(x => x.getIds(0) == microClusters(idx1).getIds(0))
                  //mcInfo = mcInfo.filterNot(x => x._2 == mcInfo(idx1)._2)
                  mrgCount = mrgCount + 1
                  // log.info("mrg acured!")
                  break
                }
              }
            }
          }
        }
       // logInfo("mcInfoLen: " + mcInfo.length)
     //   logInfo("mrgCount: " + mrgCount)
     //   logInfo("mcLen After: " + microClusters.length + " count: " + microClusters.map(x => x.getN).sum)
        //-------------------   -------------------------------------------------
      }
    }
  }
  // END OF MODEL
}


/**
 * Object complementing the MicroCluster Class to allow it to create
 * new IDs whenever a new instance of it is created.
 *
 * */

protected object MicroCluster extends Serializable {
  private var current = 49

  private def inc = {
    current += 1
    current
  }
}

/**
 * Packs the microcluster object and its features in one single class
 *
 * */

protected class MicroCluster(
                              var cf2x: breeze.linalg.Vector[Double],
                              var cf1x: breeze.linalg.Vector[Double],
                              var cf2t: Long,
                              var cf1t: Long,
                              var n: Long,
                              var ids: Array[Int]

                            ) extends Serializable {
  def this(cf2x: breeze.linalg.Vector[Double], cf1x: breeze.linalg.Vector[Double], cf2t: Long, cf1t: Long, n: Long) = this(cf2x, cf1x, cf2t, cf1t, n, Array(MicroCluster.inc))

  val numDimensions = 2

  def this() = this(Vector(Array.fill(2)(0.0)), Vector(Array.fill(2)(0.0)), 0, 0, 0, Array(0))

  def setCf2x(cf2x: breeze.linalg.Vector[Double]): Unit = {
    this.cf2x = cf2x
  }

  def getCf2x: breeze.linalg.Vector[Double] = {
    this.cf2x
  }

  def setCf1x(cf1x: breeze.linalg.Vector[Double]): Unit = {
    this.cf1x = cf1x
  }

  def getCf1x: breeze.linalg.Vector[Double] = {
    this.cf1x
  }

  def setCf2t(cf2t: Long): Unit = {
    this.cf2t = cf2t
  }

  def getCf2t: Long = {
    this.cf2t
  }

  def setCf1t(cf1t: Long): Unit = {
    this.cf1t = cf1t
  }

  def getCf1t: Long = {
    this.cf1t
  }

  def setN(n: Long): Unit = {
    this.n = n
  }

  def getN: Long = {
    this.n
  }

  def setIds(ids: Array[Int]): Unit = {
    this.ids = ids
  }

  def getIds: Array[Int] = {
    this.ids
  }
}


/**
 * Packs some microcluster information to reduce the amount of data to be
 * broadcasted.
 *
 * */

protected class MicroClusterInfo(
                                  var centroid: breeze.linalg.Vector[Double],
                                  var rmsd: Double,
                                  var n: Long) extends Serializable {
  val numDimensions = 2

  def this() = this(Vector(Array.fill(2)(0.0)), 0.0, 0L)

  def setCentroid(centroid: Vector[Double]): Unit = {
    this.centroid = centroid
  }

  def setRmsd(rmsd: Double): Unit = {
    this.rmsd = rmsd
  }

  def setN(n: Long): Unit = {
    this.n = n
  }
}

*/
