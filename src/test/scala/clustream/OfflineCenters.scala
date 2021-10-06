package clustream

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.{Duration, Instant}
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Path

object OfflineCenters {
  var durationStep: Long = 0L

  def useKmeans(sc: SparkContext, k: Int, numPoint: Integer, snap1: ListBuffer[MicroCluster], clustream: CluStream, tc: Int) = {
    val clusters = clustream.fakeKMeans(sc, k, numPoint, snap1)
    if (clusters != null) {
      clusters.clusterCenters.foreach(c => Path(s"${Setting.centersOfflinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNumber}" + "\n"))
    }
  }

  def useBisectingKmeans(sc: SparkContext, k: Int, numPoint: Integer, snap1: ListBuffer[MicroCluster], clustream: CluStream, tc: Int) = {
    val clusters = clustream.bisectingKmeans(sc, k, numPoint, snap1)
    if (clusters != null) {
      clusters.clusterCenters.foreach(c => Path(s"${Setting.centersOfflinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNumber}" + "\n"))

    }
  }

  def useMrg(sc: SparkContext, k: Int, numPoint: Integer, snap1: ListBuffer[MicroCluster], clustream: CluStream, tc: Int) = {
   val t0=Instant.now
    val clusters = clustream.getFinalClusteringRdd(sc, k, numPoint, snap1)
    val t1=Instant.now
    if(tc==5 | tc==10 | tc==20 |tc==40| tc==80 |tc==100| tc==150 |tc==200) {
      durationStep = Duration.between(t0, t1).toMillis
      println(s"-------------  execution time${tc}: ${durationStep} ms  ---------------")
    }
    if (clusters != null) {

    //  clusters.foreach(c => Path(s"${Setting.centersOfflinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNumber}" + "\n"))

    }
  }

  def main(args: Array[String]): Unit = {
    var startNum: Int = Setting.StartNum
    val loopEndNum: Int = Setting.EndNum
    val dir = Setting.snapsPath
    val h = Setting.horizon
    val k = 5
    val numPoint = Setting.numPoint

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    sc.setLogLevel("ERROR")
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

   /* val dirPath = Paths.get(s"${Setting.centersOfflinePath}")
    val directory = new File(dirPath.toString)
    if (!directory.exists()) {
      Files.createDirectories(dirPath)
    }*/
    val clustream = new CluStream(null)

    for (tc <- startNum to loopEndNum) {

      val snap1 = clustream.getMCsFromSnapshotSW(dir, tc)
      println("snapshots " + "("+ tc +","+ (tc-h)+")" )//clustream.getSnapShots(dir, tc, h.toLong)
      println(snap1.map(a => a.getN).mkString("[", ",", "]"))
      println("mics points = " + snap1.map(_.getN).sum)
      val algoType: Int = Setting.algoName
      algoType match {
        case 0 => useKmeans(sc, k, numPoint, snap1.filter(x=>x.getN!=0), clustream, tc)
        case 1 => useBisectingKmeans(sc, k, numPoint, snap1, clustream, tc)
        case 2 => useMrg(sc, k, numPoint, snap1, clustream, tc)
      }

    }

  }


  def getOfflineCenters(sc: SparkContext, clustream: CluStream, snaps: String, tc: Long) {
    val snapshot = clustream.getMCsFromSnapshots(snaps, tc, 70)

    //println("snapshots----> " + tc) //clustream.getSnapShots(snaps, number, 70)
    //println(snapshot.map(a => a.getN).mkString("[", ",", "]"))
    // println("mics points = " + snapshot.map(_.getN).sum)

    val clusters = clustream.fakeKMeans(sc, 5, 5000, snapshot)

    clusters.clusterCenters.foreach(c => Path(s"src/test/resources/offlineKdd2_sw/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + "\n"))

  }
}