package clustream

import java.io.File
import java.nio.file.{Files, Paths}
import scala.reflect.io.Path

object OnlineCenters {
  def main(args: Array[String]): Unit = {
  }

  def getCentersPyramidal(clustream:CluStream,snaps:String,tc:Long,h:Int): Unit = {

    val snapshot = clustream.getMCsFromSnapshots(snaps, tc, h)
    val dirPath = Paths.get(s"${Setting.centersOnlinePath}")
    val directory = new File(dirPath.toString)
    val di=new File("src/test/resources/lblFile")
    if (!directory.exists()) {
      Files.createDirectories(dirPath)
    }
    if (!di.exists()) {
      Files.createDirectories(Paths.get("src/test/resources/lblFile"))
    }
    println(snapshot.map(x=>x.getN).mkString(","))
   /* snapshot.filter(s=>s.getN!=0).foreach(z=>Path(s"${"src/test/resources/lblFile"}/mc${tc}").createFile().appendAll(z.getLbl.map(x => x.stripLineEnd).mkString("[", ",", "]") + "\n"))
    var centers = clustream.getCentersFromMC(snapshot).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    centers.foreach(c => Path(s"${Setting.centersOnlinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNum}" + "\n"))
*/
  }
  def getCenters(clustream: CluStream, snaps: String, tc: Long): Unit = {

    val snapshot = clustream.getMCsFromSnapshotSW(snaps, tc)
    // println("mics points = " + snapshot.map(_.getN).sum)
    val dirPath = Paths.get(s"${Setting.centersOnlinePath}")
    val directory = new File(dirPath.toString)
    if (!directory.exists()) {
      Files.createDirectories(dirPath)
    }
    var centers = clustream.getCentersFromMC(snapshot).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    centers.foreach(c => Path(s"${Setting.centersOnlinePath}/centers${tc}").createFile().appendAll(c.toArray.mkString("", ",", "") + s"_${Setting.runNum}" + "\n"))

  }
}