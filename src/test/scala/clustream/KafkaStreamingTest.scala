package clustream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaStreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark CluStream").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .set("spark.ui.enabled", "True")
      .set("spark.ui.port", "4040")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val log = LogManager.getRootLogger
   //log.setLevel(Level.INFO)

    val ssc = new StreamingContext(sc, Milliseconds(1000))
    // val lines = ssc.socketTextStream("localhost", 9998)

    // ------ using kafka -------
    ///     kafka consumer
    // --------------------------
    val topics = Seq("cluTest7")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val model = new dynamicCluWithPca(100, Setting.numDimension, 2000).removeExpiredSW(Setting.rmExpirePoint).setDelta(Setting.windowTime).setM(20).setInitNormalKMeans(true)
    val clustream = new CluStream(model)
    ssc.addStreamingListener(new PrintClustersListener(clustream, sc))
    // if(!Setting.initialize) {
    //  val bool = clustream.StartInitialize(Setting.snapsPath,sc: SparkContext, Setting.initPathFile)
    //  if (bool) Setting.initialize = true
    //  }
    // if(Setting.initialize) {
   // clustream.startOnline(stream.map(z => z.value().split(",").map(_.toDouble)).map(DenseVector(_)))
    clustream.startOnline(stream.map(z => Vectors.dense(z.value().split(",").map(_.toDouble))))//.map(DenseVector(_)))

    // }
    ssc.start()
    ssc.awaitTermination()
  }

}


private[clustream] class PrintClustersListener(clustream: CluStream, sc: SparkContext) extends StreamingListener {
  var durationStep: Long = 0L

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    if (batchCompleted.batchInfo.numRecords > 0) {

      val tc = clustream.model.getCurrentTime
      val n = clustream.model.getTotalPoints
      clustream.saveSnapShotsToDisk(Setting.snapsPath, tc, 2, 10)
      println("tc = " + tc + ", n = " + n)

      if (tc >= Setting.centersStartNum) {
        // online phase centers
       //  OnlineCenters.getCentersPyramidal(clustream, Setting.snapsPath,tc,Setting.windowTime)
       OnlineCenters.getCenters(clustream, Setting.snapsPath, tc)

      }


    }
  }
}


