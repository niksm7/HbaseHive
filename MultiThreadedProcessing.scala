import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object HBaseCopyLimitedRows {
  def main(args: Array[String]): Unit = {
    // Your main method code to set up Spark session, HBase configuration, etc.

    // Define a mutable list to store Put objects
    val putList: mutable.ListBuffer[Put] = mutable.ListBuffer.empty[Put]

    // Your existing code to process batches and obtain rowkeysAndIngestionTimes

    // Multi-threaded processing of rowkeysAndIngestionTimes
    implicit val ec: ExecutionContext = ExecutionContext.global

    val futures: Seq[Future[Unit]] = rowkeysAndIngestionTimes.grouped(100).map { group =>
      Future {
        group.foreach { case (rowKey, ingestionTime) =>
          val rowKeyB = Bytes.toString(rowKey) + "_" + ingestionTime.split(" ")(0) // Form rowkey for table B
          val put = new Put(Bytes.toBytes(rowKeyB))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ingestion_time"), Bytes.toBytes(ingestionTime))

          // Add the Put object to the list in a thread-safe manner
          synchronized {
            putList += put
          }
        }
      }
    }.toSeq

    // Wait for all futures to complete
    Future.sequence(futures).onComplete {
      case Success(_) =>
        // All processing is complete, now insert the Put objects into table B
        tableB.put(putList.asJava)
        // Clear the putList for the next batch
        synchronized {
          putList.clear()
        }

        // Continue with any further processing or cleanup

      case Failure(exception) =>
        // Handle failure
        println(s"Failed to process rowkeysAndIngestionTimes: ${exception.getMessage}")
        // Continue with any further processing or cleanup
    }

    // Your remaining code
  }
}
