import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, IntegerDeserializer}

import scala.jdk.CollectionConverters._
import java.util.{Collections, Properties}
import java.time.Duration

object Consumer extends App {

  def printLastKafkaMsg(props: Properties, topic_name: String, partition_no: Int, cntMsg: Int) ={

    println(s"Topic: ${topic_name}, Partition No: ${partition_no}, Last ${cntMsg} messages")

    val consumerBooks = new KafkaConsumer(props, new IntegerDeserializer, new StringDeserializer)
    val tpBooks = new TopicPartition(topic_name, partition_no)
    consumerBooks.assign(Collections.singleton(tpBooks))

    consumerBooks.seekToEnd(Collections.singleton(tpBooks))

    val lastOffset = consumerBooks.position(tpBooks)

    if (lastOffset-cntMsg >= 0) {
      consumerBooks.seek(tpBooks, lastOffset - cntMsg)

      consumerBooks
        .poll(Duration.ofSeconds(1))
        .forEach {
          msg => println(s"${msg.partition}\t${msg.offset}\t${msg.key}\t${msg.value}")
        }
      consumerBooks.close()
    }
  }

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumerBooks")
  props.put("auto.offset.reset", "earliest")
  props.put("enable.auto.commit", false)

  printLastKafkaMsg(props,"books",  0, 5)
  printLastKafkaMsg(props,"books",  1, 5)
  printLastKafkaMsg(props,"books",  2, 5)

}

