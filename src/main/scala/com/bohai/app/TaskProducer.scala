package com.bohai.app

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * bin/kafka-topics.sh --zookeeper localhost:2181 --list
  * bin/kafka-topics.sh --create --topic bobiao_task --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * Created by liukai on 2020/10/13.
  */

class TaskProducer extends Thread{
  override def run(): Unit = {
    System.out.println("生产数据 ... ... ")
      try{

          TaskProducer.producer.send(new ProducerRecord[String, String]("bobiao_task", "RUN_LDM_CARD_POS_TRAN"))
          Thread.sleep(25000)

      } catch {
        case e: InterruptedException => {
          e.printStackTrace()
        }
      }
    }
}
object TaskProducer extends Thread{
  private var producer: KafkaProducer[String, String] = new KafkaProducer(createProducerConfig())
  def main(args: Array[String]): Unit = {
    new TaskProducer().start()
  }

  private def createProducerConfig():Properties = {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
}
