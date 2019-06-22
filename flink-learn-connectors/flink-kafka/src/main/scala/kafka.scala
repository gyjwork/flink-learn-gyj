import java.util.Properties

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, SimpleStringSchema}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}


/**
  * @title: kafka
  * @description: TODO
  * @author gyj
  * @date 2019/5/1710:43
  */
object kafka {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.182.83.222:21005,10.182.83.223:21005,10.182.83.224:21005,10.182.83.225:21005,10.182.83.226:21005")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "10.182.83.227:24002,10.182.83.228:24002,10.182.83.229:24002")
    properties.setProperty("group.id", "test")
    val transaction = env.addSource(new FlinkKafkaConsumer010[String]("gyj_test1", new SimpleStringSchema(), properties).setStartFromLatest())
    val a = transaction.map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5),Time.seconds(1))
      .sum(1)
      .map(t => new ObjectMapper().writeValueAsString(t))
      .addSink(new FlinkKafkaProducer010("10.182.83.222:21005,10.182.83.223:21005,10.182.83.224:21005,10.182.83.225:21005,10.182.83.226:21005","gyj_test2",new SimpleStringSchema ()))
    //.addSink(new SinkToKafka())
    env.execute()
  }


  class SinkToKafka extends RichSinkFunction[(String,Int)] {
    override def invoke(value: (String, Int)) {
      var a = value._1 + "---" + value._2
      import java.util.Properties
      val props = new Properties()
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("bootstrap.servers", "xxx")
      props.put("request.required.acks", "1")
      props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer")
      props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props) // key 和 value 都是 String类型
      val sleepFlag = true
      if (sleepFlag) Thread.sleep(1000)
      producer.send(new ProducerRecord("gyj_test2", a))
      producer.flush()
    }
  }

  class MESSAGE(xc: String, yc: Int) {
    var x: String = xc
    var y: Int = yc
  }
}
