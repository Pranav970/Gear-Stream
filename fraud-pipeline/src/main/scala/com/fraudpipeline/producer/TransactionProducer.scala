package com.fraudpipeline.producer

import com.fraudpipeline.models.Transaction
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Random

object TransactionProducer {
  def main(args: Array[String]): Unit = {
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "transactions")
    val brokers = sys.env.getOrElse("KAFKA_BROKERS", "localhost:9092")

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all") // High durability

    val producer = new KafkaProducer[String, String](props)
    val random = new Random()

    println(s"Starting transaction simulation to Kafka topic: $topic...")

    while (true) {
      val isFraud = random.nextDouble() > 0.95 // 5% chance of anomaly
      
      val accountId = if (isFraud) "ACC_FRAUD_999" else s"ACC_${random.nextInt(100)}"
      val amount = if (isFraud) 15000.0 + random.nextDouble() * 5000 else random.nextDouble() * 1000.0
      
      val tx = Transaction(
        transactionId = UUID.randomUUID().toString,
        accountId = accountId,
        amount = BigDecimal(amount).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
        timestamp = Instant.now().toString,
        merchantId = s"MERCH_${random.nextInt(50)}",
        location = "US"
      )

      val record = new ProducerRecord[String, String](topic, tx.accountId, tx.asJson.noSpaces)
      producer.send(record)

      // Sleep to simulate stream (high frequency burst if fraud account)
      val sleepTime = if (isFraud) 10 else 200 
      Thread.sleep(sleepTime)
    }
  }
}
