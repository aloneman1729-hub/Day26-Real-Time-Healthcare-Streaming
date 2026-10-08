package com.day26healthcare

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

case class VitalEvent(
    patientId: String,
    timestamp: String,
    heartRate: Double,
    temperature: Double,
    oxygenSaturation: Double,
    systolicBP: Double,
    diastolicBP: Double
)

case class VitalThresholds(
    minHeartRate: Double,
    maxHeartRate: Double,
    maxTemperature: Double,
    minOxygenSaturation: Double,
    maxSystolicBP: Double,
    minSystolicBP: Double
)

object HealthcareStreamingApp {

  def parseVital(line: String): Option[VitalEvent] = {

    val parts = line.split(",")

    if (parts.length != 7) {
      None
    } else {
      try {
        Some(
          VitalEvent(
            patientId = parts(0).trim,
            timestamp = parts(1).trim,
            heartRate = parts(2).trim.toDouble,
            temperature = parts(3).trim.toDouble,
            oxygenSaturation = parts(4).trim.toDouble,
            systolicBP = parts(5).trim.toDouble,
            diastolicBP = parts(6).trim.toDouble
          )
        )
      } catch {
        case _: NumberFormatException =>
          None
      }
    }
  }

  def abnormalReasons(
      vital: VitalEvent,
      thresholds: VitalThresholds
  ): Seq[String] = {

    var reasons = Seq.empty[String]

    if (vital.heartRate < thresholds.minHeartRate) {
      reasons = reasons :+ "LOW_HEART_RATE"
    }

    if (vital.heartRate > thresholds.maxHeartRate) {
      reasons = reasons :+ "HIGH_HEART_RATE"
    }

    if (vital.temperature > thresholds.maxTemperature) {
      reasons = reasons :+ "HIGH_TEMPERATURE"
    }

    if (vital.oxygenSaturation < thresholds.minOxygenSaturation) {
      reasons = reasons :+ "LOW_OXYGEN_SATURATION"
    }

    if (vital.systolicBP > thresholds.maxSystolicBP) {
      reasons = reasons :+ "HIGH_SYSTOLIC_BP"
    }

    if (vital.systolicBP < thresholds.minSystolicBP) {
      reasons = reasons :+ "LOW_SYSTOLIC_BP"
    }

    reasons
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("HealthcareRealTimeStreaming")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    streamingContext.sparkContext.setLogLevel("WARN")

    /*
     * Broadcast healthcare thresholds.
     */
    val thresholds =
      streamingContext.sparkContext.broadcast(
        VitalThresholds(
          minHeartRate = 60.0,
          maxHeartRate = 100.0,
          maxTemperature = 38.0,
          minOxygenSaturation = 92.0,
          maxSystolicBP = 140.0,
          minSystolicBP = 90.0
        )
      )

    /*
     * Accumulator for abnormal vital readings.
     */
    val abnormalReadingAccumulator =
      streamingContext.sparkContext.longAccumulator(
        "Total Abnormal Vital Readings"
      )

    /*
     * Receive real-time vital events from TCP socket.
     */
    val inputStream =
      streamingContext.socketTextStream(
        "localhost",
        9999
      )

    /*
     * Parse incoming records.
     */
    val vitalStream: DStream[VitalEvent] =
      inputStream.flatMap(parseVital)

    /*
     * Display valid vital readings.
     */
    vitalStream.foreachRDD { rdd =>

      rdd.foreach { vital =>

        println(
          s"VITAL | " +
            s"patient=${vital.patientId} | " +
            s"time=${vital.timestamp} | " +
            s"heartRate=${vital.heartRate} | " +
            s"temperature=${vital.temperature} | " +
            s"SpO2=${vital.oxygenSaturation} | " +
            s"BP=${vital.systolicBP}/${vital.diastolicBP}"
        )
      }
    }

    /*
     * Detect abnormal readings.
     *
     * The thresholds are read from the broadcast variable.
     */
    val abnormalStream: DStream[(String, String, String)] =
      vitalStream.flatMap { vital =>

        val reasons =
          abnormalReasons(
            vital,
            thresholds.value
          )

        if (reasons.nonEmpty) {

          abnormalReadingAccumulator.add(1)

          Some(
            (
              vital.patientId,
              reasons.mkString("|"),
              vital.timestamp
            )
          )

        } else {
          None
        }
      }

    /*
     * Print an immediate alert for every abnormal reading.
     */
    abnormalStream.foreachRDD { rdd =>

      rdd.foreach {
        case (patientId, reasons, timestamp) =>

          println(
            s"ALERT | " +
              s"patient=$patientId | " +
              s"time=$timestamp | " +
              s"reasons=$reasons"
          )
      }
    }

    /*
     * Convert abnormal readings into:
     *
     * patientId -> 1
     *
     * This will be used for stateful and window processing.
     */
    val abnormalByPatient: DStream[(String, Int)] =
      abnormalStream.map {
        case (patientId, _, _) =>
          (patientId, 1)
      }

    /*
     * Stateful processing.
     *
     * The state stores the total number of abnormal
     * readings seen for each patient.
     */
    val patientAbnormalState: DStream[(String, Int)] =
      abnormalByPatient.updateStateByKey[Int](
        (
            newValues: Seq[Int],
            previousState: Option[Int]
        ) => {

          val previousCount =
            previousState.getOrElse(0)

          val currentCount =
            previousCount + newValues.sum

          Some(currentCount)
        }
      )

    /*
     * Print the state for every patient.
     *
     * When a patient reaches 3 or more abnormal
     * readings overall, generate a repeated alert.
     */
    patientAbnormalState.foreachRDD { rdd =>

      rdd.foreach {
        case (patientId, count) =>

          println(
            s"STATE | " +
              s"patient=$patientId | " +
              s"totalAbnormalReadings=$count"
          )

          if (count >= 3) {

            println(
              s"REPEATED ABNORMAL ALERT | " +
                s"patient=$patientId | " +
                s"totalAbnormalReadings=$count"
            )
          }
      }
    }

    /*
     * Window processing.
     *
     * Window duration = 30 seconds
     * Sliding interval = 10 seconds
     */
    val abnormalWindow: DStream[(String, Int)] =
      abnormalByPatient.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(30),
        Seconds(10)
      )

    /*
     * Generate an alert when a patient has at least
     * 3 abnormal readings within the latest 30 seconds.
     */
    abnormalWindow.foreachRDD { rdd =>

      rdd.foreach {
        case (patientId, count) =>

          println(
            s"WINDOW | " +
              s"patient=$patientId | " +
              s"abnormalReadingsLast30Seconds=$count"
          )

          if (count >= 3) {

            println(
              s"WINDOW ALERT | " +
                s"patient=$patientId | " +
                s"abnormalReadingsLast30Seconds=$count"
            )
          }
      }
    }

    /*
     * Display the accumulator value.
     */
    abnormalStream.foreachRDD { _ =>

      println(
        s"TOTAL ABNORMAL READINGS SO FAR = " +
          abnormalReadingAccumulator.value
      )
    }

    /*
     * Checkpointing is required for updateStateByKey.
     */
    streamingContext.checkpoint("checkpoint")

    println()
    println("============================================")
    println("Healthcare Real-Time Streaming Application")
    println("============================================")
    println("Socket: localhost:9999")
    println("Batch interval: 5 seconds")
    println("Window: 30 seconds")
    println("Window slide: 10 seconds")
    println("Repeated abnormal threshold: 3")
    println("Waiting for patient vital events...")
    println("============================================")
    println()

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
