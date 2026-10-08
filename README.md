# Day 26 - Real-Time Healthcare Streaming

## Overview

This project demonstrates a real-time healthcare streaming application using
Apache Spark Streaming and Scala.

The application receives patient vital-sign events through a TCP socket,
detects abnormal readings, generates alerts, maintains state for repeated
abnormal readings, and performs window-based analysis.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.1
- Spark Streaming / DStreams
- SBT
- Java 17
- Netcat (`nc`)

## Patient Vital Schema

Each incoming event follows this format:

```text
PATIENT_ID,TIMESTAMP,HEART_RATE,TEMPERATURE,OXYGEN_SATURATION,SYSTOLIC_BP,DIASTOLIC_BP
Example:

P001,2026-10-08T10:00:00,125,39.2,88,155,95
Fields
Field	Description
patientId	Unique patient identifier
timestamp	Vital reading timestamp
heartRate	Heart rate in BPM
temperature	Body temperature
oxygenSaturation	Blood oxygen saturation percentage
systolicBP	Systolic blood pressure
diastolicBP	Diastolic blood pressure
Abnormal Vital Thresholds

The application uses broadcast variables to distribute the following
threshold configuration:

Vital	Abnormal Condition
Heart Rate	Below 60 or above 100
Temperature	Above 38.0
Oxygen Saturation	Below 92
Systolic BP	Above 140 or below 90
Streaming Architecture
Patient Vital Events
        |
        v
TCP Socket - localhost:9999
        |
        v
Spark Streaming
        |
        v
Vital Event Parsing
        |
        +--------------------+
        |                    |
        v                    v
Normal/Abnormal         Abnormal Reasons
Detection                    |
                             v
                    Real-Time Alerts
                             |
             +---------------+---------------+
             |                               |
             v                               v
      Stateful Processing             Window Processing
      updateStateByKey               30-second window
             |                         10-second slide
             v                               |
      Repeated Abnormal                     v
           Alert                       Window Alert
Spark Streaming Features Demonstrated
1. Broadcast Variables

Vital thresholds are stored in a Spark broadcast variable so that the
threshold configuration can be efficiently accessed during stream processing.

2. Accumulator

A Spark accumulator tracks abnormal vital readings detected by the streaming
application.

3. Stateful Processing

updateStateByKey maintains the cumulative number of abnormal readings for
each patient.

A repeated abnormal alert is generated when a patient reaches three or more
abnormal readings.

4. Window Processing

A 30-second window with a 10-second slide is used to monitor recent abnormal
readings.

A window alert is generated when three or more abnormal readings for a
patient occur within the window.

Running the Project
1. Compile
sbt compile
2. Start the socket

Open another terminal:

nc -lk 9999
3. Start the Spark application
sbt run
4. Send sample vital events

In the socket terminal:

P001,2026-10-08T10:00:00,125,39.2,88,155,95
P001,2026-10-08T10:00:05,130,39.5,87,160,100
P001,2026-10-08T10:00:10,135,39.7,86,165,105
Expected Output

The application produces output similar to:

ALERT | patient=P001 | time=2026-10-08T10:00:00 | reasons=HIGH_HEART_RATE|HIGH_TEMPERATURE|LOW_OXYGEN_SATURATION|HIGH_SYSTOLIC_BP

STATE | patient=P001 | totalAbnormalReadings=3

REPEATED ABNORMAL ALERT | patient=P001 | totalAbnormalReadings=3

WINDOW | patient=P001 | abnormalReadingsLast30Seconds=3

WINDOW ALERT | patient=P001 | abnormalReadingsLast30Seconds=3
Key Learning Outcomes
Designing a healthcare vital-event schema
Processing real-time streams with Spark DStreams
Using broadcast variables
Using Spark accumulators
Stateful stream processing with updateStateByKey
Window-based stream processing
Generating real-time abnormal-vital alerts
Working with Scala and Apache Spark Streaming
Project Structure
Day26-Real-Time-Healthcare-Streaming/
│
├── .gitignore
├── README.md
├── build.sbt
│
└── src/
    └── main/
        └── scala/
            └── com/
                └── day26healthcare/
                    └── HealthcareStreamingApp.scala
Result

The project successfully demonstrates a real-time healthcare monitoring
pipeline capable of detecting abnormal patient vital readings and identifying
repeated abnormalities using both stateful and window-based processing.
EOF

