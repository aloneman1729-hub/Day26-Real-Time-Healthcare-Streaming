# Day 26 - Real-Time Healthcare Streaming

## Overview

This project demonstrates a real-time healthcare monitoring application using
Scala and Apache Spark Streaming.

The application receives patient vital-sign events through a TCP socket,
parses the incoming events, detects abnormal vital readings, generates alerts,
tracks repeated abnormal readings using stateful processing, and performs
window-based analysis.

The project also demonstrates important Spark concepts including:

- Patient-vital event schema design
- Real-time stream processing
- Broadcast variables
- Accumulators
- Stateful processing
- Window processing
- Partitions
- DAG (Directed Acyclic Graph)
- Fault tolerance
- Checkpointing

---

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.1
- Spark Streaming / DStreams
- SBT
- Java 17
- Netcat (`nc`)

---

## Patient Vital Event Schema

Each incoming patient vital event follows this format:

```text
PATIENT_ID,TIMESTAMP,HEART_RATE,TEMPERATURE,OXYGEN_SATURATION,SYSTOLIC_BP,DIASTOLIC_BP
```

### Example

```text
P001,2026-10-08T10:00:00,125,39.2,88,155,95
```

### Fields

| Field | Description |
|---|---|
| `patientId` | Unique patient identifier |
| `timestamp` | Timestamp of the vital reading |
| `heartRate` | Heart rate in beats per minute |
| `temperature` | Body temperature |
| `oxygenSaturation` | Blood oxygen saturation percentage |
| `systolicBP` | Systolic blood pressure |
| `diastolicBP` | Diastolic blood pressure |

---

## Abnormal Vital Thresholds

The application uses a broadcast variable containing the vital-sign
threshold configuration.

| Vital | Abnormal Condition |
|---|---|
| Heart Rate | Below 60 or above 100 |
| Temperature | Above 38.0 |
| Oxygen Saturation | Below 92 |
| Systolic Blood Pressure | Above 140 or below 90 |

For example, a reading such as:

```text
P001,2026-10-08T10:00:00,125,39.2,88,155,95
```

is abnormal because it contains:

- Heart rate above 100
- Temperature above 38.0
- Oxygen saturation below 92
- Systolic blood pressure above 140

---

## Streaming Architecture

```text
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
        v
VitalEvent DStream
        |
        +-------------------------+
        |                         |
        v                         v
Abnormal Detection          Vital Output
        |
        v
Abnormal Reasons
        |
        +-------------------------+
        |                         |
        v                         v
Stateful Processing        Window Processing
        |                         |
        v                         v
Repeated Abnormal Alert     Window Alert
```

---

## Spark Streaming Configuration

The application uses a 5-second micro-batch interval:

```scala
new StreamingContext(conf, Seconds(5))
```

The application runs locally using:

```text
local[2]
```

This provides two local execution threads.

The application reads streaming data from:

```text
localhost:9999
```

---

## 1. Real-Time Vital Stream Processing

Incoming text records are converted into structured `VitalEvent` objects.

The application parses each record and extracts:

- Patient ID
- Timestamp
- Heart rate
- Temperature
- Oxygen saturation
- Systolic blood pressure
- Diastolic blood pressure

Invalid records that cannot be parsed are ignored.

---

## 2. Abnormal-Vital Alerts

Each vital event is checked against the broadcast threshold configuration.

When one or more values are outside the configured thresholds, an alert is
generated.

Example:

```text
ALERT | patient=P001 | time=2026-10-08T10:00:00 | reasons=HIGH_HEART_RATE|HIGH_TEMPERATURE|LOW_OXYGEN_SATURATION|HIGH_SYSTOLIC_BP
```

This allows the streaming application to identify potentially dangerous
patient readings in real time.

---

## 3. Broadcast Variables

The application uses a Spark broadcast variable for the vital thresholds.

The thresholds are created once and distributed to the Spark executors:

```scala
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
```

### Why Broadcast Variables?

Broadcast variables are useful when the same read-only configuration is
needed by many tasks.

Instead of repeatedly sending the threshold configuration with every task,
Spark can distribute the broadcast value to the executors.

In this project, the broadcast variable contains the healthcare vital
thresholds.

---

## 4. Accumulator

The application uses a Spark LongAccumulator to count abnormal vital
readings:

```scala
streamingContext.sparkContext.longAccumulator(
  "Total Abnormal Vital Readings"
)
```

When an abnormal reading is detected, the accumulator is incremented.

Example output:

```text
TOTAL ABNORMAL READINGS SO FAR = 3
```

### Important Note

Spark accumulators are primarily intended for counters and monitoring
information. Because Spark may recompute tasks, accumulator updates can be
reapplied during recomputation.

Therefore, the accumulator value should be treated as an operational
counter for this demonstration rather than as a guaranteed exactly-once
business-event count.

The stateful and window-based counts are used to demonstrate the actual
patient-level abnormal-reading logic.

---

## 5. Stateful Processing

The project uses `updateStateByKey` to maintain the cumulative number of
abnormal readings for each patient.

The state is maintained using:

```text
patientId -> total abnormal readings
```

Example:

```text
P001 -> 1
P001 -> 2
P001 -> 3
```

When the patient reaches three or more abnormal readings, the application
generates a repeated abnormal alert.

Example:

```text
STATE | patient=P001 | totalAbnormalReadings=3

REPEATED ABNORMAL ALERT | patient=P001 | totalAbnormalReadings=3
```

This demonstrates stateful stream processing because information from
previous micro-batches is maintained and combined with new data.

---

## 6. Window Processing

The application also performs window-based processing.

Configuration:

```text
Window duration: 30 seconds
Window slide: 10 seconds
```

The application uses:

```scala
reduceByKeyAndWindow(
  (a: Int, b: Int) => a + b,
  Seconds(30),
  Seconds(10)
)
```

The window keeps track of abnormal readings occurring during the most recent
30-second period.

If a patient has three or more abnormal readings within the window, a window
alert is generated.

Example:

```text
WINDOW | patient=P001 | abnormalReadingsLast30Seconds=3

WINDOW ALERT | patient=P001 | abnormalReadingsLast30Seconds=3
```

---

# Partitions, DAG, and Fault Tolerance

## 7. Partitions

Spark divides data into partitions so that different portions of the data can
be processed by different tasks.

In Spark Streaming, each micro-batch is processed using Spark RDDs, and those
RDDs can contain multiple partitions.

Conceptually:

```text
Incoming Vital Events
        |
        v
     Spark RDD
     /   |   \
    P0   P1   P2
    |    |    |
  Task Task Task
```

Each partition represents a portion of the data.

Partitions allow Spark to distribute processing across available execution
resources.

This project uses:

```text
local[2]
```

so Spark has two local execution threads available for processing.

### Why Partitions Matter

Partitions provide:

- Parallel processing
- Better resource utilization
- Scalability
- Distribution of work across Spark tasks

The exact number of partitions can depend on the input and the operations
being performed.

---

## 8. DAG - Directed Acyclic Graph

Spark represents a sequence of transformations as a Directed Acyclic Graph
(DAG).

For this healthcare streaming application, the processing flow can be
conceptually represented as:

```text
Socket Input
     |
     v
parseVital
     |
     v
VitalEvent DStream
     |
     +----------------------+
     |                      |
     v                      v
Abnormal Detection      Vital Output
     |
     v
Abnormal Reasons
     |
     +-------------------+
     |                   |
     v                   v
Stateful Processing   Window Processing
     |                   |
     v                   v
Repeated Alert       Window Alert
```

Important transformations in the project include:

```text
flatMap
map
updateStateByKey
reduceByKeyAndWindow
```

Spark uses the transformation graph to determine how the computation should
be executed.

### DAG Benefits

The DAG allows Spark to:

- Understand dependencies between transformations
- Divide work into execution stages
- Schedule tasks efficiently
- Recompute lost data using lineage when possible

---

## 9. Fault Tolerance

Fault tolerance means that Spark can recover from certain failures without
requiring the entire computation to be manually restarted.

Spark provides fault tolerance through mechanisms such as:

- RDD lineage
- Task recomputation
- Checkpointing for stateful streaming

### RDD Lineage

Spark remembers how an RDD was created from previous transformations.

Conceptually:

```text
Input RDD
   |
   v
Transformation 1
   |
   v
Transformation 2
   |
   v
Result RDD
```

If a partition is lost and the required lineage is available, Spark can
recompute the lost partition.

---

## 10. Checkpointing

Stateful streaming requires checkpointing so that Spark can persist important
streaming state and recover it after a failure.

This project enables checkpointing with:

```scala
streamingContext.checkpoint("checkpoint")
```

The checkpoint directory stores information required for recovery of
stateful streaming computations.

Checkpointing is particularly important for:

```text
updateStateByKey
```

because state from previous micro-batches must be maintained.

### Fault-Tolerance Flow

```text
Streaming Application
        |
        v
Micro-Batch Processing
        |
        v
State Update
        |
        v
Checkpoint
        |
        X
     Failure
        |
        v
Recovery
        |
        v
Restore State / Recompute
```

---

## End-to-End Processing Flow

The complete project can be summarized as:

```text
Patient Vital Event
        |
        v
TCP Socket
        |
        v
Spark Streaming
        |
        v
Parse Vital Event
        |
        v
Apply Broadcast Thresholds
        |
        v
Abnormal Detection
        |
        +---------------------------+
        |                           |
        v                           v
Accumulator                  Abnormal Stream
                                    |
                         +----------+----------+
                         |                     |
                         v                     v
                  Stateful State        Window Processing
                         |                     |
                         v                     v
                  Repeated Alert          Window Alert
```

---

## Running the Project

### Step 1 - Compile

From the project directory:

```bash
sbt compile
```

### Step 2 - Start the Socket

Open a second terminal:

```bash
cd ~/Day26-Real-Time-Healthcare-Streaming
nc -lk 9999
```

Leave this terminal running.

### Step 3 - Start the Spark Application

In the first terminal:

```bash
cd ~/Day26-Real-Time-Healthcare-Streaming
sbt run
```

The application displays:

```text
============================================
Healthcare Real-Time Streaming Application
============================================
Socket: localhost:9999
Batch interval: 5 seconds
Window: 30 seconds
Window slide: 10 seconds
Repeated abnormal threshold: 3
Waiting for patient vital events...
============================================
```

### Step 4 - Send Sample Vital Events

In the socket terminal, send:

```text
P001,2026-10-08T10:00:00,125,39.2,88,155,95
P001,2026-10-08T10:00:05,130,39.5,87,160,100
P001,2026-10-08T10:00:10,135,39.7,86,165,105
```

These readings are intentionally abnormal.

---

## Expected Output

The application produces output similar to:

```text
VITAL | patient=P001 | time=2026-10-08T10:00:00 | heartRate=125.0 | temperature=39.2 | SpO2=88.0 | BP=155.0/95.0

ALERT | patient=P001 | time=2026-10-08T10:00:00 | reasons=HIGH_HEART_RATE|HIGH_TEMPERATURE|LOW_OXYGEN_SATURATION|HIGH_SYSTOLIC_BP
```

After three abnormal readings:

```text
STATE | patient=P001 | totalAbnormalReadings=3

REPEATED ABNORMAL ALERT | patient=P001 | totalAbnormalReadings=3

WINDOW | patient=P001 | abnormalReadingsLast30Seconds=3

WINDOW ALERT | patient=P001 | abnormalReadingsLast30Seconds=3
```

---

## Key Learning Outcomes

This project demonstrates:

1. Designing a patient-vital event schema
2. Processing real-time patient vital streams
3. Detecting abnormal vital readings
4. Generating real-time abnormal-vital alerts
5. Using broadcast variables for shared thresholds
6. Using Spark accumulators
7. Performing stateful stream processing
8. Detecting repeated abnormal readings
9. Performing window-based stream processing
10. Understanding Spark partitions
11. Understanding Spark DAG execution
12. Understanding Spark fault tolerance
13. Using checkpointing for stateful streaming recovery
14. Working with Scala and Apache Spark Streaming

---

## Project Structure

```text
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
```

Generated directories such as `target/`, `checkpoint/`, `.bsp/`, and
`project/` are excluded from Git using `.gitignore`.

---

## Result

The project successfully demonstrates a real-time healthcare monitoring
pipeline using Spark Streaming.

It processes patient vital events, identifies abnormal readings, generates
alerts, maintains patient-level abnormal state, performs window-based
analysis, and demonstrates Spark fundamentals including partitions, DAG
execution, and fault tolerance through lineage and checkpointing.
