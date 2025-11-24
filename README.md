# Iot-Equipment-Health-Analytics

## Project Overview
This project implements a *real-time machine health monitoring solution* using AWS services. The goal is to detect anomalies and prevent unplanned downtime by leveraging streaming data, robust ETL pipelines, and interactive dashboards.

## Problem Statement
- Unplanned machine downtime leads to high operational costs.
- Lack of real-time monitoring increases risk of failure.
- Manual analysis of sensor data is inefficient.
- Need for predictive maintenance to improve uptime and efficiency.

## Solution Highlights
- *Synthetic Data Generation*: Lambda function triggered by EventBridge every 2 minutes to simulate machine data (temperature, pressure, vibration, timestamp).
- *Streaming Pipeline*: Kinesis Data Stream + Firehose to capture and deliver data to S3 Raw Zone.
- *ETL Workflow*:
  - *Job 1*: Clean data, enforce schema, store in curated table.
  - *Job 2*: Feature engineering (fail_count, ok_count, warm_count), hourly aggregations, statistical metrics.
- *Analytics Layer*: Golden table connected to Athena and visualized in QuickSight.

## Architecture
AWS Components:
- Lambda
- EventBridge
- Kinesis Data Stream & Firehose
- S3 (Raw, Curated, Golden layers)
- Glue (Crawler + ETL Jobs)
- Athena
- QuickSuite
- Lake Formation

## Dashboard Features
- Average vibration level per equipment.
- Daily/Hourly anomaly count.
- Equipment health score (0–100).


## Tech Stack
- *AWS Services*: Lambda, EventBridge, Kinesis, Firehose, S3, Glue, Athena, QuickSuite, Lake Formation.
- *Languages*: Python (Lambda, ETL).

## Flow Diagram

![Architecture Diagram](Snapshots/Capstone_workflow.png)

## QuickSuite Dashboard
![Dashboard](Snapshots/Capstone_dashboard.png)



