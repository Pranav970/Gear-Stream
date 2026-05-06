# Gear-Stream
Apache Kafka, Apache Spark, Scala, Databricks, Snowflake, Data Engineering, and Structured Streaming.

<div align="center">

<!-- Animated Header Banner -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&height=250&section=header&text=Real-Time%20Fraud%20Detection&fontSize=50&animation=fadeIn&fontAlignY=38&desc=Enterprise%20Streaming%20Pipeline&descAlignY=55&descAlign=50" alt="Header Banner"/>

<!-- Dynamic Typing Effect -->
<a href="https://git.io/typing-svg">
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=600&size=20&pause=1000&color=2196F3&center=true&vCenter=true&width=600&lines=🚀+High-Throughput+Event+Processing;🕵️‍♂️+Real-Time+Anomaly+Detection;🔁+Exactly-Once+Semantics;❄️+Snowflake+Data+Cloud+Integration" alt="Typing SVG" />
</a>

<br/>

<!-- Badges -->
![Scala](https://img.shields.io/badge/Scala-2.12+-DC322F?style=for-the-badge&logo=scala&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark_Streaming-3.4.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-Kraft-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Cloud-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)

---
**A production-ready, highly scalable, and fault-tolerant streaming pipeline for detecting financial fraud in near real-time.**
</div>

## 📖 Table of Contents
- [✨ Features](#-features)
- [🏗️ Architecture](#️-architecture)
-[🧰 Tech Stack](#-tech-stack)
- [📂 Project Structure](#-project-structure)
-[🚀 Getting Started](#-getting-started)
- [💻 Usage](#-usage)
- [⚙️ Configuration](#️-configuration)

---

## ✨ Features

- **🌊 Stateful Stream Processing:** Utilizes Spark Structured Streaming with **Watermarking** to handle late-arriving events.
- **⏱️ Sliding Windows:** Identifies high-frequency transaction bursts (e.g., > 3 transactions in a 1-minute rolling window).
- **🛡️ Deduplication:** Bulletproof idempotency using `dropDuplicates` to filter out network retries.
- **🔁 Exactly-Once Guarantees:** Kafka offset checkpointing integrated directly with Snowflake micro-batch commits.
- **🚫 Null & Schema Safety:** Strict schema enforcement to prevent `NullPointerException` (NPE) on corrupted payloads.
- **🔌 Plug-and-Play:** Driven purely by environment variables for easy deployment across Databricks, EMR, or local clusters.

---

## 🏗️ Architecture

The pipeline follows a modern decoupled streaming architecture. Data flows continuously from the simulated point-of-sale through Kafka, into Spark for complex event processing (CEP), and lands securely in Snowflake.

```mermaid
graph LR
    A[💳 POS Simulation] -->|JSON Events| B(🦓 Apache Kafka)
    B -->|Stream| C{⚡ Spark Streaming}
    C -->|Micro-batch 1| D[🚨 Amount Alerts]
    C -->|Micro-batch 2| E[📈 Frequency Alerts]
    D -->|ForeachBatch JDBC| F[(❄️ Snowflake)]
    E -->|ForeachBatch JDBC| F
    
    style A fill:#ff9999,stroke:#333,stroke-width:2px
    style B fill:#e6e6fa,stroke:#333,stroke-width:2px
    style C fill:#ffe4b5,stroke:#333,stroke-width:2px
    style F fill:#cce5ff,stroke:#333,stroke-width:2px
