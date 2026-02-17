# Distributed Job Processing System

A distributed background job processing system built in Go using gRPC and Apache Kafka.  
The system enables asynchronous task execution across services with reliable message delivery, concurrency handling, and fault tolerance mechanisms.

---

## 🚀 Tech Stack

- Go
- gRPC (Protocol Buffers)
- Apache Kafka
- Docker
- Goroutines & Channels (Concurrency)

---

## 📌 Overview

This project demonstrates a distributed architecture for processing background jobs asynchronously using an event-driven model.

It consists of:

- **gRPC API Service** – Handles job submission and status queries
- **Kafka Broker** – Distributes tasks between services
- **Worker Service** – Processes jobs concurrently
- **Dockerized Setup** – Enables multi-service local development

---

## 🏗 Architecture

1. Client submits job via gRPC.
2. API service publishes job event to Kafka.
3. Worker service consumes job from Kafka topic.
4. Worker processes job concurrently using goroutines.
5. Retry logic ensures reliability on transient failures.

---

## ⚙️ Features

- gRPC-based job submission service
- Kafka-based asynchronous job dispatching
- Concurrent worker pool using goroutines and channels
- Retry handling for fault tolerance
- Graceful shutdown using Go `context`
- Dockerized multi-service setup

---

## 🧠 Key Concepts Demonstrated

- Producer–Consumer architecture
- Event-driven systems
- Service decoupling using Kafka
- Concurrent job execution
- Idempotency & retry handling
- Graceful service shutdown

---

## 🐳 Running with Docker

```bash
docker-compose up --build
