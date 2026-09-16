# Kafka Learning Project

This repository is a personal learning project focused on understanding Apache Kafka by building a small distributed streaming system with Python.

The goal is not only to make the system work, but to understand what I am building, why each component exists, and how the system evolves over time.

## Project Goals

Through this project, I want to learn how to:

* Run Kafka locally using Docker
* Create and manage Kafka topics
* Build Python producers and consumers
* Understand partitions and offsets
* Understand message keys and partitioning
* Work with consumer groups
* Handle multiple consumers
* Understand replication and fault tolerance
* Process streaming data
* Understand how Kafka behaves when services fail or restart
* Gradually build a distributed event-driven system

The project will grow step by step as I learn new Kafka and distributed-systems concepts.

## Architecture

The initial architecture is intentionally simple:

```text
Python Producer
      |
      v
Kafka Broker
      |
      v
Kafka Topic
      |
      v
Python Consumer
```

The producer sends events to Kafka.

Kafka stores those events inside topics.

The consumer reads and processes those events.

As the project progresses, I will gradually introduce more components, partitions, consumers, services, and failure scenarios.

## Project Structure

```text
kafka-streaming-project/
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── consumer.py
│
├── learning_logs/
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Learning Logs

The `learning_logs/` directory is a development and learning journal for the project.

Each learning log documents two things:

1. **What I worked on**
2. **What I understood from it**

Instead of only keeping the final code, I document the steps I took to build the system, the commands I used, the architecture at that point in time, and my current understanding of the concepts involved.

For example, a learning log may explain:

* How I configured Kafka with Docker
* Why Kafka is using different ports
* How I created a topic
* What a topic represents
* What partitions are
* How offsets identify events
* How a producer sends events
* How a consumer reads events
* Problems I encountered and how I solved them
* How my understanding changed after testing something

The logs are written in my own words and reflect my understanding at that stage of the project.

> **Note:** Learning logs are never AI-generated. They are written entirely by me, by hand, in my own words, based on what I learned from videos, documentation, experimentation, and questions I asked AI while working through the project.

They are not meant to be formal Kafka documentation. They are meant to show the progression of the project and how my understanding develops while I build it.

A log might include diagrams, commands, notes, explanations, and examples such as:

```text
Producer
    |
    | localhost:9092
    v
Kafka Broker
    |
    v
network-events
```

or notes explaining a concept such as:

```text
A partition is an ordered append-only sequence of events.

Each partition has its own offsets:

Partition 0
Offset:     0    1    2
Events:    e1   e2   e3
```

This means the learning logs act as both:

* a record of how the project was built
* a record of what I learned while building it

An example structure could be:

```text
learning_logs/
├── day-01.txt
├── day-02.txt
├── day-03.txt
├── day-04.txt
├── day-05.txt
└── ...
```
> **Note:** Git steps and commands used throughout the project will also be documented in the learning logs.


## Technologies

* Python
* Apache Kafka
* Docker
* Docker Compose

More technologies may be introduced as the project becomes more advanced.

## Learning Approach

The project follows a simple approach:

> Build something, understand why it works, experiment with it, document what I did and what I learned, then move to the next concept.

The system starts intentionally small and will become more complex only when there is a reason to introduce that complexity.
