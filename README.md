# Real-Time Kafka Event Processing Pipeline

A Docker-based, real-time event processing system built using **Python, Apache Kafka, and Docker Compose**. This project demonstrates how distributed applications exchange and process messages asynchronously using a Kafka-based producer-consumer architecture.

The application generates sample order events, publishes them to a Kafka topic, validates and processes the messages through a consumer, and stores the processed results in a JSON Lines file.

## Features

* **Event-driven architecture:** Uses Apache Kafka for asynchronous communication between the producer and consumer.
* **Python producer:** Generates and publishes sample customer order events.
* **Kafka consumer:** Subscribes to the order topic, receives messages, validates the data, and processes valid orders.
* **Data validation:** Checks order information before processing.
* **Dockerized environment:** Runs Kafka, ZooKeeper, the producer, and the consumer using Docker Compose.
* **Persistent output:** Saves processed orders to a JSON Lines file.
* **Automated testing:** Includes unit tests using pytest.
* **Continuous integration:** Uses GitHub Actions to run tests automatically when code is pushed or a pull request is opened.

## Technology Stack

| Technology     | Purpose                              |
| -------------- | ------------------------------------ |
| Python         | Application development              |
| Apache Kafka   | Message streaming and event handling |
| ZooKeeper      | Kafka coordination in this setup     |
| Docker         | Containerization                     |
| Docker Compose | Multi-container orchestration        |
| pytest         | Automated testing                    |
| GitHub Actions | Continuous integration               |

## System Architecture

```text
Order Events
     |
     v
+---------------+
| Python        |
| Producer      |
+---------------+
     |
     v
+---------------+
| Apache Kafka  |
| orders topic  |
+---------------+
     |
     v
+---------------+
| Python        |
| Consumer      |
+---------------+
     |
     v
+---------------+
| Validation &  |
| Processing    |
+---------------+
     |
     v
+---------------+
| JSONL Output  |
| File          |
+---------------+
```

ZooKeeper coordinates the Kafka broker in this Docker Compose environment.

## Project Structure

```text
kafka-event-pipeline/
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── consumer.py
│
├── tests/
│   ├── test_event.py
│   ├── test_validation.py
│   └── test_consumer.py
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── data/
│   └── processed_orders.jsonl
│
├── Dockerfile.producer
├── Dockerfile.consumer
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

The `data` directory is created or populated when the consumer processes events. Generated output files are excluded from Git using `.gitignore`.

## Prerequisites

Install the following tools:

* Python 3.11 or a compatible version
* Docker Desktop with Docker Compose
* Git
* Visual Studio Code (recommended)

Make sure Docker Desktop is running before starting the containers.

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/shubham161205/kafka-event-pipeline.git
cd kafka-event-pipeline
```

### 2. Install Python dependencies

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install the dependencies:

```bash
python -m pip install -r requirements.txt
```

### 3. Start the application with Docker Compose

Build the images and start all services:

```bash
docker compose up --build -d
```

Check the running containers:

```bash
docker compose ps
```

View the producer logs:

```bash
docker compose logs producer
```

View the consumer logs:

```bash
docker compose logs consumer
```

You can also follow the logs continuously:

```bash
docker compose logs -f producer
docker compose logs -f consumer
```

### 4. Verify processed events

The consumer saves processed order events to:

```text
data/processed_orders.jsonl
```

On Windows PowerShell, view the output with:

```powershell
Get-Content .\data\processed_orders.jsonl
```

Example output:

```json
{"order_id":"ORD1","customer":"Customer1","product":"Laptop","amount":51000,"status":"PROCESSED"}
{"order_id":"ORD2","customer":"Customer2","product":"Laptop","amount":52000,"status":"PROCESSED"}
```

The output confirms that the consumer has processed the corresponding order events.

The producer sends sample orders and exits, while the consumer remains available to receive events.

### 5. Stop the application

Stop and remove the running containers:

```bash
docker compose down
```

To remove the Compose-managed volumes as well:

```bash
docker compose down -v
```

Use the second command only when you intend to remove the stored Kafka and ZooKeeper data.

## Running Automated Tests

The project includes unit tests for event creation, validation, consumer processing, and file output.

Install the dependencies if you have not already done so:

```bash
python -m pip install -r requirements.txt
```

Run the tests:

```bash
python -m pytest -v
```

The unit tests do not require a running Kafka broker.

## Continuous Integration with GitHub Actions

This repository includes a GitHub Actions workflow:

```text
.github/workflows/ci.yml
```

The workflow automatically installs Python dependencies and runs the pytest test suite when changes are pushed or pull requests are opened, according to the configured triggers.

To view the workflow results:

1. Open the repository on GitHub.
2. Select the **Actions** tab.
3. Open the latest workflow run.
4. Review the test results and logs.

For additional protection, a GitHub branch protection rule can be configured to require the test status check before merging changes into `main`.

## Example Workflow

1. The producer creates sample order events.
2. The producer publishes events to the Kafka `orders` topic.
3. The consumer subscribes to the topic and receives the events.
4. The consumer validates each order.
5. Valid orders are marked as `PROCESSED`.
6. Processed events are written to the JSON Lines output file.

## Concepts Demonstrated

* Event-driven architecture
* Producer-consumer pattern
* Apache Kafka topics and messages
* Kafka consumer groups and offsets
* Asynchronous message processing
* Data validation
* Service separation
* Docker containerization
* Docker Compose orchestration
* Automated unit testing with pytest
* Continuous integration using GitHub Actions

## Learning Outcomes

This project provides practical experience with:

* Event-driven and distributed application architecture
* Kafka topics, producers, and consumers
* Asynchronous message processing
* Data validation and processing
* Docker containerization and orchestration
* Unit testing with pytest
* Continuous integration using GitHub Actions

## Scope and Limitations

This project is designed as an educational demonstration of Kafka-based event processing.

The current setup uses a single Kafka broker and a consumer instance. It is not configured for broker high availability or exactly-once processing.

In certain failure scenarios, such as a crash after writing an output record but before committing a Kafka offset, duplicate output records may occur.

## Future Improvements

* Add a dead-letter topic for invalid events.
* Introduce structured logging and monitoring.
* Store processed orders in PostgreSQL.
* Add retry mechanisms for transient processing failures.
* Support configurable event generation and larger message volumes.
* Add integration tests that run against a live Kafka broker.
* Add multiple Kafka brokers for improved availability.
* Add monitoring and metrics for production-style deployments.

## License

This project is available for educational and learning purposes. Add a license file if you intend to distribute it under a specific open-source license.
