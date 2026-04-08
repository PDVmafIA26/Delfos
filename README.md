# Delfos
Delfos is a high school project focused in real life event predictions. It uses inside trading analysis over polymarket to create its predictions.
![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Message_Broker-black)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-Processing-orange)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-cyan)

## Overview

### Featrures


## Architecture

## Structure
```
├── infrastructure/       # Docker Compose files, database init scripts
├── orchestration/        # Airflow DAGs
├── processing/           # PySpark scripts and ML anomaly models
├── extraction/           # Polymarket data producers
├── notifications/        # Telegram Bot integration
├── requirements.txt      # Global Python dependencies
└── README.md             # Project documentation
```
## Prerequisites 
To run this project locally, you must have the following installed on your machine:
* 
## Installation 

**1. Clone the repository:**
git clone https://github.com/PDVmafIA26/Delfos.git
cd Delfos

## Usage

## Team
The contribuitors are structured in multidisciplinar topic based teams. Each team focuses in a certain topic such us Geopolitics, but each component has a different role within the project.
The teams are the following:
### Team 1: 
* NAME - roles: Data ingest, data reporting
* NAME - roles: Data analysis
* NAME - roles: DB design,


##  Git Workflow & Contributing
Since multiple groups are working on this repository, we follow a strict **GitFlow** branching model to prevent conflicts:

1. **Never commit directly to `main` or `develop`.**
2. Create a new branch for your task from `develop`:
   `git checkout -b feature/[task-description]`
   *(Example: `feature/api-ingestion`)*
3. Commit your changes with descriptive messages.
4. Push your branch and open a **Pull Request (PR)** targeting the `develop` branch.
5. At least one member from another group (or the instructor) must review and approve the PR before merging.
