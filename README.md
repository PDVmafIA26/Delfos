# Delfos
Delfos is a high school project focused in real life event predictions. It uses inside trading analysis over polymarket to create its predictions.
[![My Skills](https://skillicons.dev/icons?i=python,docker,kafka,spark,airflow)](https://skillicons.dev)
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
* Andrés @andpramor - roles: Systems, Reporting & Notifications.
* Manuel J. @nastupiste - roles: Systems, Analysis & Notifications.
* Tatiana @Tati314 - roles: Ingest, Bronze Layer & Analysis
* Rubén @RubenPR2024 - roles: Ingest, Bronze Layer & Gold Layer

### Team 2: 
* Alejandro @BPA-SER-2223 - roles: Gold Layer & Analysis.
* Raúl @RMTorrabadella04 - roles: Orchestration & Notifications
* Jorge @jorgecg646 - roles: Ingest & Reporting
* Pedro @Pedro-ZM - roles: Bronze Layer & Systems

### Team 3: 
* Eva M. @edev999 - roles: Orchestration & Gold Layer
* Pablo @PabloBaezaGomez - roles: Ingest & Bronze Layer
* Adrián @4drian04 - roles: Gold Layer, Orchestration, Reporting & Systems 
* David @DavidCaraballoBulnes - roles: Ingest, Bronze Layer, Reporting & Notifications

### Team 4: 
* Ivana @Ivanasp43 - roles: Bronze Layer & Systems
* Alejandro @Alebernabe5 - roles: Reporting & Orchestration
* Belén @belenmrqz - roles: Analysis & Gold Layer
* Paula @paulaschez - roles: Ingest & Notifications

##  Git Workflow & Contributing
Since multiple groups are working on this repository, we follow a strict **GitFlow** branching model to prevent conflicts:

1. **Never commit directly to `main` or `develop`.**
2. Create a new branch for your task from `develop`:
   `git checkout -b feature/[task-description]`
   *(Example: `feature/api-ingestion`)*
3. Commit your changes with descriptive messages.
4. Push your branch and open a **Pull Request (PR)** targeting the `develop` branch.
5. At least one member from another group (or the instructor) must review and approve the PR before merging.
