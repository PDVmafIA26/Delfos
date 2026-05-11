# Delfos

Delfos is a high school project focused in real life event predictions. It uses inside trading analysis over polymarket to create its predictions.
https://t.me/+mNquI5Ob4WNkNDRk

[![My Skills](https://skillicons.dev/icons?i=python,docker,kafka,spark,airflow)](https://skillicons.dev)

## Overview

### Featrures

## Architecture

## Structure

```
├── bronze-layer/       
├── orchestration/        # Airflow DAGs
├── processing/           # PySpark scripts and ML anomaly models
├── data_ingestion/       # Polymarket data producers
├── notifications/        # Telegram Bot integration
└── README.md             # Project documentation
```

## Prerequisites

To run this project locally, you must have the following installed on your machine:

-Python 3
-Docker engine

## Installation

**1. Clone the repository:**
git clone https://github.com/PDVmafIA26/Delfos.git
cd Delfos

One thing needed for this project to work is a telegram bot for notifying the anomalies detected. Here's a little guide on how to create a bot and get the token to control it.

### How to Create a Telegram Bot and Get Your Token

#### Prerequisites

- A Telegram account (mobile or desktop)
- The Telegram app installed

---

#### Step 1: Open BotFather

BotFather is the official Telegram bot used to create and manage bots.

1. Open Telegram and search for **@BotFather** in the search bar.
2. Make sure it has a **verified checkmark** (blue tick).
3. Tap **Start** or send `/start`.

---

#### Step 2: Create a New Bot

Send the following command to BotFather:

```
/newbot
```

BotFather will then ask you two things:

| Prompt                                      | Example          |
| ------------------------------------------- | ---------------- |
| **Name** – the display name of your bot     | `My Awesome Bot` |
| **Username** – must end in `bot`, no spaces | `my_awesome_bot` |

---

#### Step 3: Get Your Token

Once the username is accepted, BotFather will reply with a message like this:

```
Done! Congratulations on your new bot. You will find it at t.me/my_awesome_bot.
Use this token to access the HTTP API:

123456789:AAFxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

Keep your token secure and store it safely — it can be used by anyone to control your bot.
```

**Copy and save that token.** It follows this format:

```
<bot_id>:<random_string>
```

> ⚠️ **Keep your token private.** Anyone with it can fully control your bot.

---

#### Step 4: Test Your Bot

You can verify it's working by calling the Telegram API in your browser:

```
https://api.telegram.org/bot<YOUR_TOKEN>/getMe
```

A successful response looks like:

```json
{
  "ok": true,
  "result": {
    "id": 123456789,
    "is_bot": true,
    "first_name": "My Awesome Bot",
    "username": "my_awesome_bot"
  }
}
```

---

#### Useful BotFather Commands

| Command           | Description                    |
| ----------------- | ------------------------------ |
| `/newbot`         | Create a new bot               |
| `/mybots`         | List your existing bots        |
| `/setdescription` | Set a description for your bot |
| `/setuserpic`     | Set a profile picture          |
| `/deletebot`      | Delete a bot                   |
| `/revoke`         | Revoke and regenerate a token  |

---

#### Next Steps

- Add your freshly generated token to the Delfos' project `.env` file.

## Usage

## Team

The contribuitors are structured in multidisciplinar topic based teams. Each team focuses in a certain topic such us Geopolitics, but each component has a different role within the project.
The teams are the following:

### Team 1:

- Andrés @andpramor - roles: Systems, Reporting & Notifications.
- Manuel J. @nastupiste - roles: Systems, Analysis & Notifications.
- Tatiana @Tati314 - roles: Ingest, Bronze Layer & Analysis
- Rubén @RubenPR2024 - roles: Ingest, Bronze Layer & Gold Layer

### Team 2:

- Alejandro @BPA-SER-2223 - roles: Gold Layer & Analysis.
- Raúl @RMTorrabadella04 - roles: Orchestration & Notifications
- Jorge @jorgecg646 - roles: Ingest & Reporting
- Pedro @Pedro-ZM - roles: Bronze Layer & Systems

### Team 3:

- Eva M. @edev999 - roles: Orchestration & Gold Layer
- Pablo @PabloBaezaGomez - roles: Ingest & Bronze Layer
- Adrián @4drian04 - roles: Gold Layer, Orchestration, Reporting & Systems
- David @DavidCaraballoBulnes - roles: Ingest, Bronze Layer, Reporting & Notifications

### Team 4:

- Ivana @Ivanasp43 - roles: Bronze Layer & Systems
- Alejandro @Alebernabe5 - roles: Reporting & Orchestration
- Belén @belenmrqz - roles: Analysis & Gold Layer
- Paula @paulaschez - roles: Ingest & Notifications

## Git Workflow & Contributing

Since multiple groups are working on this repository, we follow a strict **GitFlow** branching model to prevent conflicts:

1. **Never commit directly to `main` or `develop`.**
2. Create a new branch for your task from `develop`:
   `git checkout -b feature/[task-description]`
   _(Example: `feature/api-ingestion`)_
3. Commit your changes with descriptive messages.
4. Push your branch and open a **Pull Request (PR)** targeting the `develop` branch.
5. At least one member from another group (or the instructor) must review and approve the PR before merging.
