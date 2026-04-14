# 🤖 Delfos Discord Bot (IN DEVELOPMENT)

---

## 📋 Features

- Connects to a Discord server and sends messages to a configured channel
- Reads notification data from a JSON file
- Sends richly formatted Discord embeds with title, summary, details, and image
- Color-coded embeds based on topic (politics, sports, tech, etc.)
- All sensitive configuration managed via environment variables

---

## 🗂️ Project Structure

```
discord_delfosbot/
├── assets/
│   └── politics.jpg        # Local images for embeds
├── main.py                 # Bot entry point
├── notification.json       # Notification data (not committed)
├── .env                    # Environment variables (not committed)
├── .env.example            # Environment variable template
├── .gitignore
├── pyproject.toml
└── README.md
```

---

## ⚙️ Setup

### 1. Clone the repository

```bash
git clone https://github.com/PDVmafIA26/Delfos.git
cd Delfos
```

### 2. Create a virtual environment and install dependencies

```bash
python -m venv .venv
.venv\Scripts\activate      # Windows
source .venv/bin/activate   # macOS / Linux

pip install discord.py python-dotenv
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```env
DISCORD_TOKEN=your_bot_token_here
DISCORD_CANAL_ID=your_channel_id_here
DISCORD_SERVER_ID=your_server_id_here
JSON_PATH=notification.json
```

> ⚠️ Never commit your `.env` file. It is already listed in `.gitignore`.

## 🔐 Environment Variables

| Variable              | Description                        |
| --------------------- | ---------------------------------- |
| `DISCORD_TOKEN`     | Your Discord bot token             |
| `DISCORD_CANAL_ID`  | ID of the target channel           |
| `DISCORD_SERVER_ID` | ID of the Discord server           |
| `JSON_PATH`         | Path to the notification JSON file |

---

## 🛠️ Built With

- [Python 3](https://www.python.org/)
- [discord.py](https://discordpy.readthedocs.io/)
- [python-dotenv](https://pypi.org/project/python-dotenv/)
- [ ] [![My Skills](https://skillicons.dev/icons?i=python,discord)](https://skillicons.dev/)

---

## 📄 License

This project is for internal use by the Delfos team.
