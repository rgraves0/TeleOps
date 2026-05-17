# 🤖 TeleOps-AI

<div align="center">

### 🚀 Production-Ready Telegram AI Assistant

Lightweight AI Operations Assistant built with **Python**, **FastAPI**, **Telegram Bot API**, and **Groq AI**.

Designed for:
- ☁️ Oracle Free Tier VPS
- 🪶 Low RAM Servers
- ⚡ Fast Async Performance
- 🤖 AI-Powered Automation

---

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-Async-green)
![Telegram](https://img.shields.io/badge/Telegram-Bot-blue)
![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

</div>

---

# ✨ Features

## 🧠 AI Assistant

Powered by:
- ⚡ Groq API
- 🦙 Llama 3.3 70B Versatile

### Commands

```bash
/ai
```

### Example

```bash
/ai Explain Docker networking
```

---

# 📧 Gmail Integration

Secure Gmail API integration with AI summaries.

### Features

- 📥 Read latest emails
- 🔎 Search emails
- 🤖 AI email summarization
- 🔐 OAuth support

### Commands

```bash
/latest
/find
```

### Example

```bash
/find invoices
```

---

# 📰 RSS Feed Monitoring

Monitor RSS feeds directly from Telegram.

### Features

- ➕ Add feeds
- 📋 List feeds
- 👀 Watch feed updates
- 🤖 AI summaries

### Commands

```bash
/addrss
/feeds
/watch
```

---

# 📅 Event Reminder System

Simple event management system.

### Features

- ➕ Save events
- 📋 List events
- ❌ Delete events

### Commands

```bash
/event
/events
/deleteevent
```

---

# 📝 Notes System

Personal notes inside Telegram.

### Features

- 📝 Save notes
- 📚 List notes
- ❌ Delete notes

### Commands

```bash
/note
/notes
/deletenote
```

---

# 🖥️ Server Monitoring

Monitor your VPS directly from Telegram.

### Features

- 💾 RAM usage
- ⚙️ CPU usage
- 📶 Ping test

### Commands

```bash
/server
/ping
```

---

# ⚡ Tech Stack

| Technology | Usage |
|---|---|
| Python 3.10+ | Backend |
| FastAPI | API framework |
| python-telegram-bot | Telegram bot |
| SQLite | Database |
| Groq API | AI inference |
| Gmail API | Gmail integration |
| Uvicorn | ASGI server |
| Nginx | Reverse proxy |

---

# 📂 Project Structure

```text
TeleOps-AI/
│
├── handlers/
│   ├── __init__.py
│   └── commands.py
│
├── services/
│   └── .gitkeep
│
├── utils/
│   └── .gitkeep
│
├── data/
│   └── .gitkeep
│
├── main.py
├── config.py
├── database.py
├── ai_utils.py
├── gmail_utils.py
├── rss_utils.py
├── monitoring_utils.py
│
├── requirements.txt
├── README.md
├── .env.example
├── .gitignore
└── LICENSE
```

---

# 🔑 Environment Variables

Create a `.env` file:

```env
TELEGRAM_BOT_TOKEN=
APP_URL=
WEBHOOK_SECRET=
GROQ_API_KEY=
TELEGRAM_ADMIN_ID=

GMAIL_CREDENTIALS_FILE=credentials.json
GMAIL_TOKEN_FILE=token.json

DATABASE_PATH=data/teleops.db
```

---

# 🛠️ Installation

## 1️⃣ Clone Repository

```bash
git clone https://github.com/YOUR_USERNAME/TeleOps-AI.git

cd TeleOps-AI
```

---

## 2️⃣ Create Virtual Environment

```bash
python3 -m venv venv

source venv/bin/activate
```

---

## 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

---

# ▶️ Run The Bot

```bash
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

---

# 🌐 Webhook Configuration

Webhook route:

```text
/webhook/{secret}
```

---

# 🔒 Nginx Reverse Proxy

```nginx
server {

    server_name your-domain.com;

    location / {

        proxy_pass http://127.0.0.1:8000;

        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

---

# 🔐 SSL Setup

Install Certbot:

```bash
sudo apt install certbot python3-certbot-nginx
```

Generate SSL:

```bash
sudo certbot --nginx
```

---

# 🤖 Telegram Commands

| Command | Description |
|---|---|
| `/start` | Start the bot |
| `/help` | Show help menu |
| `/about` | About project |
| `/status` | Bot status |
| `/ai` | Ask AI |
| `/latest` | Latest Gmail emails |
| `/find` | Search Gmail |
| `/addrss` | Add RSS feed |
| `/feeds` | List RSS feeds |
| `/watch` | Watch RSS updates |
| `/event` | Save event |
| `/events` | List events |
| `/deleteevent` | Delete event |
| `/note` | Save note |
| `/notes` | List notes |
| `/deletenote` | Delete note |
| `/server` | Server stats |
| `/ping` | Ping test |

---

# ☁️ Deployment

Recommended VPS:

- ☁️ Oracle Cloud Free Tier
- 🐧 Ubuntu 22.04
- 💾 512MB RAM+
- ⚡ 1 vCPU+

---

# 🪶 Lightweight Design

This project is optimized for:
- Low RAM VPS
- Free Tier cloud servers
- Async performance
- Minimal CPU usage

No local LLM required.

AI inference runs on Groq Cloud.

---

# 🔥 Future Improvements

Planned upgrades:

- 🌤️ Internet tools
- 📅 Scheduler system
- 🧠 Tool-aware AI agent
- 🔔 Reminder notifications
- 🎤 Voice assistant
- 🌐 Web dashboard
- 🧩 Plugin system

---

# 🛡️ Security

- 🔐 Webhook secret validation
- 🔒 Environment variables
- 🚫 Sensitive files excluded
- 📁 SQLite local storage

---

# 📜 License

MIT License

---

# ❤️ Credits

Built with:

- FastAPI
- python-telegram-bot
- Groq API
- Gmail API
- SQLite

---

# ⭐ Support

If you like this project:

⭐ Star the repository  
🍴 Fork the project  
🛠️ Contribute improvements

---

<div align="center">

### 🚀 TeleOps-AI

Lightweight Personal AI Operations Assistant

</div>
