# TeleOps-AI 🤖

A secure modular Telegram AI Operations Assistant built for automation, workflows, cloud storage search, reminders, AI chat, and system operations.

---

## ✨ Features

### 🤖 AI Assistant

- Conversational AI chat
- Workflow-based task execution
- Automatic tool routing
- English ↔ Burmese language support
- Context memory
- Multi-step AI workflows

---

### 📱 Telegram Integration

- Telegram bot interface
- AI chat mode
- Interactive command system
- Middleware authentication
- Inline callback menus

---

### 📅 Reminder & Calendar System

- Create reminders
- Delete reminders
- List scheduled events
- Persistent scheduler restoration
- APScheduler integration

---

### 🌐 Web Search

- AI-powered web search
- Search summarization
- Internet query workflows
- Serper API integration

---

### ☁️ Cloud Storage Search

- Rclone metadata indexing
- Cloud file search
- Multi-remote support

---

### 📧 Email Assistant

- Unread email summaries
- Inbox integrations
- AI email summarization

---

### 🔐 Security

- Default-deny access model
- Role-based access control
- Middleware validation
- Audit logging
- Admin-only operations

---

# 🏗 Architecture

```text
TeleOps-AI
│
├── app/
│   ├── ai/
│   ├── core/
│   ├── database/
│   ├── interfaces/
│   ├── plugins/
│   ├── services/
│   └── utils/
│
├── data/
├── logs/
├── main.py
└── requirements.txt
```

---

# ⚙️ Requirements

## System Requirements

- Python 3.10+
- Ubuntu / Debian recommended
- 1GB RAM minimum
- Linux VPS recommended

---

# 🚀 Installation

## Clone Repository

```bash
git clone https://github.com/yourusername/TeleOps-AI.git

cd TeleOps-AI
```

---

## Create Virtual Environment

```bash
python3 -m venv venv
```

Activate:

```bash
source venv/bin/activate
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

# 🔧 Environment Configuration

Create `.env`

```env
# =====================================================
# TELEGRAM
# =====================================================

TELEGRAM_BOT_TOKEN=

# =====================================================
# AI PROVIDER
# =====================================================

AI_PROVIDER=groq

GROQ_API_KEY=
GROQ_MODEL=qwen/qwen3-32b

# =====================================================
# WEATHER
# =====================================================

OPENWEATHER_API_KEY=

# =====================================================
# WEB SEARCH
# =====================================================

SERPER_API_KEY=

# =====================================================
# DATABASE
# =====================================================

DATABASE_PATH=data/teleops.db

# =====================================================
# SYSTEM
# =====================================================

TIMEZONE=Asia/Bangkok
```

---

# ▶️ Running the Project

```bash
python main.py
```

---

# 📖 Telegram Commands

## 🤖 AI Commands

| Command | Description |
|---|---|
| `/ai` | Enable AI chat mode |
| `/exitai` | Disable AI chat mode |
| `/clear` | Clear AI conversation memory |

---

## 📅 Calendar Commands

| Command | Description |
|---|---|
| `/calendar` | Open calendar menu |

### Calendar Features

- Add reminders
- List reminders
- Delete reminders
- Persistent scheduler jobs

---

# 💬 AI Chat Examples

## 🌦 Weather

```text
weather in tokyo
```

```text
ရန်ကုန်ရာသီဥတု
```

---

## 🌐 Web Search

```text
latest AI news
```

```text
YGN to BKK flight schedule
```

---

## ☁️ Cloud Storage Search

```text
find backup.zip
```

---

## 📧 Email Summary

```text
check unread emails
```

---

# 🗄 Database

SQLite database location:

```text
data/teleops.db
```

Core tables:

- users
- roles
- reminders
- audit_logs
- inboxes
- user_inboxes
- chat_memory

---

# 🧩 Plugin System

Plugins are loaded dynamically from:

```text
app/plugins/
```

Example plugins:

```text
weather/
websearch/
```

Each plugin exposes async tool functions used by the AI workflow router.

---

# 🧠 Supported AI Providers

## ⚡ Groq

Recommended for lightweight VPS deployment.

Recommended models:

```text
qwen/qwen3-32b
deepseek-r1-distill-llama-70b
```

---

## 🔷 Gemini

Supported but quota-limited on free tier.

Recommended models:

```text
gemini-2.0-flash
gemini-2.0-flash-lite
```

---

# 🖥 Recommended VPS Providers

- Oracle Cloud Free Tier
- Hetzner
- Contabo
- DigitalOcean

---

# 🛠 Recommended Stack

```text
Ubuntu 22.04
Python 3.10+
Systemd
Nginx (optional)
```

---

# 🚢 Deployment

## Run with Systemd

Example service:

```ini
[Unit]
Description=TeleOps-AI
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/TeleOps
ExecStart=/home/ubuntu/TeleOps/venv/bin/python main.py
Restart=always

[Install]
WantedBy=multi-user.target
```

---

## Enable Service

```bash
sudo systemctl daemon-reload

sudo systemctl enable teleops

sudo systemctl start teleops
```

---

## 📜 Check Logs

```bash
journalctl -u teleops -f
```

---

# 🔐 Security Notes

- Never expose `.env`
- Protect API keys
- Restrict Telegram access
- Use firewall rules
- Rotate secrets regularly

---

# 🧯 Troubleshooting

## Telegram Bot Not Responding

Verify:

```bash
python main.py
```

Check:

- TELEGRAM_BOT_TOKEN
- Middleware authentication
- Handler registration

---

## AI Provider Errors

Check:

- API keys
- Model names
- Provider quotas

---

## Database Errors

Reset database:

```bash
rm -f data/teleops.db
```

Restart:

```bash
python main.py
```

---

# 🛣 Future Roadmap

- Gmail OAuth integration
- Shared inbox support
- Voice commands
- Docker deployment
- Web dashboard
- Multi-user RBAC
- Vector memory
- Multi-agent workflows

---

# 📄 License

MIT License

---

# ⚠️ Disclaimer

This project is intended for educational, automation, and personal operational use.

Users are responsible for:

- API usage
- Infrastructure security
- Credential management
- Compliance with platform policies
