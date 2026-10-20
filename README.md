# CinePrime Serials Uploader (GitHub Actions Offloader)

⚡ **10 Gbps Cloud Runner for Tamil TV Serials Automation (0 MB AWS Bandwidth)**

This repository runs automatically via **GitHub Actions** to download Tamil TV serials (Sun TV, Vijay TV, Zee Tamil) directly from protected streams (TamilDhool, MediaDelivery, Vimeo) with Chrome TLS Impersonation, generate compliant 320x180 thumbnails, probe exact durations, and publish them to rotating Telegram channels and Dump storage.

---

## 🚀 How It Works

1. **Trigger:** Your EC2 Bot (`@CPHSERIALBOT`) sends a tiny webhook request (`< 1 KB`) to GitHub Actions via `repository_dispatch`.
2. **Download & Process:** GitHub Cloud Runners (10 Gbps unmetered network) run `uploader_worker.py`:
   - Bypasses Cloudflare & BunnyCDN using `curl_cffi` Chrome TLS impersonation.
   - Downloads 720p HD stream in ~10-15 seconds.
   - Extracts exact video duration and generates Telegram-compliant 320x180 thumbnails.
3. **Publish:** Publishes directly to Day 1/Day 2 channels, Dump storage, and Public hub (`@cine_serials`).
4. **Deduplicate:** Syncs completed state back to MongoDB (`cph_serials_processed`).
5. **Bandwidth Consumed on EC2:** **0 MB!**

---

## 🔑 GitHub Repository Secrets Configuration

Add the following secrets to your GitHub repository:
**Settings ➔ Secrets and variables ➔ Actions ➔ New repository secret**

| Secret Name | Description |
|---|---|
| `API_ID` | Telegram API ID |
| `API_HASH` | Telegram API Hash |
| `BOT_TOKEN` | Telegram Bot Token (`@CPHSERIALBOT`) |
| `MONGO_URI` | MongoDB Connection URI (`cineprimehub` database) |

---

## 🧪 Manual Run

You can also trigger uploads manually anytime from the **Actions** tab in GitHub:
1. Go to **Actions** ➔ **CinePrime Serial Uploader**.
2. Click **Run workflow**.
3. Enter the TamilDhool episode URL, Serial Name, Date, and Target Channel.
4. Click **Run workflow**!
