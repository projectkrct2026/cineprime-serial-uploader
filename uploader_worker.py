#!/usr/bin/env python3
"""
CinePrime Fully Autonomous Serial Uploader (GitHub Actions 10 Gbps Runner)
========================================================================
- Runs 24/7 on GitHub Cloud Runners (10 Gbps unmetered network)
- 100% Free - Consumes 0 MB of AWS EC2 Bandwidth
- Real-Time Telegram Progress Bar (Download [██████░░░░] -> Upload [██████████])
- Dual Operational Modes:
    1. Single Mode: Dispatched instantly by EC2 bot via webhook when a new serial is found.
    2. Auto Mode: Triggered automatically every 20 minutes by GitHub cron to scrape Sun TV,
       Vijay TV, and Zee Tamil, downloading & uploading any missing daily episodes hands-free.
- Resolves protected streams (TamilDhool, BunnyCDN, Vimeo) with Chrome TLS Impersonation.
- Generates compliant 320x180 thumbnails & exact duration badges.
- Publishes to Day 1 / Day 2 rotating channels, Dump Storage, and Public Hub.
- Syncs completed status back to MongoDB (cph_serials_processed).
"""

import os
import sys
import re
import json
import time
import shutil
import asyncio
import tempfile
import subprocess
from datetime import datetime, timezone, timedelta

import aiohttp
from bs4 import BeautifulSoup
from curl_cffi import requests
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, MessageNotModified
import pyrogram.utils

# Pyrogram 64-bit Channel ID support
pyrogram.utils.MIN_CHANNEL_ID = -1009999999999999
pyrogram.utils.MIN_CHAT_ID = -2147483647

IST = timezone(timedelta(hours=5, minutes=30))

# 1. Configuration & Secrets
API_ID = int(os.getenv("API_ID", "36869346"))
API_HASH = os.getenv("API_HASH") or os.getenv("APT_HASH") or "9abc474ef05c5e46b2210b02eb4c81fc"
BOT_TOKEN = os.getenv("BOT_TOKEN", "8654460032:AAF51fHYkqf1HE7FV8obL8X7aFvH7hbgj0I")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://firee4531_db_user:firee_2006@cluster0.n2iizcz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "1391692876"))
LEECH_GROUP_ID = int(os.getenv("LEECH_GROUP_ID", "-1001978437651"))

DEFAULT_BRAND = "@CinePrimeHub"
MAIN_SERIALS_USERNAME = "@cine_serials"
DUMP_CHANNEL_ID = -1004168330397
PUBLIC_CHANNEL_ID = -1004401720796

# Day 1 Channels (Odd days: 1, 3, 5, 7, 9...)
CHANNELS_DAY1 = {
    "SUN": -1003913807258,
    "VIJAY": -1004426277997,
    "ZEE": -1004359462556
}

# Day 2 Channels (Even days: 2, 4, 6, 8, 10...)
CHANNELS_DAY2 = {
    "SUN": -1003853458965,
    "VIJAY": -1003898846073,
    "ZEE": -1004487133721
}

CATEGORY_PAGES = [
    ("SUN", "https://tamildhool.tech/sun-tv/"),
    ("VIJAY", "https://tamildhool.tech/star-vijay/"),
    ("ZEE", "https://tamildhool.tech/zee-tamil/")
]

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://tamildhool.tech/",
    "Origin": "https://tamildhool.tech"
}

def get_current_rotation(dt=None):
    if not dt:
        dt = datetime.now(IST)
    day_num = dt.day
    is_day_1 = (day_num % 2 != 0)
    label = "Day 1" if is_day_1 else "Day 2"
    chans = CHANNELS_DAY1 if is_day_1 else CHANNELS_DAY2
    return label, chans

def format_size(bytes_val):
    if not bytes_val or bytes_val <= 0: return "0.0 MB"
    mb = bytes_val / (1024 * 1024)
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.1f} MB"

def format_speed(speed_bps):
    if not speed_bps or speed_bps <= 0: return "0 KB/s"
    mbps = speed_bps / (1024 * 1024)
    if mbps >= 1.0:
        return f"{mbps:.1f} MB/s"
    return f"{speed_bps / 1024:.0f} KB/s"

def format_eta(seconds):
    if not seconds or seconds <= 0: return "--"
    m = int(seconds) // 60
    s = int(seconds) % 60
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

def make_progress_bar(current, total, length=10):
    pct = max(0.0, min(1.0, current / total)) if total > 0 else 0.0
    filled = int(round(length * pct))
    bar = "█" * filled + "░" * (length - filled)
    return bar, f"{pct * 100:.1f}%"

class TelegramLiveReporter:
    def __init__(self, client: Client, chat_id: int, loop):
        self.client = client
        self.chat_id = chat_id
        self.loop = loop
        self.status_msg = None
        self.last_update = 0.0
        self.update_interval = 4.0  # 4 seconds protects against Telegram FloodWait
        self.task_name = ""
        self.network = ""
        self.date = ""
        self.target_channel = 0
        self.day_label = ""
        self.stage = "Initializing"
        self.start_time = time.time()

    async def init_message(self, task_name, network, date, target_channel, day_label):
        self.task_name = task_name
        self.network = network
        self.date = date
        self.target_channel = target_channel
        self.day_label = day_label
        self.stage = "Starting Runner 🚀"
        self.start_time = time.time()
        
        try:
            text = self.render_card(0, 1, 0, 0, "Initializing Cloud Runner 🚀")
            self.status_msg = await self.client.send_message(
                chat_id=self.chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📢 Main Serials Hub", url="https://t.me/cine_serials")
                ]])
            )
        except Exception as e:
            print(f"Notice creating status message: {e}")

    def render_card(self, cur_b, tot_b, spd, eta, stage_label):
        bar, pct_str = make_progress_bar(cur_b, tot_b)
        elapsed = int(time.time() - self.start_time)
        el_m, el_s = divmod(elapsed, 60)
        elapsed_str = f"{el_m}m {el_s}s" if el_m else f"{el_s}s"
        tag_name = f"@CinePrimeHub_{re.sub(r'[^a-zA-Z0-9_]', '_', self.task_name)}_{self.date}.mp4"

        return (
            f"**CinePrime Serials Bot**                                `720p HD`\n"
            f"**Dharani** ➔ **{self.network} TV**\n"
            f"`{tag_name}`\n"
            f"📁 **Name:** `{self.task_name} ({self.date})`\n"
            f"[{bar}] **{pct_str}**\n"
            f"├ 🌀 **Process:** `{format_size(cur_b)} of {format_size(tot_b)}`\n"
            f"├ ⏳ **Status:** `{stage_label}` | **ETA:** `{format_eta(eta)}`\n"
            f"├ 📶 **Speed:** `{format_speed(spd)}` | **Elapsed:** `{elapsed_str}`\n"
            f"├ ⚙️ **Engine:** `GitHub Cloud Runner (10 Gbps) • 0 MB EC2`\n"
            f"├ 🏷️ **Mode:** `#SerialLeech` | `#{self.day_label.replace(' ', '')}Rotation`\n"
            f"├ 📡 **Target:** `{self.network} TV ({self.day_label})` ➔ `{self.target_channel}`\n"
            f"└ 👤 **User:** Dharani | **ID:** `1391692876`"
        )

    def on_download_progress(self, d):
        if d.get('status') == 'downloading':
            now = time.time()
            if now - self.last_update < self.update_interval:
                return
            self.last_update = now

            downloaded = d.get('downloaded_bytes')
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            speed = d.get('speed') or 0
            eta = d.get('eta') or 0

            # Handle HLS / chunk downloads where total_bytes is None
            frag_idx = d.get('fragment_index')
            frag_cnt = d.get('fragment_count')

            if frag_cnt and frag_cnt > 0:
                cur_frag = frag_idx or 0
                if not downloaded:
                    downloaded = int(cur_frag * 460 * 1024)
                if not total:
                    total = int(frag_cnt * 460 * 1024)
                if not speed:
                    elapsed = max(1.0, now - self.start_time)
                    speed = downloaded / elapsed
                if not eta and speed > 0:
                    eta = max(0, (total - downloaded) / speed)

            downloaded = downloaded or 0
            total = total or 1

            text = self.render_card(downloaded, total, speed, eta, "Downloading (10 Gbps ⚡)")
            self.schedule_edit(text)

    async def update_stage(self, stage_name):
        self.stage = stage_name
        text = self.render_card(0, 1, 0, 0, stage_name)
        await self.edit_text_safe(text)

    async def on_upload_progress(self, current, total):
        now = time.time()
        if now - self.last_update < self.update_interval and current < total:
            return
        self.last_update = now

        elapsed = max(1, time.time() - self.start_time)
        speed = current / elapsed
        eta = (total - current) / max(1, speed)

        text = self.render_card(current, total, speed, eta, "Uploading to Telegram 📤")
        await self.edit_text_safe(text)

    async def complete_card(self, channel_name):
        elapsed = int(time.time() - self.start_time)
        el_m, el_s = divmod(elapsed, 60)
        elapsed_str = f"{el_m}m {el_s}s" if el_m else f"{el_s}s"
        bar = "██████████"
        text = (
            f"**CinePrime Serials Bot**                                `720p HD`\n"
            f"**Dharani** ➔ **{self.network} TV**\n"
            f"📁 **Name:** `{self.task_name} ({self.date})`\n"
            f"[{bar}] **100.0%** ✅\n"
            f"├ ⏳ **Status:** `Successfully Published!`\n"
            f"├ ⏱️ **Total Time:** `{elapsed_str}`\n"
            f"├ ⚙️ **Engine:** `GitHub Cloud Runner (10 Gbps) • 0 MB EC2`\n"
            f"├ 📡 **Destination:** `{self.network} TV ({self.day_label})`\n"
            f"└ 👤 **User:** Dharani | **ID:** `1391692876`\n\n"
            f"✅ **Episode is live in channel and dump storage!**"
        )
        await self.edit_text_safe(text)

    def schedule_edit(self, text):
        if self.status_msg and self.loop:
            asyncio.run_coroutine_threadsafe(self.edit_text_safe(text), self.loop)

    async def edit_text_safe(self, text):
        if not self.status_msg:
            return
        try:
            await self.status_msg.edit_text(
                text=text,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📢 Main Serials Hub", url="https://t.me/cine_serials")
                ]])
            )
        except MessageNotModified:
            pass
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except Exception as e:
            print(f"Notice editing status message: {e}")

def extract_episode_metadata(title: str, href: str):
    date_m = re.search(r'\b(\d{1,2}[-–./]\d{1,2}[-–./](?:20)?\d{2})\b', title) or re.search(r'\b(\d{1,2}[-–./]\d{1,2}[-–./](?:20)?\d{2})\b', href)
    if not date_m:
        date_short = re.search(r'\b(\d{1,2}[-–./]\d{1,2})\b', title) or re.search(r'\b(\d{1,2}[-–./]\d{1,2})\b', href)
        if date_short:
            d_parts = re.split(r'[-–./]', date_short.group(1))
            if len(d_parts) == 2:
                cur_yr = datetime.now(IST).year
                ep_date = f"{int(d_parts[0]):02d}-{int(d_parts[1]):02d}-{cur_yr}"
            else:
                return None, None, None
        else:
            return None, None, None
    else:
        raw_d = date_m.group(1)
        d_parts = re.split(r'[-–./]', raw_d)
        if len(d_parts) == 3:
            y = int(d_parts[2])
            if y < 100: y += 2000
            ep_date = f"{int(d_parts[0]):02d}-{int(d_parts[1]):02d}-{y}"
        else:
            return None, None, None

    clean_name = re.sub(r'\|\s*\d{1,2}[-–.]\d{1,2}[-–.](?:20)?\d{2}\s*\|.*', '', title).strip()
    clean_name = re.sub(r'\s*\|\s*(?:Vijay|Sun|Zee)\s*Tv\s*(?:Serial|Show).*', '', clean_name, flags=re.IGNORECASE).strip()
    clean_name = re.sub(r'\s*\|\s*Day\s*\d+.*', '', clean_name, flags=re.IGNORECASE).strip()
    clean_name = re.sub(r'\s*Season\s*\d+.*', '', clean_name, flags=re.IGNORECASE).strip()
    clean_name = re.sub(r'\b\d{1,2}[-–./]\d{1,2}[-–./](?:20)?\d{2}\b', '', clean_name).strip()
    clean_name = re.sub(r'\b\d{1,2}[-–./]\d{1,2}\b', '', clean_name).strip()
    clean_name = re.sub(r'\s*(?:Sun|Vijay|Zee)\s*Tv\s*(?:Serial|Show)?\b.*', '', clean_name, flags=re.IGNORECASE).strip()
    clean_name = clean_name.replace("-", " ").strip()
    clean_name = re.sub(r'\s+', ' ', clean_name).title().strip()

    if not clean_name or len(clean_name) < 3:
        return None, None, None

    ep_tag = ""
    day_m = re.search(r'\b(Day\s*\d+)\b', title, re.IGNORECASE)
    if day_m:
        ep_tag = day_m.group(1).title()
    else:
        ep_m = re.search(r'\b(EP\s*\d+|Episode\s*\d+)\b', title, re.IGNORECASE)
        if ep_m:
            ep_tag = ep_m.group(1).upper()

    return clean_name, ep_date, ep_tag

def parse_input_payload():
    raw_payload = os.getenv("PAYLOAD_JSON")
    payload = {}
    if raw_payload and raw_payload.strip() not in ["null", "None", ""]:
        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception as e:
            print(f"Notice parsing PAYLOAD_JSON: {e}")

    url = payload.get("url") or os.getenv("MANUAL_URL")
    if not url or url.strip() == "":
        return None  # Triggers Auto-Scrape Mode

    serial_name = payload.get("serial_name") or os.getenv("MANUAL_NAME") or "Serial"
    date = payload.get("date") or os.getenv("MANUAL_DATE") or datetime.now(IST).strftime("%d-%m-%Y")
    network = payload.get("network") or os.getenv("MANUAL_NET") or "SUN"
    
    label, chans = get_current_rotation()
    target_channel = int(payload.get("target_channel") or os.getenv("MANUAL_CHAN") or chans.get(network, chans["SUN"]))
    day_label = payload.get("day_label") or os.getenv("MANUAL_DAY") or label
    ep_tag = payload.get("ep_tag") or ""
    post_title = payload.get("post_title") or f"{serial_name} {date}"
    status_chat_id = int(payload.get("status_chat_id") or os.getenv("STATUS_CHAT_ID") or ADMIN_USER_ID)

    return {
        "url": url.strip(),
        "serial_name": serial_name.strip(),
        "date": date.strip(),
        "network": network.strip().upper(),
        "target_channel": target_channel,
        "day_label": day_label.strip(),
        "ep_tag": ep_tag,
        "post_title": post_title,
        "status_chat_id": status_chat_id
    }

def resolve_video_id_stream(vid_id: str):
    tt_url = f"https://teamstoday.com/?video={vid_id}"
    headers_tt = {"User-Agent": HTTP_HEADERS["User-Agent"], "Referer": "https://www.tamildhool.tech/"}
    next_url = None
    try:
        r1 = requests.get(tt_url, headers=headers_tt, impersonate="chrome110", timeout=12)
        if r1.status_code == 200:
            m_redir = re.search(r'url=(https?://[^\'\" >]+)', r1.text) or re.search(r'href=[\'\"](https?://[^\'\">]+video=[^\'\">]+)', r1.text)
            if m_redir:
                next_url = m_redir.group(1)
    except Exception as e:
        print(f"Notice fetching teamstoday for {vid_id}: {e}")

    if not next_url:
        next_url = f"https://futuregentrends.com/?video={vid_id}"

    headers_next = {"User-Agent": HTTP_HEADERS["User-Agent"], "Referer": "https://teamstoday.com/"}
    try:
        r2 = requests.get(next_url, headers=headers_next, impersonate="chrome110", timeout=12)
        if r2.status_code == 200:
            soup2 = BeautifulSoup(r2.text, "html.parser")
            for ifr2 in soup2.find_all("iframe"):
                s2 = ifr2.get("src") or ifr2.get("data-src") or ""
                if not s2: continue
                if any(k in s2 for k in ["mediadelivery.net", "vimeo.com", "dailymotion.com", "streamwish", "filelions"]):
                    return s2, next_url
                if any(k in s2 for k in ["embed", "player", "pulselab"]):
                    try:
                        r3 = requests.get(s2, headers={"User-Agent": HTTP_HEADERS["User-Agent"], "Referer": next_url}, impersonate="chrome110", timeout=10)
                        clean_r3 = r3.text.replace(r"\/", "/")
                        m_m3u8 = re.search(r'(https?://[^\'\" >]+\.m3u8[^\'\" >]*)', clean_r3)
                        if m_m3u8:
                            return m_m3u8.group(1), s2
                    except Exception as e3:
                        print(f"Notice resolving embed player: {e3}")
                    return s2, next_url

            clean_r2 = r2.text.replace(r"\/", "/")
            m_media = re.search(r'https?://player\.mediadelivery\.net/embed/[^\'\" >]+', clean_r2)
            if m_media: return m_media.group(0), next_url
            m_vim = re.search(r'https?://player\.vimeo\.com/video/\d+(?:\?[^\'\"\s<>]+)?', clean_r2)
            if m_vim: return m_vim.group(0), next_url
            m_m3u8_r2 = re.search(r'(https?://[^\'\" >]+\.m3u8[^\'\" >]*)', clean_r2)
            if m_m3u8_r2: return m_m3u8_r2.group(1), next_url
    except Exception as e:
        print(f"Notice fetching embed page for {vid_id}: {e}")
    return None, None

def resolve_stream(episode_url: str):
    try:
        r = requests.get(episode_url, headers=HTTP_HEADERS, impersonate="chrome110", timeout=15)
        if r.status_code != 200:
            return None, None, f"HTTP {r.status_code}", None
        soup = BeautifulSoup(r.text, "html.parser")

        poster_url = None
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            poster_url = og_img["content"].strip()
        if not poster_url:
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src")
                if src and any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]) and not any(skip in src.lower() for skip in ["logo", "icon", "avatar", "badge"]):
                    poster_url = src.strip()
                    break

        for ifr in soup.find_all("iframe"):
            src = ifr.get("src") or ifr.get("data-src") or ""
            if any(v in src.lower() for v in ["mediadelivery", "dailymotion", "vimeo", "vk.com", "streamwish", "filelions"]):
                return src, episode_url, "iframe", poster_url
            if "video=" in src:
                vm = re.search(r'video=([a-zA-Z0-9\-]+)', src)
                if vm:
                    res, p_ref = resolve_video_id_stream(vm.group(1))
                    if res: return res, p_ref, "resolved_iframe", poster_url

        for a in soup.find_all("a", href=True):
            h = a["href"]
            if "video=" in h:
                vm = re.search(r'video=([a-zA-Z0-9\-]+)', h)
                if vm:
                    res, p_ref = resolve_video_id_stream(vm.group(1))
                    if res: return res, p_ref, "resolved_redirect", poster_url

        clean_text = r.text.replace(r"\/", "/")
        m_m3u8 = re.search(r'(https?://[^\'\" >]+\.m3u8[^\'\" >]*)', clean_text)
        if m_m3u8:
            return m_m3u8.group(1), episode_url, "html_direct_m3u8", poster_url

    except Exception as e:
        return None, None, str(e), None
    return None, None, "No video stream player found on page", None

def download_video(video_url: str, output_path: str, referer: str = None, progress_hook=None):
    import yt_dlp
    from yt_dlp.networking.impersonate import ImpersonateTarget

    stream_ref = referer or 'https://tamildhool.tech/'
    if "vimeo.com" in video_url:
        if not referer or "tamildhool" in referer:
            stream_ref = "https://teamstoday.com/"

    hdrs = dict(HTTP_HEADERS)
    hdrs['Referer'] = stream_ref

    hooks = [progress_hook] if progress_hook else []

    ydl_opts = {
        'format': 'best[height<=720]/bestvideo[height<=720]+bestaudio/best',
        'outtmpl': output_path,
        'paths': {
            'home': os.path.dirname(output_path),
            'temp': os.path.dirname(output_path),
        },
        'concurrent_fragment_downloads': 10,
        'buffersize': 1024 * 1024,
        'http_chunk_size': 5242880,
        'retries': 10,
        'fragment_retries': 10,
        'quiet': False,
        'no_warnings': True,
        'http_headers': hdrs,
        'referer': stream_ref,
        'impersonate': ImpersonateTarget.from_str('chrome'),
        'progress_hooks': hooks
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        if os.path.exists(output_path) and os.path.getsize(output_path) > 1024 * 1024:
            return True, None
        return False, "Downloaded video file is empty"
    except Exception as e:
        return False, str(e)

def extract_metadata_and_thumb(video_path: str, thumb_out_path: str, poster_url: str = None):
    duration = 0
    width = 1280
    height = 720
    raw_poster_tmp = thumb_out_path + ".raw.jpg"

    try:
        cmd_probe = [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration:format=duration",
            "-of", "json", video_path
        ]
        res = subprocess.run(cmd_probe, capture_output=True, text=True, timeout=10)
        if res.returncode == 0 and res.stdout:
            data = json.loads(res.stdout)
            streams = data.get("streams", [])
            if streams:
                s0 = streams[0]
                width = int(s0.get("width") or 1280)
                height = int(s0.get("height") or 720)
                if s0.get("duration"):
                    duration = int(float(s0["duration"]))
            if not duration:
                fmt_dur = data.get("format", {}).get("duration")
                if fmt_dur: duration = int(float(fmt_dur))
    except Exception as e:
        print(f"Notice ffprobe: {e}")

    if duration <= 0 or not width or not height:
        try:
            res_ff = subprocess.run(["ffmpeg", "-i", video_path], capture_output=True, text=True, timeout=10)
            ff_out = (res_ff.stderr or "") + (res_ff.stdout or "")
            m_dur = re.search(r'Duration:\s*(\d+):(\d+):(\d+\.?\d*)', ff_out)
            if m_dur:
                h, mn, s = int(m_dur.group(1)), int(m_dur.group(2)), float(m_dur.group(3))
                duration = int(h * 3600 + mn * 60 + s)
            m_res = re.search(r',\s*(\d{3,4})x(\d{3,4})\b', ff_out)
            if m_res:
                width = int(m_res.group(1))
                height = int(m_res.group(2))
        except Exception as e:
            print(f"Notice ffmpeg metadata fallback: {e}")

    thumb_generated = False
    if poster_url:
        try:
            r_img = requests.get(poster_url, headers=HTTP_HEADERS, timeout=10)
            if r_img.status_code == 200 and len(r_img.content) > 3000:
                with open(raw_poster_tmp, "wb") as f:
                    f.write(r_img.content)
                cmd_convert = ["ffmpeg", "-y", "-i", raw_poster_tmp, "-vf", "scale=320:-1", "-q:v", "2", "-frames:v", "1", thumb_out_path]
                res_c = subprocess.run(cmd_convert, capture_output=True, timeout=10)
                if res_c.returncode == 0 and os.path.exists(thumb_out_path) and os.path.getsize(thumb_out_path) > 500:
                    thumb_generated = True
        except Exception as e:
            print(f"Notice downloading poster: {e}")
        finally:
            if os.path.exists(raw_poster_tmp):
                try: os.remove(raw_poster_tmp)
                except Exception: pass

    if not thumb_generated or not os.path.exists(thumb_out_path) or os.path.getsize(thumb_out_path) < 500:
        try:
            seek_sec = str(min(45, max(5, int(duration * 0.1)))) if duration > 10 else "5"
            cmd_thumb = ["ffmpeg", "-y", "-ss", seek_sec, "-i", video_path, "-vf", "scale=320:-1", "-q:v", "2", "-frames:v", "1", thumb_out_path]
            res_t = subprocess.run(cmd_thumb, capture_output=True, timeout=10)
            if res_t.returncode == 0 and os.path.exists(thumb_out_path) and os.path.getsize(thumb_out_path) > 500:
                thumb_generated = True
        except Exception as e:
            print(f"Notice ffmpeg video frame: {e}")

    has_thumb = thumb_generated and os.path.exists(thumb_out_path) and (0 < os.path.getsize(thumb_out_path) < 200000)
    final_thumb = thumb_out_path if has_thumb else None
    return final_thumb, duration, width, height

async def process_single_serial(app: Client, db, info: dict, loop):
    url = info["url"]
    existing = await db["cph_serials_processed"].find_one({"_id": url})
    if existing:
        print(f"⏩ [Already Processed] {info['serial_name']} ({info['date']}) is already published. Skipping.")
        return True

    status_chat = info.get("status_chat_id") or ADMIN_USER_ID
    reporter = TelegramLiveReporter(app, status_chat, loop)
    await reporter.init_message(
        info["serial_name"], info["network"], info["date"], info["target_channel"], info["day_label"]
    )

    print("\n" + "=" * 60)
    print(f"🎬 Processing:     {info['serial_name']}")
    print(f"📅 Date:           {info['date']}")
    print(f"📺 Target Channel: {info['target_channel']} ({info['day_label']})")
    print(f"🔗 Episode URL:    {url}")
    print("=" * 60)

    await reporter.update_stage("Resolving Stream with Chrome TLS Impersonation 🔍")
    stream_url, parent_ref, method, poster_url = resolve_stream(url)
    if not stream_url:
        print(f"❌ Failed to resolve video stream: {method}")
        await reporter.update_stage(f"Failed: Stream not found ({method}) ❌")
        return False
    print(f"✅ Stream resolved via {method}")

    work_dir = tempfile.mkdtemp(prefix="gh_serial_")
    clean_title = re.sub(r'[^a-zA-Z0-9_]', '_', info["serial_name"])
    video_path = os.path.join(work_dir, f"{DEFAULT_BRAND}_{clean_title}_{info['date']}.mp4")
    thumb_path = os.path.join(work_dir, "thumb.jpg")

    try:
        print(f"📥 Downloading episode video (10 Gbps cloud network)...")
        await reporter.update_stage("Downloading (10 Gbps Cloud Network ⚡)")
        t0 = time.time()
        ok, dl_err = await asyncio.to_thread(
            download_video, stream_url, video_path, parent_ref, reporter.on_download_progress
        )
        if not ok:
            print(f"❌ Download failed: {dl_err}")
            await reporter.update_stage(f"Download Error: {dl_err} ❌")
            return False
        dl_sec = time.time() - t0
        print(f"✅ Downloaded in {dl_sec:.1f}s! ({format_size(os.path.getsize(video_path))})")

        print("🖼️ Preparing duration and 320x180 thumbnail...")
        await reporter.update_stage("Generating 320x180 Poster Preview 🖼️")
        final_thumb, duration, width, height = await asyncio.to_thread(
            extract_metadata_and_thumb, video_path, thumb_path, poster_url
        )

        ep_num_m = re.search(r'\b(?:EP|Episode|Day)\s*[-:]?\s*(\d+)\b', info["post_title"], re.IGNORECASE)
        ep_line = f"🎞️ **Episode:** Episode {ep_num_m.group(1)}\n" if ep_num_m else ""
        title_header = f"📺 **{info['serial_name'].upper()} – EPISODE {ep_num_m.group(1)}**" if ep_num_m else f"📺 **{info['serial_name'].upper()} – {info['date']}**"
        
        dur_line = ""
        if duration > 0:
            mins = duration // 60
            secs = duration % 60
            dur_line = f"⏱️ **Duration:** {mins}:{secs:02d} Min\n"

        quality_label = f"{height}p HD" if height else "720p HD"
        size_str = format_size(os.path.getsize(video_path))

        caption = (
            f"{title_header}\n\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"{ep_line}"
            f"📅 **Date:** {info['date']}\n"
            f"{dur_line}"
            f"🎥 **Quality:** {quality_label}\n"
            f"🔊 **Audio:** Tamil\n"
            f"📦 **Size:** {size_str}\n\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ **Powered by {DEFAULT_BRAND} | {MAIN_SERIALS_USERNAME}**"
        )

        print(f"📤 Uploading to Telegram Target Channel ({info['target_channel']})...")
        await reporter.update_stage("Uploading to Telegram 📤")
        sent = await app.send_video(
            chat_id=info["target_channel"],
            video=video_path,
            caption=caption,
            thumb=final_thumb,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            progress=reporter.on_upload_progress
        )
        msg_id = sent.id
        print(f"✅ Successfully posted to Channel! Message ID: {msg_id}")

        try: await sent.copy(chat_id=DUMP_CHANNEL_ID, caption=caption)
        except Exception as e: print(f"Notice copying to dump: {e}")

        try: await sent.copy(chat_id=PUBLIC_CHANNEL_ID, caption=caption)
        except Exception as e: print(f"Notice copying to public: {e}")

        await db["cph_serials_processed"].insert_one({
            "_id": url,
            "title": f"{info['serial_name']} - {info['date']}",
            "channel_id": info["target_channel"],
            "message_id": msg_id,
            "posted_at": datetime.now(timezone.utc),
            "uploaded_by": "github-actions-10gbps"
        })
        print(f"💾 Synced completion to MongoDB!")
        await reporter.complete_card(f"{info['network']} TV ({info['day_label']})")
        return True
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

async def run_autonomous_scan(app: Client, db, loop, network_filter: str = None):
    print("\n🔍 [24/7 Auto-Scan] Checking TamilDhool for new episodes...")
    label, chans = get_current_rotation()
    today_dt = datetime.now(IST).date()
    all_episodes = []
    seen = set()

    pages = CATEGORY_PAGES
    if network_filter and network_filter.upper() in ["SUN", "VIJAY", "ZEE"]:
        pages = [p for p in CATEGORY_PAGES if p[0] == network_filter.upper()]

    for net, cat_url in pages:
        try:
            r = requests.get(cat_url, headers=HTTP_HEADERS, impersonate="chrome110", timeout=15)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                title = a.get_text(strip=True)
                if not title or len(title) < 5 or href in seen:
                    continue
                if any(x in href.lower() for x in ["/page/", "/category/", "/tag/", "promo"]):
                    continue
                
                s_name, s_date, s_tag = extract_episode_metadata(title, href)
                if not s_name or not s_date:
                    continue
                
                try:
                    d_obj = datetime.strptime(s_date, "%d-%m-%Y").date()
                    if abs((today_dt - d_obj).days) > 1:
                        continue
                except Exception:
                    continue

                seen.add(href)
                target_chan = chans.get(net, chans["SUN"])
                all_episodes.append({
                    "url": href,
                    "serial_name": s_name,
                    "date": s_date,
                    "network": net,
                    "target_channel": target_chan,
                    "day_label": label,
                    "ep_tag": s_tag,
                    "post_title": f"{s_name} {s_date}",
                    "status_chat_id": ADMIN_USER_ID
                })
        except Exception as e:
            print(f"Notice scanning {net}: {e}")

    print(f"📊 Discovered {len(all_episodes)} total recent episodes across scanned categories.")

    # Deduplicate against MongoDB to find only pending episodes
    unprocessed = []
    for ep in all_episodes:
        existing = await db["cph_serials_processed"].find_one({"_id": ep["url"]})
        if not existing:
            unprocessed.append(ep)

    if not unprocessed:
        print("🎉 All serials are already published and up to date!")
        return

    print(f"⚡ Launching PARALLEL 10 BATCH ({len(unprocessed)} episodes to process, 10 running simultaneously)...")
    sem = asyncio.Semaphore(10)

    async def bounded_worker(ep_info):
        async with sem:
            return await process_single_serial(app, db, ep_info, loop)

    results = await asyncio.gather(*[bounded_worker(ep) for ep in unprocessed], return_exceptions=True)
    uploaded_count = sum(1 for r in results if r is True)
    print(f"\n🎉 [Batch Completed] Successfully uploaded {uploaded_count}/{len(unprocessed)} episodes in parallel 10 batch!")

async def main():
    single_info = parse_input_payload()

    db_client = AsyncIOMotorClient(MONGO_URI)
    db = db_client["cineprimehub"]

    app = Client("github_serial_uploader", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    await app.start()
    loop = asyncio.get_running_loop()

    try:
        if single_info:
            print("🚀 Executing Single Episode Upload (Dispatched from EC2/Manual)...")
            await process_single_serial(app, db, single_info, loop)
        else:
            net_filter = os.getenv("MANUAL_NET")
            print(f"🤖 Executing Parallel 10 Autonomous Scan & Upload (Filter: {net_filter or 'ALL'})...")
            await run_autonomous_scan(app, db, loop, network_filter=net_filter)
    finally:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
