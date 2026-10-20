#!/usr/bin/env python3
"""
CinePrime Fully Autonomous Serial Uploader (GitHub Actions 10 Gbps Runner)
========================================================================
- Runs 24/7 on GitHub Cloud Runners (10 Gbps unmetered network)
- 100% Free - Consumes 0 MB of AWS EC2 Bandwidth
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

def format_size(size_bytes):
    if not size_bytes or size_bytes <= 0: return "0 MB"
    mb = size_bytes / (1024 * 1024)
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.1f} MB"

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
    if raw_payload:
        try:
            payload = json.loads(raw_payload)
        except Exception as e:
            print(f"Notice parsing PAYLOAD_JSON: {e}")

    url = payload.get("url") or os.getenv("MANUAL_URL")
    if not url:
        return None  # Triggers Auto-Scrape Mode

    serial_name = payload.get("serial_name") or os.getenv("MANUAL_NAME") or "Serial"
    date = payload.get("date") or os.getenv("MANUAL_DATE") or datetime.now(IST).strftime("%d-%m-%Y")
    network = payload.get("network") or os.getenv("MANUAL_NET") or "SUN"
    
    label, chans = get_current_rotation()
    target_channel = int(payload.get("target_channel") or os.getenv("MANUAL_CHAN") or chans.get(network, chans["SUN"]))
    day_label = payload.get("day_label") or os.getenv("MANUAL_DAY") or label
    ep_tag = payload.get("ep_tag") or ""
    post_title = payload.get("post_title") or f"{serial_name} {date}"

    return {
        "url": url,
        "serial_name": serial_name,
        "date": date,
        "network": network,
        "target_channel": target_channel,
        "day_label": day_label,
        "ep_tag": ep_tag,
        "post_title": post_title
    }

def resolve_video_id_stream(vid_id: str):
    tt_url = f"https://teamstoday.com/?video={vid_id}"
    headers_tt = {"User-Agent": HTTP_HEADERS["User-Agent"], "Referer": "https://www.tamildhool.tech/"}
    next_url = None
    try:
        r1 = requests.get(tt_url, headers=headers_tt, impersonate="chrome110", timeout=12)
        if r1.status_code == 200:
            m_redir = re.search(r'url=([^\'\" >]+)', r1.text) or re.search(r'href=[\'\"](https?://[^\'\">]+video=[^\'\">]+)', r1.text)
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
                if any(k in s2 for k in ["mediadelivery.net", "vimeo.com", "dailymotion.com", "streamwish", "filelions"]):
                    return s2, next_url
            m_media = re.search(r'https?://player\.mediadelivery\.net/embed/[^\'\" >]+', r2.text)
            if m_media: return m_media.group(0), next_url
            m_vim = re.search(r'https?://player\.vimeo\.com/video/\d+(?:\?[^\'\"\s<>]+)?', r2.text)
            if m_vim: return m_vim.group(0), next_url
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

    except Exception as e:
        return None, None, str(e), None
    return None, None, "No video stream player found on page", None

def download_video(video_url: str, output_path: str, referer: str = None):
    import yt_dlp
    from yt_dlp.networking.impersonate import ImpersonateTarget

    stream_ref = referer or 'https://tamildhool.tech/'
    if "vimeo.com" in video_url:
        if not referer or "tamildhool" in referer:
            stream_ref = "https://teamstoday.com/"

    hdrs = dict(HTTP_HEADERS)
    hdrs['Referer'] = stream_ref

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
        'impersonate': ImpersonateTarget.from_str('chrome')
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

async def process_single_serial(app: Client, db, info: dict):
    url = info["url"]
    existing = await db["cph_serials_processed"].find_one({"_id": url})
    if existing:
        print(f"⏩ [Already Processed] {info['serial_name']} ({info['date']}) is already published. Skipping.")
        return True

    print("\n" + "=" * 60)
    print(f"🎬 Processing:     {info['serial_name']}")
    print(f"📅 Date:           {info['date']}")
    print(f"📺 Target Channel: {info['target_channel']} ({info['day_label']})")
    print(f"🔗 Episode URL:    {url}")
    print("=" * 60)

    stream_url, parent_ref, method, poster_url = resolve_stream(url)
    if not stream_url:
        print(f"❌ Failed to resolve video stream: {method}")
        return False
    print(f"✅ Stream resolved via {method}")

    work_dir = tempfile.mkdtemp(prefix="gh_serial_")
    clean_title = re.sub(r'[^a-zA-Z0-9_]', '_', info["serial_name"])
    video_path = os.path.join(work_dir, f"{DEFAULT_BRAND}_{clean_title}_{info['date']}.mp4")
    thumb_path = os.path.join(work_dir, "thumb.jpg")

    try:
        print(f"📥 Downloading episode video (10 Gbps cloud network)...")
        t0 = time.time()
        ok, dl_err = download_video(stream_url, video_path, referer=parent_ref)
        if not ok:
            print(f"❌ Download failed: {dl_err}")
            return False
        dl_sec = time.time() - t0
        print(f"✅ Downloaded in {dl_sec:.1f}s! ({format_size(os.path.getsize(video_path))})")

        print("🖼️ Preparing duration and 320x180 thumbnail...")
        final_thumb, duration, width, height = extract_metadata_and_thumb(video_path, thumb_path, poster_url)

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
        sent = await app.send_video(
            chat_id=info["target_channel"],
            video=video_path,
            caption=caption,
            thumb=final_thumb,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True
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
        return True
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

async def run_autonomous_scan(app: Client, db):
    print("\n🔍 [24/7 Auto-Scan] Checking TamilDhool for new episodes...")
    label, chans = get_current_rotation()
    today_dt = datetime.now(IST).date()
    all_episodes = []
    seen = set()

    for net, cat_url in CATEGORY_PAGES:
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
                
                # Check date match (today or yesterday)
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
                    "post_title": f"{s_name} {s_date}"
                })
        except Exception as e:
            print(f"Notice scanning {net}: {e}")

    print(f"📊 Discovered {len(all_episodes)} total recent episodes across Sun, Vijay & Zee.")
    
    uploaded_count = 0
    for ep in all_episodes:
        ok = await process_single_serial(app, db, ep)
        if ok:
            uploaded_count += 1

    print(f"\n🎉 [Auto-Scan Completed] Uploaded {uploaded_count} episodes. Everything up-to-date!")

async def main():
    single_info = parse_input_payload()

    db_client = AsyncIOMotorClient(MONGO_URI)
    db = db_client["cineprimehub"]

    app = Client("github_serial_uploader", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    await app.start()

    try:
        if single_info:
            print("🚀 Executing Single Episode Upload (Dispatched from EC2/Manual)...")
            await process_single_serial(app, db, single_info)
        else:
            print("🤖 Executing Full Autonomous 24/7 Scan & Upload...")
            await run_autonomous_scan(app, db)
    finally:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
