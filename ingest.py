#!/usr/bin/env python3
"""
TikTok Video Ingestion Pipeline — Single-command, complete ingestion.

Usage:
    python ingest.py <tiktok_url_1> [tiktok_url_2] [...]

This script takes one or more TikTok URLs and, for each one, runs every
ingestion step in sequence with zero manual intervention:

    1. Scrape page metadata (creator, likes, comments, shares, caption, hashtags, post date)
    2. Download the video file
    3. Extract audio (WAV) from video
    4. Transcribe audio with OpenAI Whisper
    5. Extract frames at regular intervals
    6. Analyze frames visually with Claude API (shot types, on-screen text, CTAs, visual notes)
    7. Insert ALL data into Supabase across all 7 tables

If any step fails, the script reports the error and moves on to the next URL.
At the end, it prints a summary of what succeeded and what failed.

Environment variables required:
    ANTHROPIC_API_KEY   — for Claude visual analysis
    SUPABASE_URL        — your Supabase project URL
    SUPABASE_KEY        — your Supabase service role key
"""

import os
import sys
import json
import re
import base64
import subprocess
import tempfile
import hashlib
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MAX_FRAMES = 12          # frames to sample per video for visual analysis
FRAME_INTERVAL_FALLBACK = 3  # seconds between frames if duration unknown
WHISPER_MODEL = "base"   # whisper model size

# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------
def check_dependencies():
    """Verify all required tools and libraries are available."""
    errors = []

    # Python packages
    try:
        import yt_dlp
    except ImportError:
        errors.append("yt-dlp not installed. Run: pip install yt-dlp --break-system-packages")

    try:
        import whisper
    except ImportError:
        errors.append("openai-whisper not installed. Run: pip install openai-whisper --break-system-packages")

    try:
        import anthropic
    except ImportError:
        errors.append("anthropic not installed. Run: pip install anthropic --break-system-packages")

    # ffmpeg
    result = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True)
    if result.returncode != 0:
        errors.append("ffmpeg not found. Install ffmpeg.")

    # Environment variables
    if not os.environ.get("ANTHROPIC_API_KEY"):
        errors.append("ANTHROPIC_API_KEY environment variable not set.")
    if not os.environ.get("SUPABASE_URL"):
        errors.append("SUPABASE_URL environment variable not set.")
    if not os.environ.get("SUPABASE_KEY"):
        errors.append("SUPABASE_KEY environment variable not set.")

    if errors:
        print("Missing dependencies:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Step 1: Scrape metadata
# ---------------------------------------------------------------------------
def scrape_metadata(url):
    """Use yt-dlp to extract all available metadata from a TikTok URL."""
    print(f"  [1/7] Scraping metadata...")
    import yt_dlp

    with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True, "skip_download": True}) as ydl:
        info = ydl.extract_info(url, download=False)

    video_id = str(info.get("id", ""))
    description = info.get("description", "") or ""

    # Extract hashtags from description
    hashtags = re.findall(r"#(\w+)", description)

    # Parse upload date
    upload_date_raw = info.get("upload_date", "")
    post_date = None
    if upload_date_raw and len(upload_date_raw) == 8:
        post_date = f"{upload_date_raw[:4]}-{upload_date_raw[4:6]}-{upload_date_raw[6:8]}"

    metadata = {
        "video_id": video_id,
        "tiktok_url": url,
        "creator_username": info.get("uploader") or info.get("creator") or info.get("channel") or "unknown",
        "tiktok_handle": "@" + (info.get("uploader") or info.get("creator") or info.get("channel") or "unknown"),
        "caption": (info.get("title") or description or "")[:500],
        "hashtags": hashtags,
        "post_date": post_date,
        "likes": info.get("like_count"),
        "comments": info.get("comment_count"),
        "shares": info.get("repost_count"),
        "favorites": info.get("collect_count"),
        "duration_seconds": info.get("duration"),
        "audio_name": info.get("track") or info.get("music_info", {}).get("title") if isinstance(info.get("music_info"), dict) else None,
        "audio_artist": info.get("artist") or info.get("music_info", {}).get("author") if isinstance(info.get("music_info"), dict) else None,
    }

    print(f"         @{metadata['creator_username']} | {metadata['likes']} likes | {metadata['duration_seconds']}s")
    return metadata, info


# ---------------------------------------------------------------------------
# Step 2: Download video
# ---------------------------------------------------------------------------
def download_video(url, work_dir):
    """Download the video file using yt-dlp."""
    print(f"  [2/7] Downloading video...")
    import yt_dlp

    video_path = work_dir / "video.mp4"
    opts = {
        "quiet": True,
        "no_warnings": True,
        "outtmpl": str(video_path),
        "format": "best[ext=mp4]/best",
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])

    if not video_path.exists():
        # yt-dlp sometimes appends extension
        candidates = list(work_dir.glob("video.*"))
        if candidates:
            video_path = candidates[0]

    size_mb = video_path.stat().st_size / (1024 * 1024)
    print(f"         Downloaded: {size_mb:.1f} MB")
    return video_path


# ---------------------------------------------------------------------------
# Step 3: Extract audio
# ---------------------------------------------------------------------------
def extract_audio(video_path, work_dir):
    """Extract WAV audio from video using ffmpeg."""
    print(f"  [3/7] Extracting audio...")
    audio_path = work_dir / "audio.wav"
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(video_path), "-ar", "16000", "-ac", "1", str(audio_path)],
        capture_output=True, check=True,
    )
    print(f"         Audio extracted: {audio_path.name}")
    return audio_path


# ---------------------------------------------------------------------------
# Step 4: Transcribe
# ---------------------------------------------------------------------------
def transcribe_audio(audio_path):
    """Transcribe audio using OpenAI Whisper."""
    print(f"  [4/7] Transcribing with Whisper ({WHISPER_MODEL})...")
    import whisper

    model = whisper.load_model(WHISPER_MODEL)
    result = model.transcribe(str(audio_path))

    transcript = result.get("text", "").strip()
    segments = result.get("segments", [])

    # Build segments JSON for storage
    segment_data = []
    for seg in segments:
        segment_data.append({
            "start": round(seg["start"], 2),
            "end": round(seg["end"], 2),
            "text": seg["text"].strip(),
        })

    word_count = len(transcript.split())
    print(f"         Transcribed: {word_count} words")
    return transcript, segment_data


# ---------------------------------------------------------------------------
# Step 5: Extract frames
# ---------------------------------------------------------------------------
def extract_frames(video_path, work_dir, duration=None):
    """Extract evenly-spaced frames from the video."""
    print(f"  [5/7] Extracting frames...")
    frames_dir = work_dir / "frames"
    frames_dir.mkdir(exist_ok=True)

    if duration and duration > 0:
        # Calculate interval to get ~MAX_FRAMES frames
        interval = max(1, duration / MAX_FRAMES)
    else:
        interval = FRAME_INTERVAL_FALLBACK

    subprocess.run(
        ["ffmpeg", "-y", "-i", str(video_path), "-vf", f"fps=1/{interval}", str(frames_dir / "frame_%03d.jpg")],
        capture_output=True, check=True,
    )

    frame_files = sorted(frames_dir.glob("*.jpg"))
    print(f"         Extracted {len(frame_files)} frames")
    return frame_files


# ---------------------------------------------------------------------------
# Step 6: Visual analysis with Claude
# ---------------------------------------------------------------------------
ANALYSIS_PROMPT = """Analyze these video frames from a TikTok product video. The frames are in chronological order.

Transcript of this video:
{transcript}

For each frame, classify the shot type as one of:
- FACE (creator's face visible, talking head)
- PRODUCT_CLOSEUP (close-up of product)
- HANDS_ON_DEMO (hands using/demonstrating product)
- LIFESTYLE (lifestyle/context shot)
- TEXT_SCREEN (mostly text overlay)
- TRANSITION (transition/effect)
- BEFORE_AFTER (comparison shot)
- PACKAGING (product packaging/unboxing)
- SCREEN_RECORDING (phone/screen recording)

Also extract:
1. Any on-screen text visible in frames (with approximate timestamp in seconds)
2. Any call-to-action elements
3. Overall visual style and format notes
4. Product name and category if identifiable

Return a JSON object with this exact structure:
{{
  "product_name": "<product name if identifiable>",
  "product_category": "<category>",
  "content_type": "talking_head|product_demo|voiceover_broll|comparison|tutorial|montage|music_montage",
  "voice_delivery": "on_camera|voiceover|no_voice",
  "content_angle": "value_price|comparison|transformation|before_after|scarcity|tutorial|lifestyle|shock_value",
  "hook_format": "question|warning|bold_claim|before_after|curiosity|demonstration|shock_value|standard",
  "shot_breakdown": {{
    "total_frames": <number>,
    "face_frames": <number>,
    "broll_frames": <number>,
    "shot_counts": {{"FACE": N, "PRODUCT_CLOSEUP": N, ...}},
    "dominant_broll_type": "<most common non-face type or null>",
    "secondary_broll_type": "<second most common or null>",
    "shot_sequence": ["FACE", "PRODUCT_CLOSEUP", ...],
    "analysis_confidence": "high|medium|low",
    "notes": "<brief analysis note>"
  }},
  "onscreen_text": [
    {{
      "timestamp_seconds": <number>,
      "text_content": "<exact text>",
      "text_type": "hook_banner|product_name|price_tag|cta_overlay|educational_overlay|comparison_graphic|subtitle",
      "is_persistent": true/false
    }}
  ],
  "ctas": [
    {{
      "timestamp_seconds": <number>,
      "cta_text": "<CTA text>",
      "cta_type": "verbal|visual|link_in_bio|shopping_cart|swipe_up",
      "cta_position": "beginning|middle|end"
    }}
  ],
  "visual_notes": {{
    "format_type": "talking_head|product_demo|voiceover_broll|comparison|tutorial|montage",
    "hook_style": "question|warning|bold_claim|before_after|curiosity|demonstration|shock_value",
    "scene_descriptions": [
      {{"timestamp": <seconds>, "description": "<what happens>"}}
    ],
    "product_reveal_timestamp": <seconds or null>,
    "overall_notes": "<2-3 sentence summary of visual strategy>",
    "cross_reference_notes": "<how visuals relate to transcript>"
  }},
  "visual_script": "<Full visual script describing what happens visually from start to finish. 3-5 sentences.>"
}}

Return ONLY valid JSON, no markdown fences."""


def analyze_frames(frame_files, transcript, duration):
    """Send frames to Claude for visual analysis."""
    print(f"  [6/7] Analyzing frames with Claude...")
    import anthropic

    client = anthropic.Anthropic()

    # Sample down to MAX_FRAMES if needed
    frames = frame_files
    if len(frames) > MAX_FRAMES:
        step = len(frames) / MAX_FRAMES
        frames = [frames[int(i * step)] for i in range(MAX_FRAMES)]

    # Build message content
    content = [{"type": "text", "text": ANALYSIS_PROMPT.format(transcript=transcript[:2000])}]
    for f in frames:
        with open(f, "rb") as fh:
            data = base64.standard_b64encode(fh.read()).decode("utf-8")
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": data},
            })

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=3000,
        messages=[{"role": "user", "content": content}],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw[:-3]

    analysis = json.loads(raw)
    print(f"         Visual analysis complete: {len(frames)} frames analyzed")
    return analysis


# ---------------------------------------------------------------------------
# Step 7: Insert into Supabase
# ---------------------------------------------------------------------------
def insert_into_supabase(metadata, transcript, segments, analysis):
    """Insert all data into Supabase across all 7 tables."""
    print(f"  [7/7] Inserting into Supabase...")
    import urllib.request

    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_KEY"]

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    def api_post(table, data):
        url = f"{supabase_url}/rest/v1/{table}"
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())

    # --- Table 1: tiktok_videos ---
    video_row = {
        "video_id": metadata["video_id"],
        "tiktok_url": metadata["tiktok_url"],
        "creator_username": metadata["creator_username"],
        "tiktok_handle": metadata["tiktok_handle"],
        "caption": metadata["caption"],
        "hashtags": metadata["hashtags"],
        "post_date": metadata["post_date"],
        "likes": metadata["likes"],
        "comments": metadata["comments"],
        "shares": metadata["shares"],
        "favorites": metadata["favorites"],
        "duration_seconds": metadata["duration_seconds"],
        "audio_name": metadata.get("audio_name"),
        "audio_artist": metadata.get("audio_artist"),
        "product_name": analysis.get("product_name", metadata.get("product_name")),
        "product_category": analysis.get("product_category"),
        "content_type": analysis.get("content_type", "voiceover"),
        "voice_delivery": analysis.get("voice_delivery", "on_camera"),
        "content_angle": analysis.get("content_angle"),
        "hook_format": analysis.get("hook_format", "standard"),
    }
    result = api_post("tiktok_videos", video_row)
    uuid = result[0]["id"]
    print(f"         tiktok_videos: inserted (uuid={uuid[:8]}...)")

    # --- Table 2: video_transcripts ---
    api_post("video_transcripts", {
        "video_id": uuid,
        "full_transcript": transcript,
        "segments": segments,
        "transcript_type": "creator_speech",
    })
    print(f"         video_transcripts: inserted")

    # --- Table 3: video_shot_breakdown ---
    sb = analysis.get("shot_breakdown", {})
    api_post("video_shot_breakdown", {
        "video_id": uuid,
        "total_frames_analyzed": sb.get("total_frames", 0),
        "face_frames": sb.get("face_frames", 0),
        "broll_frames": sb.get("broll_frames", 0),
        "shot_counts": sb.get("shot_counts", {}),
        "dominant_broll_type": sb.get("dominant_broll_type"),
        "secondary_broll_type": sb.get("secondary_broll_type"),
        "shot_sequence": sb.get("shot_sequence", []),
        "analysis_confidence": sb.get("analysis_confidence"),
        "notes": sb.get("notes"),
    })
    print(f"         video_shot_breakdown: inserted")

    # --- Table 4: video_onscreen_text ---
    ost_count = 0
    for ost in analysis.get("onscreen_text", []):
        api_post("video_onscreen_text", {
            "video_id": uuid,
            "timestamp_seconds": ost.get("timestamp_seconds", 0),
            "text_content": ost.get("text_content", ""),
            "text_type": ost.get("text_type", "subtitle"),
            "is_persistent": ost.get("is_persistent", False),
        })
        ost_count += 1
    print(f"         video_onscreen_text: {ost_count} entries")

    # --- Table 5: video_ctas ---
    cta_count = 0
    for cta in analysis.get("ctas", []):
        api_post("video_ctas", {
            "video_id": uuid,
            "timestamp_seconds": cta.get("timestamp_seconds", 0),
            "cta_text": cta.get("cta_text", ""),
            "cta_type": cta.get("cta_type", "verbal"),
            "cta_position": cta.get("cta_position", "end"),
        })
        cta_count += 1
    print(f"         video_ctas: {cta_count} entries")

    # --- Table 6: video_visual_notes ---
    vn = analysis.get("visual_notes", {})
    api_post("video_visual_notes", {
        "video_id": uuid,
        "format_type": vn.get("format_type"),
        "hook_style": vn.get("hook_style"),
        "scene_descriptions": vn.get("scene_descriptions", []),
        "product_reveal_timestamp": vn.get("product_reveal_timestamp"),
        "overall_notes": vn.get("overall_notes"),
        "cross_reference_notes": vn.get("cross_reference_notes"),
    })
    print(f"         video_visual_notes: inserted")

    # --- Table 7: video_visual_scripts ---
    api_post("video_visual_scripts", {
        "video_id": uuid,
        "full_visual_script": analysis.get("visual_script", ""),
        "is_primary_script": True,
    })
    print(f"         video_visual_scripts: inserted")

    return uuid


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def ingest_video(url, work_dir):
    """Run the complete ingestion pipeline for a single TikTok URL."""
    print(f"\n{'='*60}")
    print(f"Ingesting: {url}")
    print(f"{'='*60}")

    # Step 1: Scrape metadata
    metadata, info = scrape_metadata(url)

    # Step 2: Download video
    video_path = download_video(url, work_dir)

    # Step 3: Extract audio
    audio_path = extract_audio(video_path, work_dir)

    # Step 4: Transcribe
    transcript, segments = transcribe_audio(audio_path)

    # Step 5: Extract frames
    frame_files = extract_frames(video_path, work_dir, metadata.get("duration_seconds"))

    # Step 6: Visual analysis
    analysis = analyze_frames(frame_files, transcript, metadata.get("duration_seconds"))

    # Step 7: Insert into Supabase
    uuid = insert_into_supabase(metadata, transcript, segments, analysis)

    print(f"\n  COMPLETE: {metadata['video_id']} by @{metadata['creator_username']}")
    print(f"  UUID: {uuid}")
    print(f"  All 7 tables populated.\n")

    return {
        "video_id": metadata["video_id"],
        "uuid": uuid,
        "creator": metadata["creator_username"],
        "likes": metadata["likes"],
        "status": "success",
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python ingest.py <tiktok_url_1> [tiktok_url_2] [...]")
        print("\nExample:")
        print("  python ingest.py https://www.tiktok.com/@creator/video/1234567890")
        print("\nRequired environment variables:")
        print("  ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_KEY")
        sys.exit(1)

    check_dependencies()

    urls = sys.argv[1:]
    results = []

    for url in urls:
        work_dir = Path(tempfile.mkdtemp(prefix="tiktok_ingest_"))
        try:
            result = ingest_video(url, work_dir)
            results.append(result)
        except Exception as e:
            print(f"\n  FAILED: {url}")
            print(f"  Error: {e}")
            results.append({"url": url, "status": "failed", "error": str(e)})

    # Summary
    print(f"\n{'='*60}")
    print(f"INGESTION SUMMARY")
    print(f"{'='*60}")
    success = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    print(f"  Succeeded: {len(success)}/{len(results)}")
    for r in success:
        print(f"    - {r['video_id']} by @{r['creator']} ({r['likes']} likes)")
    if failed:
        print(f"  Failed: {len(failed)}/{len(results)}")
        for r in failed:
            print(f"    - {r['url']}: {r['error']}")


if __name__ == "__main__":
    main()
