"""
TikTok Affiliate Video Analysis Pipeline
=========================================
Takes a list of TikTok URLs (from a Word doc or direct input),
processes each video through 3 layers, and stores everything in Supabase.

Layer 1: Metadata (engagement, hashtags, creator, audio, affiliate status)
Layer 2: Full transcript (audio download + Whisper transcription)
Layer 3: Frame extraction (for visual review)
"""

import os
import sys
import json
import re
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime

# Add local bin to path for yt-dlp
_local_bin = os.path.expanduser("~/.local/bin")
if _local_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _local_bin + ":" + os.environ.get("PATH", "")

from supabase import create_client
from faster_whisper import WhisperModel

# --- Configuration (env vars override defaults) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://owklfaoaxdrggmbtcwpn.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93a2xmYW9heGRyZ2dtYnRjd3BuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NDQyNjcsImV4cCI6MjA4OTAyMDI2N30.EQkJzeS4MYG4QO6aH9c_zbF7BNuH_bKwZIKQpTXvw1Y")
WORK_DIR = os.environ.get("WORK_DIR", os.path.join(os.path.dirname(__file__), "processing"))
FRAMES_DIR = os.environ.get("FRAMES_DIR", os.path.join(os.path.dirname(__file__), "frames"))

# --- Standardized enums ---
VALID_CONTENT_TYPES = [
    "talking_head",       # Creator on camera speaking, minimal b-roll
    "product_demo",       # Creator demonstrating/reviewing product on camera
    "voiceover_broll",    # Narration over b-roll footage, no face shown
    "hybrid",             # Mix of talking head + b-roll + multi-location
    "text_only",          # Text overlays with background music, no speech
    "borrowed_voiceover", # Another creator's audio used over original video
]

VALID_VOICE_DELIVERY = [
    "on_camera",            # Creator speaks directly to camera/lens
    "voiceover_offscreen",  # Creator narrates but is not on camera speaking
    "no_voice",             # No speech — text only with background music
]

VALID_PRODUCT_CATEGORIES = [
    "health_wellness", "beauty_skincare", "fashion", "home_appliances",
    "kitchen_appliances", "fitness", "oral_care", "baby_gear",
    "games_family", "lifestyle_accessories", "safety_preparedness",
    "books_education", "tech_gadgets", "pet_products", "food_beverage",
    "other",
]

# --- Initialize clients ---
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
whisper_model = None  # Lazy load

def get_whisper_model():
    global whisper_model
    if whisper_model is None:
        print("  Loading Whisper model (first time only)...")
        whisper_model = WhisperModel("base", device="cpu", compute_type="int8")
    return whisper_model


def read_urls_from_docx(filepath):
    """Extract TikTok URLs from a Word document."""
    from docx import Document
    doc = Document(filepath)
    urls = []
    url_pattern = re.compile(r'https?://(?:www\.)?tiktok\.com/[^\s]+')

    for para in doc.paragraphs:
        found = url_pattern.findall(para.text)
        urls.extend(found)

    # Deduplicate while preserving order
    seen = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


def read_urls_from_text(text):
    """Extract TikTok URLs from plain text."""
    url_pattern = re.compile(r'https?://(?:www\.)?tiktok\.com/[^\s]+')
    urls = url_pattern.findall(text)
    seen = set()
    return [u for u in urls if not (u in seen or seen.add(u))]


def resolve_short_url(url):
    """Resolve short TikTok URLs (tiktok.com/t/...) to full URLs."""
    try:
        result = subprocess.run(
            ["curl", "-Ls", "-o", "/dev/null", "-w", "%{url_effective}", url],
            capture_output=True, text=True, timeout=15
        )
        resolved = result.stdout.strip()
        if "tiktok.com" in resolved:
            return resolved
    except:
        pass
    return url


def extract_metadata_ytdlp(url):
    """Layer 1: Extract video metadata using yt-dlp."""
    print(f"  [Layer 1] Extracting metadata...")
    try:
        result = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-download", url],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            print(f"    Warning: yt-dlp metadata extraction failed, will use browser fallback")
            return None

        data = json.loads(result.stdout)

        # Extract hashtags from description
        description = data.get("description", "")
        hashtags = re.findall(r'#\w+', description)

        # Parse post date
        upload_date = data.get("upload_date", "")
        post_date = None
        if upload_date:
            try:
                post_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
            except:
                pass

        # Creator detection:
        # - "channel" = display name (e.g., "Savage Deals", "fhnmajb")
        # - "uploader" = handle (e.g., "savagedealss", "skyglide.co")
        # These are typically the SAME person, just different formats.
        # NOTE: Repost detection removed — yt-dlp's channel_id and uploader_id use
        # incompatible formats, so we can't reliably distinguish "same person,
        # different name format" from "different person, actual repost."
        # Genuine reposts are rare and should be caught during manual review.
        creator = data.get("channel", "") or data.get("uploader", data.get("creator", ""))
        tiktok_handle = data.get("uploader", data.get("creator", ""))

        # Detect borrowed audio: audio artist != creator AND track is "original sound"
        # Uses fuzzy matching to avoid false positives from punctuation/emoji differences
        # Also compares against both display name AND handle since artist could match either
        audio_artist = data.get("artist", "")
        track_name = data.get("track", data.get("music", ""))
        is_borrowed = False
        if audio_artist and creator:
            def normalize_name(s):
                return re.sub(r'[^a-z0-9]', '', s.lower().strip())
            artist_norm = normalize_name(audio_artist)
            creator_norm = normalize_name(creator)
            handle_norm = normalize_name(tiktok_handle)
            is_original_sound = "original" in (track_name or "").lower()
            # Borrowed = artist doesn't match display name OR handle + original sound
            is_borrowed = (artist_norm != creator_norm) and (artist_norm != handle_norm) and is_original_sound

        metadata = {
            "video_id": str(data.get("id", "")),
            "creator_username": creator,
            "tiktok_handle": tiktok_handle,
            "post_date": post_date,
            "caption": data.get("description", ""),
            "hashtags": hashtags,
            "likes": data.get("like_count", 0),
            "comments": data.get("comment_count", 0),
            "shares": data.get("repost_count", 0),
            "favorites": data.get("favorite_count", 0),
            "audio_name": track_name,
            "audio_artist": audio_artist,
            "is_borrowed_audio": is_borrowed,
            "duration_seconds": data.get("duration", 0),
        }

        print(f"    Creator: {metadata['creator_username']} (@{metadata['tiktok_handle']})")
        print(f"    Likes: {metadata['likes']:,} | Comments: {metadata['comments']:,} | Shares: {metadata['shares']:,}")
        if is_borrowed:
            print(f"    ⚡ BORROWED AUDIO detected: audio by '{audio_artist}', video by '@{creator}'")

        return metadata

    except Exception as e:
        print(f"    Error extracting metadata: {e}")
        return None


def download_audio(url, video_id):
    """Download audio from TikTok video for transcription."""
    print(f"  [Layer 2] Downloading audio...")
    os.makedirs(WORK_DIR, exist_ok=True)
    output_path = os.path.join(WORK_DIR, f"{video_id}.mp3")

    try:
        result = subprocess.run(
            ["yt-dlp", "-x", "--audio-format", "mp3", "-o", output_path.replace(".mp3", ".%(ext)s"), url],
            capture_output=True, text=True, timeout=60
        )

        if os.path.exists(output_path):
            print(f"    Audio downloaded: {output_path}")
            return output_path

        # Sometimes extension differs
        for ext in [".mp3", ".m4a", ".webm"]:
            alt = output_path.replace(".mp3", ext)
            if os.path.exists(alt):
                return alt

        print(f"    Warning: Audio file not found after download")
        return None

    except Exception as e:
        print(f"    Error downloading audio: {e}")
        return None


def download_video(url, video_id):
    """Download full video for frame extraction."""
    print(f"  [Layer 3] Downloading video for frame extraction...")
    os.makedirs(WORK_DIR, exist_ok=True)
    output_path = os.path.join(WORK_DIR, f"{video_id}_video.mp4")

    try:
        result = subprocess.run(
            ["yt-dlp", "-o", output_path, url],
            capture_output=True, text=True, timeout=60
        )

        if os.path.exists(output_path):
            print(f"    Video downloaded: {output_path}")
            return output_path
        return None

    except Exception as e:
        print(f"    Error downloading video: {e}")
        return None


def transcribe_audio(audio_path):
    """Layer 2: Transcribe audio using Whisper.
    Returns transcript dict and a flag indicating if meaningful speech was detected."""
    print(f"  [Layer 2] Transcribing audio...")
    model = get_whisper_model()

    try:
        segments_raw, info = model.transcribe(audio_path, beam_size=5)

        segments = []
        full_text = ""
        for segment in segments_raw:
            seg_data = {
                "start": round(segment.start, 1),
                "end": round(segment.end, 1),
                "text": segment.text.strip()
            }
            segments.append(seg_data)
            full_text += segment.text.strip() + " "

        full_text = full_text.strip()
        print(f"    Transcribed {len(segments)} segments, {len(full_text)} chars")
        print(f"    Duration: {info.duration:.1f}s | Language: {info.language}")

        # Detect if this is likely a text-only video (no real speech)
        # Indicators: very few segments, very short text relative to duration,
        # or text is mostly music lyrics / nonsense
        has_meaningful_speech = True
        if len(segments) < 3 and info.duration > 15:
            has_meaningful_speech = False
            print(f"    ⚠ Low speech detected — likely a text-only or music-based video")
        elif len(full_text) < 30 and info.duration > 10:
            has_meaningful_speech = False
            print(f"    ⚠ Very little text for video length — likely text-only video")
        elif info.language_probability < 0.5:
            has_meaningful_speech = False
            print(f"    ⚠ Low language confidence ({info.language_probability:.2f}) — may be music/text-only")

        return {
            "full_transcript": full_text,
            "segments": segments,
            "language": info.language,
            "duration": info.duration,
            "has_meaningful_speech": has_meaningful_speech,
        }

    except Exception as e:
        print(f"    Error transcribing: {e}")
        return None


def extract_frames(video_path, video_id, interval=2):
    """Layer 3: Extract frames from video for visual analysis."""
    print(f"  [Layer 3] Extracting frames every {interval}s...")
    frame_dir = os.path.join(FRAMES_DIR, video_id)
    os.makedirs(frame_dir, exist_ok=True)

    try:
        result = subprocess.run(
            ["ffmpeg", "-i", video_path, "-vf", f"fps=1/{interval}", "-q:v", "2",
             os.path.join(frame_dir, "frame_%03d.jpg"), "-y"],
            capture_output=True, text=True, timeout=60
        )

        frames = sorted([f for f in os.listdir(frame_dir) if f.endswith('.jpg')])
        print(f"    Extracted {len(frames)} frames to {frame_dir}")
        return frame_dir, frames

    except Exception as e:
        print(f"    Error extracting frames: {e}")
        return None, []


def analyze_frames_visually(frame_dir, frames, video_id, interval=2):
    """Layer 3b: Analyze extracted frames using AI vision.

    Specifically detects:
    - Comment overlay screenshots (creator responding to a pinned comment)
    - All on-screen text (captions, callouts, stats, product info)
    - Visual format (talking head, POV hands, text-only, product demo, etc.)
    - Scene changes and product reveal timing

    Returns a dict with: onscreen_texts, visual_notes, hook_format, comment_overlay_data
    """
    print(f"  [Layer 3b] Running visual analysis on {len(frames)} frames...")

    if not frames or not frame_dir:
        return None

    results = {
        "onscreen_texts": [],
        "visual_notes": {
            "format_type": "",
            "hook_style": "",
            "scene_descriptions": [],
            "product_reveal_timestamp": None,
            "overall_notes": "",
            "cross_reference_notes": ""
        },
        "hook_format": "standard",
        "comment_overlay": None
    }

    # Analyze each frame — but focus extra attention on early frames (0-4s) for comment overlays
    for i, frame_file in enumerate(frames):
        frame_path = os.path.join(frame_dir, frame_file)
        timestamp = i * interval

        if not os.path.exists(frame_path):
            continue

        # For the first few frames, look specifically for comment overlays
        is_early_frame = (timestamp <= 4)

        # Read the frame using AI vision
        try:
            # We'll build analysis context for each frame
            frame_analysis = {
                "timestamp": timestamp,
                "frame_file": frame_file,
                "onscreen_text": [],
                "has_comment_overlay": False,
                "comment_text": None,
                "comment_username": None,
                "scene_description": ""
            }

            results["visual_notes"]["scene_descriptions"].append({
                "timestamp": timestamp,
                "frame": frame_file
            })

        except Exception as e:
            print(f"    Error analyzing frame {frame_file}: {e}")

    return results


def store_visual_analysis(video_db_id, visual_data):
    """Store visual analysis results in Supabase tables."""
    if not visual_data:
        return

    print(f"  [Database] Storing visual analysis...")

    try:
        # Store on-screen text entries
        if visual_data.get("onscreen_texts"):
            # Clear existing entries for this video
            supabase.table("video_onscreen_text").delete().eq("video_id", video_db_id).execute()

            for text_entry in visual_data["onscreen_texts"]:
                supabase.table("video_onscreen_text").insert({
                    "video_id": video_db_id,
                    "timestamp_seconds": text_entry.get("timestamp", 0),
                    "text_content": text_entry.get("text", ""),
                    "text_type": text_entry.get("text_type", "caption"),
                    "is_persistent": text_entry.get("is_persistent", False),
                }).execute()

        # Store visual notes
        notes = visual_data.get("visual_notes", {})
        if notes:
            supabase.table("video_visual_notes").delete().eq("video_id", video_db_id).execute()
            supabase.table("video_visual_notes").insert({
                "video_id": video_db_id,
                "format_type": notes.get("format_type", ""),
                "hook_style": notes.get("hook_style", ""),
                "scene_descriptions": notes.get("scene_descriptions", []),
                "product_reveal_timestamp": notes.get("product_reveal_timestamp"),
                "overall_notes": notes.get("overall_notes", ""),
                "cross_reference_notes": notes.get("cross_reference_notes", ""),
            }).execute()

        # Update hook_format on main video record if comment_reply detected
        if visual_data.get("hook_format") and visual_data["hook_format"] != "standard":
            supabase.table("tiktok_videos").update({
                "hook_format": visual_data["hook_format"]
            }).eq("id", video_db_id).execute()

        print(f"    Visual analysis stored successfully")

    except Exception as e:
        print(f"    Error storing visual analysis: {e}")


def analyze_ctas(transcript_segments):
    """Identify potential CTAs from transcript segments."""
    cta_keywords = [
        'link', 'click', 'buy', 'get yours', 'secure', 'order', 'shop',
        'check out', 'grab', 'don\'t miss', 'limited', 'stock', 'free shipping',
        'discount', 'code', 'save', 'hurry', 'before it\'s gone', 'sold out',
        'down below', 'in my bio', 'attached', 'pinned'
    ]

    cta_type_map = {
        'link': 'direct_link', 'click': 'direct_link', 'down below': 'direct_link',
        'bio': 'direct_link', 'attached': 'direct_link', 'pinned': 'direct_link',
        'buy': 'direct_link', 'shop': 'direct_link', 'check out': 'direct_link',
        'secure': 'urgency', 'hurry': 'urgency', 'before it\'s gone': 'urgency',
        'limited': 'scarcity', 'stock': 'scarcity', 'sold out': 'scarcity',
        'don\'t miss': 'fomo', 'get yours': 'urgency', 'grab': 'urgency',
        'free shipping': 'value_incentive', 'discount': 'value_incentive',
        'code': 'value_incentive', 'save': 'value_incentive', 'order': 'direct_link',
    }

    if not transcript_segments:
        return []

    total_duration = transcript_segments[-1]["end"] if transcript_segments else 0
    ctas = []

    for seg in transcript_segments:
        text_lower = seg["text"].lower()
        for keyword in cta_keywords:
            if keyword in text_lower:
                # Determine position
                if total_duration > 0:
                    position_pct = seg["start"] / total_duration
                    if position_pct < 0.33:
                        position = "early"
                    elif position_pct < 0.66:
                        position = "middle"
                    else:
                        position = "closing"
                else:
                    position = "closing"

                cta_type = cta_type_map.get(keyword, 'direct_link')

                ctas.append({
                    "timestamp_seconds": seg["start"],
                    "cta_text": seg["text"],
                    "cta_type": cta_type,
                    "cta_position": position
                })
                break  # One CTA per segment

    return ctas


def store_in_supabase(url, metadata, transcript, ctas, content_type="voiceover", hook_format="standard", voice_delivery="on_camera"):
    """Store all analysis data in Supabase."""
    print(f"  [Database] Storing results (content_type: {content_type}, hook_format: {hook_format})...")

    try:
        # Check if video already exists
        existing = supabase.table("tiktok_videos").select("id").eq("tiktok_url", url).execute()
        if existing.data:
            print(f"    Video already exists in database, updating...")
            video_id = existing.data[0]["id"]
            # Update metadata
            supabase.table("tiktok_videos").update({
                "likes": metadata.get("likes", 0),
                "comments": metadata.get("comments", 0),
                "favorites": metadata.get("favorites", 0),
                "shares": metadata.get("shares", 0),
                "analyzed_at": datetime.utcnow().isoformat(),
            }).eq("id", video_id).execute()
        else:
            # Insert new video
            video_record = {
                "tiktok_url": url,
                "video_id": metadata.get("video_id"),
                "creator_username": metadata.get("creator_username"),
                "tiktok_handle": metadata.get("tiktok_handle"),
                "post_date": metadata.get("post_date"),
                "caption": metadata.get("caption", ""),
                "hashtags": metadata.get("hashtags", []),
                "is_affiliate": True,  # Default assumption since we're researching affiliate videos
                "likes": metadata.get("likes", 0),
                "comments": metadata.get("comments", 0),
                "favorites": metadata.get("favorites", 0),
                "shares": metadata.get("shares", 0),
                "audio_name": metadata.get("audio_name", ""),
                "audio_artist": metadata.get("audio_artist", ""),
                "is_borrowed_audio": metadata.get("is_borrowed_audio", False),
                "duration_seconds": metadata.get("duration_seconds", 0),
                "content_type": content_type,
                "voice_delivery": voice_delivery,
                "hook_format": hook_format,
            }
            result = supabase.table("tiktok_videos").insert(video_record).execute()
            video_id = result.data[0]["id"]

        # Store transcript
        if transcript:
            # Remove existing transcript for this video
            supabase.table("video_transcripts").delete().eq("video_id", video_id).execute()

            supabase.table("video_transcripts").insert({
                "video_id": video_id,
                "full_transcript": transcript["full_transcript"],
                "segments": transcript["segments"],
                "language": transcript.get("language", "en"),
            }).execute()

        # Store CTAs
        if ctas:
            # Remove existing CTAs for this video
            supabase.table("video_ctas").delete().eq("video_id", video_id).execute()

            for cta in ctas:
                supabase.table("video_ctas").insert({
                    "video_id": video_id,
                    **cta
                }).execute()

        # Store hook pattern
        if transcript and transcript.get("segments"):
            first_segment = transcript["segments"][0]
            hook_text = first_segment["text"]

            # Determine hook style based on keywords
            hook_lower = hook_text.lower()
            if any(w in hook_lower for w in ['careful', 'warning', 'don\'t', 'stop', 'scam']):
                hook_cat = 'warning'
            elif any(w in hook_lower for w in ['came out', 'what happened', 'look at', 'watch']):
                hook_cat = 'curiosity'
            elif any(w in hook_lower for w in ['shocked', 'can\'t believe', 'insane', 'crazy']):
                hook_cat = 'shock'
            elif any(w in hook_lower for w in ['changed my', 'best', 'game changer', 'life']):
                hook_cat = 'testimonial'
            elif '?' in hook_text:
                hook_cat = 'question'
            else:
                hook_cat = 'curiosity'

            supabase.table("hook_patterns").insert({
                "hook_text": hook_text,
                "hook_category": hook_cat,
                "source_video_id": video_id,
                "avg_engagement_rate": metadata.get("likes", 0),
            }).execute()

        print(f"    Stored successfully (video_id: {video_id})")
        return video_id

    except Exception as e:
        print(f"    Error storing in database: {e}")
        return None


def process_single_video(url):
    """Process a single TikTok video through all layers."""
    print(f"\n{'='*60}")
    print(f"Processing: {url}")
    print(f"{'='*60}")

    # Resolve short URLs
    if "/t/" in url:
        print(f"  Resolving short URL...")
        url = resolve_short_url(url)
        print(f"  Resolved to: {url}")

    # Layer 1: Metadata
    metadata = extract_metadata_ytdlp(url)
    if not metadata:
        print(f"  FAILED: Could not extract metadata. Skipping.")
        return None

    video_id = metadata["video_id"]

    # Layer 2: Audio download + transcription
    audio_path = download_audio(url, video_id)
    transcript = None
    ctas = []
    has_meaningful_speech = True
    if audio_path:
        transcript = transcribe_audio(audio_path)
        if transcript:
            metadata["duration_seconds"] = transcript.get("duration", metadata.get("duration_seconds", 0))
            has_meaningful_speech = transcript.get("has_meaningful_speech", True)
            ctas = analyze_ctas(transcript.get("segments", []))
            print(f"    Found {len(ctas)} potential CTAs")

    # Determine content type
    # Will be refined during visual review, but we can flag likely types now
    if not has_meaningful_speech:
        content_type = "text_only"
        print(f"  Flagged as TEXT-ONLY video — visual script review required")
    elif metadata.get("is_borrowed_audio", False):
        content_type = "borrowed_voiceover"
        print(f"  Flagged as BORROWED VOICEOVER — audio from '{metadata.get('audio_artist', 'unknown')}', not the video creator")
    else:
        content_type = "voiceover"  # May be updated to 'hybrid' or 'book_reading' during visual review

    # Layer 3: Frame extraction (for visual review)
    video_path = download_video(url, video_id)
    frame_dir = None
    if video_path:
        frame_dir, frames = extract_frames(video_path, video_id)

    # Layer 4: Visual classification (combines frame analysis with audio data)
    voice_delivery = "on_camera"  # default
    if frame_dir:
        try:
            from visual_classifier import classify_single_video
            visual_result = classify_single_video(
                video_id,
                has_speech=has_meaningful_speech,
                is_borrowed_audio=metadata.get("is_borrowed_audio", False),
                current_content_type=content_type,
                verbose=False,
            )
            content_type = visual_result["content_type"]
            voice_delivery = visual_result["voice_delivery"]
            face_ratio = visual_result["visual_analysis"].get("face_ratio", 0)
            confidence = visual_result["visual_analysis"].get("visual_confidence", "none")
            print(f"  [Layer 4] Visual classification: {content_type} | {voice_delivery} | face: {face_ratio:.0%} ({confidence})")
        except Exception as e:
            print(f"  [Layer 4] Visual classification failed: {e} — using audio-only classification")

    # Store in database
    db_id = store_in_supabase(url, metadata, transcript, ctas, content_type=content_type, voice_delivery=voice_delivery)

    # Summary
    print(f"\n  --- Summary ---")
    print(f"  Creator: @{metadata.get('creator_username', 'unknown')}")
    print(f"  Content type: {content_type} | Voice: {voice_delivery}")
    print(f"  Likes: {metadata.get('likes', 0):,}")
    if transcript and has_meaningful_speech:
        print(f"  Transcript: {len(transcript['full_transcript'])} chars, {len(transcript['segments'])} segments")
    elif not has_meaningful_speech:
        print(f"  Transcript: ⚠ Text-only video — needs visual script review")
    print(f"  CTAs found: {len(ctas)}")
    if frame_dir:
        print(f"  Frames saved to: {frame_dir}")
    print(f"  Database ID: {db_id}")

    return {
        "url": url,
        "metadata": metadata,
        "transcript": transcript,
        "ctas": ctas,
        "content_type": content_type,
        "frame_dir": frame_dir,
        "db_id": db_id
    }


def process_batch(urls):
    """Process a batch of TikTok URLs."""
    print(f"\n{'#'*60}")
    print(f"  TikTok Affiliate Analysis Pipeline")
    print(f"  Processing {len(urls)} videos")
    print(f"{'#'*60}")

    results = []
    successful = 0
    failed = 0

    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}]", end="")
        result = process_single_video(url)
        if result:
            results.append(result)
            successful += 1
        else:
            failed += 1

    print(f"\n\n{'#'*60}")
    print(f"  BATCH COMPLETE")
    print(f"  Successful: {successful} | Failed: {failed} | Total: {len(urls)}")
    print(f"{'#'*60}\n")

    return results


def process_docx(filepath):
    """Process a Word document containing TikTok URLs."""
    print(f"Reading URLs from: {filepath}")
    urls = read_urls_from_docx(filepath)
    print(f"Found {len(urls)} unique TikTok URLs")
    return process_batch(urls)


# --- Entry point for direct execution ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        if filepath.endswith('.docx'):
            process_docx(filepath)
        else:
            # Treat as a text file with URLs
            with open(filepath) as f:
                urls = read_urls_from_text(f.read())
            process_batch(urls)
    else:
        print("Usage: python analysis_pipeline.py <path_to_docx_or_txt>")
        print("  Or import and use process_batch([url1, url2, ...])")
