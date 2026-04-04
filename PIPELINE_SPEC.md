# TikTok Content Engine — Pipeline Specification
**Version 1.0 | April 4, 2026 | Single Source of Truth**

> READ THIS AT THE START OF EVERY SESSION. No exceptions.

---

## Nonnegotiables

These rules are **permanent**. They cannot be overridden for speed, convenience, or any other reason.

| # | Rule | Why |
|---|------|-----|
| 1 | **Whisper is the ONLY transcription method.** No Google Speech Recognition. No exceptions. | Google Speech Recognition produced garbled, inaccurate transcripts. |
| 2 | **On-screen text requires frame-by-frame visual verification.** Only record what is visually confirmed in extracted frames. No inferring from transcripts. | Inference from transcripts produced fabricated data that didn't match what was on screen. |
| 3 | **Always use `--force-overwrites --no-cache-dir`** on every yt-dlp call. | Without these, yt-dlp silently skips downloads, causing cross-video contamination. |
| 4 | **Delete all stale frames before extracting new ones.** | Leftover frames from previous videos mix into new analysis. |
| 5 | **Content generation MUST filter to `transcript_type = 'creator_speech'` only.** | borrowed_audio and no_speech would pollute recommendations. |
| 6 | **2-layer validation gate must pass before any content is generated.** | Row presence alone is insufficient; content must be meaningful. |
| 7 | **Accuracy over speed, always.** No shortcuts. No fabrication. | The system is only as good as the data that feeds it. |

---

## Ingestion Pipeline (6 Steps)

### Step 1: Video Download
- **Tool:** yt-dlp at `~/.local/bin/yt-dlp`
- **Flags:** `--force-overwrites --no-cache-dir` (ALWAYS)
- **Format:** h264_540p preferred, fallback to any h264 mp4, then "best"
- **Timeout:** 120 seconds

### Step 2: Metadata Extraction
- **Target:** `tiktok_videos` table
- **Required fields:** product_name, product_category, audio_type, content_type, hook_format, voice_delivery, performance_insight
- **Source:** yt-dlp metadata + manual classification

### Step 3: Transcription
- **Tool:** OpenAI Whisper base model (CPU-only PyTorch). THE ONLY TOOL.
- **Process:** Extract WAV audio (ffmpeg) → transcribe with Whisper → classify transcript_type → store
- **Classification:**
  - `creator_speech` (91 videos) — usable for content generation
  - `borrowed_audio` (13 videos) — trending sounds, NOT usable
  - `no_speech` (18 videos) — music only / hallucinations, NOT usable

### Step 4: Visual Analysis (Frame-by-Frame)
- **Tools:** ffmpeg for extraction, visual inspection of EVERY frame
- **Frame rate:** fps=1/2 for <30s videos, fps=1/3 for longer
- **Pre-step:** Delete ALL stale frames before extraction
- **Tables updated:** video_onscreen_text, video_visual_scripts, video_visual_notes, video_shot_breakdown
- **CRITICAL:** Only record text visually confirmed in frames. If no creator overlays, insert: text_content='No creator-added on-screen text overlays', text_type='none'
- **NOT on-screen text:** TikTok UI (like/share buttons, username watermark)

### Step 5: CTA Extraction
- **Target:** `video_ctas` table
- **Source:** Transcript + visual analysis

### Step 6: Validation Gate
- **Tool:** `validate_video_completeness()` in content_engine.py
- **Layer 1:** Row presence in all 6 per-video tables + 7 required fields in tiktok_videos
- **Layer 2:** Content quality — min_length thresholds + reject_pattern checks
- **Blocks pipeline if failed:** Yes

---

## Tools Inventory

| Tool | Location | Used For |
|------|----------|----------|
| yt-dlp | `~/.local/bin/yt-dlp` | Video download |
| ffmpeg | System install | Frame extraction (JPG), audio extraction (WAV) |
| OpenAI Whisper | Base model, CPU-only | Transcription (THE ONLY METHOD) |
| Supabase | Project: owklfaoaxdrggmbtcwpn | PostgreSQL database |

---

## Database Schema (10 tables)

**Per-video tables (6):** video_transcripts, video_onscreen_text, video_visual_scripts, video_visual_notes, video_shot_breakdown, video_ctas

**Master table:** tiktok_videos (metadata, engagement, product info)

**Reference tables (3):** hook_patterns (94 rows), cta_patterns (9 rows), creators (1 row: Michelle)

**Key column types:**
- `shot_sequence` in video_shot_breakdown is a **TEXT ARRAY** — use `ARRAY['...']` syntax, NOT JSON
- `transcript_type` in video_transcripts — creator_speech / borrowed_audio / no_speech

---

## Known Gotchas

1. **Stale file contamination** — yt-dlp skips if file exists. Guard: --force-overwrites
2. **Stale frame contamination** — leftover frames from previous video. Guard: delete before extract
3. **Whisper hallucinations** — gibberish on silent videos. Guard: transcript_type classification
4. **Fabricated on-screen text** — inferred from transcript instead of frames. Guard: mandatory visual inspection
5. **shot_sequence type** — TEXT ARRAY not JSONB. Guard: use ARRAY[] syntax
6. **TikTok URL redirects** — short URLs can change over time. Guard: cross-reference content vs metadata

---

## Current State (as of 2026-04-04)
- **122 videos**, 122/122 pass both validation layers
- **91 creator_speech** | 13 borrowed_audio | 18 no_speech
- Full pipeline spec document: `TikTok_Pipeline_Spec_v1.docx` on Desktop
