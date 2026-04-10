"""
LLM Output → Content Contract Transformer (v3)
===============================================
Converts raw LLM JSON into the v3 content schemas expected by the
document generators.

v3 Architecture:
  1. Edit Guide (Document 1) — 10 video concepts with full scripts
  2. Shoot Guide (Document 2) — derived FROM the edit guide data

The LLM outputs:
  - shoot_guide.json: masterShotList with talkingHeadShots, brollShots, voiceoverAudio
  - edit_guide.json: heroVideos, remixVideos

The edit guide transform merges spokenScript + visualTimeline into a
single sequential script per video, then the shoot guide is derived
from that.

Usage:
    from v2.pipeline.transform import transform_to_v3
    edit_data, shoot_data = transform_to_v3(llm_shoot, llm_edit, product_name, creator_name, analysis_count)
"""

import re
from datetime import datetime


def transform_to_v3(llm_shoot_data, llm_edit_data, product_name,
                    creator_name="Michelle", analysis_count=122):
    """
    Transform LLM outputs into v3 edit guide + shoot guide schemas.

    The edit guide is built from the LLM edit guide output (heroVideos +
    remixVideos), enriched with b-roll info from the shoot guide output.
    The shoot guide is then derived from the edit guide.

    Args:
        llm_shoot_data: dict from Pass 4 (masterShotList)
        llm_edit_data: dict from Pass 4b (heroVideos, remixVideos)
        product_name: str
        creator_name: str
        analysis_count: int

    Returns:
        (edit_guide_content, shoot_guide_content) — both as dicts
    """
    # Build b-roll lookup from shoot guide data
    master = llm_shoot_data.get("masterShotList", {})
    broll_shots = master.get("brollShots", [])
    broll_lookup = {}
    for shot in broll_shots:
        code = shot.get("label", shot.get("code", ""))
        desc = shot.get("description", shot.get("shot", ""))
        if code:
            broll_lookup[code] = desc

    # Transform hero videos
    hero_videos_raw = llm_edit_data.get("heroVideos", [])
    remix_videos_raw = llm_edit_data.get("remixVideos", [])

    videos = []

    for i, hv in enumerate(hero_videos_raw):
        video = _transform_hero_video(hv, i + 1, broll_lookup)
        videos.append(video)

    for i, rv in enumerate(remix_videos_raw):
        video = _transform_remix_video(rv, i + 1, broll_lookup)
        videos.append(video)

    hero_count = len(hero_videos_raw)
    remix_count = len(remix_videos_raw)
    now = datetime.now()

    edit_guide_content = {
        "creator_name": f"{creator_name.upper()}'S {product_name.split('—')[0].strip().split()[0].upper()} SHOOT",
        "product_name": product_name,
        "video_count": f"{hero_count} Hero Videos + {remix_count} Remix Videos",
        "analysis_count": analysis_count,
        "date": now.strftime("%B %Y"),
        "videos": videos,
    }

    # Derive shoot guide from edit guide
    from v2.templates.shoot_guide_generator import derive_shoot_guide
    shoot_guide_content = derive_shoot_guide(edit_guide_content)

    return edit_guide_content, shoot_guide_content


def _parse_time_to_seconds(t):
    """Convert '0:24' or '0:24' to seconds."""
    t = t.strip("[] ")
    parts = t.split(":")
    if len(parts) == 2:
        return int(parts[0]) * 60 + int(parts[1])
    return 0


def _seconds_to_time(s):
    """Convert seconds to 'M:SS' format."""
    return f"{s // 60}:{s % 60:02d}"


def _parse_ost_with_ranges(ost_entries, duration_str="30-40s"):
    """Parse OST entries and derive start-end timestamp ranges.

    Each OST card stays on screen until the next one starts.
    The last card stays until the end of the video.
    """
    # Parse duration to get video end time — use the MAX number from range
    nums = re.findall(r'(\d+)', duration_str)
    video_end_secs = max(int(n) for n in nums) if nums else 35

    # First pass: extract start times and text
    parsed = []
    for entry in ost_entries:
        if isinstance(entry, str):
            m = re.match(r'^\[?(\d+:\d+)(?:-\d+:\d+)?\]?\s*(.*)', entry)
            if m:
                start_secs = _parse_time_to_seconds(m.group(1))
                parsed.append({"start": start_secs, "text": m.group(2)})
            else:
                parsed.append({"start": 0, "text": entry})
        elif isinstance(entry, dict):
            ts = entry.get("timestamp", entry.get("time", ""))
            # Extract just the start time
            m = re.match(r'\[?(\d+:\d+)', ts)
            start_secs = _parse_time_to_seconds(m.group(1)) if m else 0
            parsed.append({"start": start_secs, "text": entry.get("text", "")})

    # Second pass: derive end times from the next card's start time
    result = []
    for i, item in enumerate(parsed):
        start = item["start"]
        if i + 1 < len(parsed):
            end = parsed[i + 1]["start"]
        else:
            end = video_end_secs
        # Ensure end > start (last card gets at least 3 seconds)
        if end <= start:
            end = start + 3
        ts = f"[{_seconds_to_time(start)}-{_seconds_to_time(end)}]"
        result.append({"timestamp": ts, "text": item["text"]})

    return result


def _enforce_broll_per_vo_line(broll_used, spoken_tl, max_consecutive=2):
    """
    Enforce max consecutive b-roll shots in hero videos.

    In a hero video, the creator is on camera most of the time.
    B-roll should be brief cutaways, not long clusters. When 3+
    b-roll shots appear back-to-back without an on-camera break,
    cap at max_consecutive and extend the last kept shot to fill
    the remaining time.

    This catches cases where the LLM assigns 3 rapid-fire b-roll
    shots to cover a voiceover line — the viewer's eye can't settle
    with cuts every 2 seconds in a hero video.

    Args:
        broll_used: list of {"code": "B1", "description": "...", "timestamp": "[0:08-0:11]"}
        spoken_tl: list of spokenScript entries (used for context, not filtering)
        max_consecutive: max b-roll shots in a row without on-camera break (default 2)

    Returns:
        Cleaned broll_used list (may be shorter than input)
    """
    if not broll_used or len(broll_used) <= max_consecutive:
        return broll_used

    def _broll_start(b):
        ts = b.get("timestamp", "")
        m = re.match(r'\[?(\d+):(\d+)', ts)
        return int(m.group(1)) * 60 + int(m.group(2)) if m else 0

    def _broll_end(b):
        ts = b.get("timestamp", "")
        m = re.search(r'-(\d+):(\d+)', ts)
        return int(m.group(1)) * 60 + int(m.group(2)) if m else 999

    # Sort by start time to find consecutive clusters
    indexed = [(i, _broll_start(b), _broll_end(b)) for i, b in enumerate(broll_used)]
    indexed.sort(key=lambda x: x[1])

    # Find consecutive clusters (shots within 1 second of each other = consecutive)
    clusters = []
    current_cluster = [indexed[0]]
    for j in range(1, len(indexed)):
        prev_end = current_cluster[-1][2]
        curr_start = indexed[j][1]
        # If this shot starts within 1 second of previous shot's end, it's consecutive
        if curr_start <= prev_end + 1:
            current_cluster.append(indexed[j])
        else:
            clusters.append(current_cluster)
            current_cluster = [indexed[j]]
    clusters.append(current_cluster)

    # Enforce max_consecutive per cluster
    shots_to_remove = set()
    for cluster in clusters:
        if len(cluster) <= max_consecutive:
            continue

        # Keep first max_consecutive shots, remove the rest
        to_keep = cluster[:max_consecutive]
        to_remove = cluster[max_consecutive:]

        for idx, _, _ in to_remove:
            shots_to_remove.add(idx)

        # Extend the last kept shot's end time to cover the removed range
        last_kept_idx = to_keep[-1][0]
        last_removed_end = to_remove[-1][2]
        last_kept_start = to_keep[-1][1]
        new_ts = f"[{_seconds_to_time(last_kept_start)}-{_seconds_to_time(last_removed_end)}]"
        broll_used[last_kept_idx]["timestamp"] = new_ts

    # Build cleaned list preserving original order
    cleaned = [b for i, b in enumerate(broll_used) if i not in shots_to_remove]
    return cleaned


def _transform_hero_video(hv, number, broll_lookup):
    """Transform a single hero video from LLM output to v3 schema."""
    spoken_tl = hv.get("spokenScript", [])
    visual_tl = hv.get("visualTimeline", [])
    ost_entries = hv.get("onScreenText", [])

    # Extract hook text
    hook_data = hv.get("hook", {})
    if isinstance(hook_data, dict):
        hook_text = hook_data.get("spoken", hook_data.get("text", ""))
    else:
        hook_text = str(hook_data)

    # Build script from spokenScript
    script = []
    for sp in spoken_tl:
        on_camera = sp.get("onCamera", True)
        script.append({
            "type": "on_camera" if on_camera else "voiceover",
            "text": sp.get("text", ""),
        })

    # Extract b-roll references from visualTimeline WITH timestamps
    broll_used = []
    broll_seen = set()
    for entry in visual_tl:
        shot = entry.get("shot", "")
        if re.match(r'^B\d+', shot):
            code = re.match(r'^B\d+', shot).group()
            if code not in broll_seen:
                broll_seen.add(code)
                desc = broll_lookup.get(code, entry.get("description", ""))
                raw_time = entry.get("time", "")
                ts = f"[{raw_time}]" if raw_time and not raw_time.startswith("[") else raw_time
                broll_used.append({
                    "code": code,
                    "description": desc,
                    "timestamp": ts,
                })

    # Enforce max 2 b-roll shots per voiceover line (hero-only safeguard)
    broll_used = _enforce_broll_per_vo_line(broll_used, spoken_tl, max_consecutive=2)

    # Build on-screen text WITH start-end timestamps
    duration_str = hv.get("duration", "30-40s")
    on_screen_text = _parse_ost_with_ranges(ost_entries, duration_str)

    # Audio type
    music = hv.get("music", "")
    if not music or "original" in music.lower() or "no music" in music.lower():
        audio = "Original creator audio"
    else:
        audio = music

    # Avoid title duplication — LLM may already include "Hero N — " prefix
    raw_title = hv.get("title", "Untitled")
    if raw_title.lower().startswith(f"hero {number}"):
        display_title = raw_title
    else:
        display_title = f"Hero {number} — {raw_title}"

    return {
        "number": number,
        "type": "hero",
        "title": display_title,
        "hook": hook_text,
        "duration": hv.get("duration", "~30 seconds"),
        "audio": audio,
        "hook_template": hv.get("hook_template", ""),
        "script": script,
        "broll_used": broll_used,
        "on_screen_text": on_screen_text,
    }


def _transform_remix_video(rv, number, broll_lookup):
    """Transform a single remix video from LLM output to v3 schema."""
    # Build script from voiceover + shot assembly
    script = []

    vo_script = rv.get("voiceoverScript", rv.get("voiceover_script", ""))
    if vo_script:
        # Split voiceover into sentences for the script
        sentences = re.split(r'(?<=[.!?])\s+', vo_script)
        for sent in sentences:
            if sent.strip():
                script.append({
                    "type": "voiceover",
                    "text": sent.strip(),
                })

    # Extract b-roll references from shotAssembly
    broll_used = []
    broll_seen = set()
    shot_assembly = rv.get("shotAssembly", rv.get("broll_assembly", []))
    for shot in shot_assembly:
        if isinstance(shot, str):
            m = re.match(r'^(B\d+)', shot)
            if m:
                code = m.group(1)
                if code not in broll_seen:
                    broll_seen.add(code)
                    desc = shot.replace(code, "").strip(" —-")
                    if not desc:
                        desc = broll_lookup.get(code, "")
                    broll_used.append({
                        "code": code,
                        "description": desc,
                    })
        elif isinstance(shot, dict):
            code = shot.get("code", "")
            if code and code not in broll_seen:
                broll_seen.add(code)
                broll_used.append({
                    "code": code,
                    "description": shot.get("description", broll_lookup.get(code, "")),
                })

    # On-screen text WITH start-end timestamps
    ost_entries = rv.get("onScreenText", rv.get("onscreen_text_script", []))
    duration_str = rv.get("duration", "15-20s")
    on_screen_text = _parse_ost_with_ranges(ost_entries, duration_str)

    # Hook — use first OST line or first VO sentence as hook
    hook = ""
    if on_screen_text:
        hook = on_screen_text[0] if isinstance(on_screen_text[0], str) else on_screen_text[0].get("text", "")
    elif vo_script:
        hook = vo_script.split(".")[0]

    # Audio type
    music = rv.get("music", "")
    if not music:
        audio = "TikTok trending sound" if not vo_script else "Original creator audio"
    else:
        audio = music

    # Avoid title duplication
    raw_title = rv.get("title", "Untitled")
    if raw_title.lower().startswith(f"remix {number}"):
        display_title = raw_title
    else:
        display_title = f"Remix {number} — {raw_title}"

    return {
        "number": number,
        "type": "remix",
        "title": display_title,
        "hook": hook,
        "duration": rv.get("duration", "15-20 seconds"),
        "audio": audio,
        "script": script,
        "broll_used": broll_used,
        "on_screen_text": on_screen_text,
    }


# ═══════════════════════════════════════════════════════════════
# LEGACY FUNCTIONS (kept for backward compatibility)
# ═══════════════════════════════════════════════════════════════

def transform_shoot_guide(llm_data, product_name, creator_name="Michelle"):
    """Legacy transform — use transform_to_v3 instead."""
    # Return minimal structure for old pipeline path
    master = llm_data.get("masterShotList", {})
    ths = master.get("talkingHeadShots", [])
    brolls = master.get("brollShots", [])
    vos = master.get("voiceoverAudio", [])

    product_tag = product_name.split()[0].upper()

    heroes = []
    for th in ths:
        hero = {
            "code": th.get("label", f"TH{len(heroes)+1}"),
            "product": product_tag,
            "title": th.get("concept", ""),
            "notes": [],
            "lines": [],
        }
        note = th.get("note", "")
        if note:
            hero["notes"] = [note]
        for line in th.get("lines", []):
            hero["lines"].append({
                "code": line.get("id", line.get("code", "")),
                "tag": line.get("type", line.get("tag", "ON CAMERA")),
                "text": line.get("text", ""),
                "is_direction": line.get("is_direction", False),
            })
        heroes.append(hero)

    broll = [{"code": s.get("label", f"B{i+1}"), "product": product_tag,
              "description": s.get("description", "")} for i, s in enumerate(brolls)]

    voiceovers = []
    for vo in vos:
        script = vo.get("script", vo.get("text", ""))
        if isinstance(script, list):
            script = " ".join(script)
        voiceovers.append({"code": vo.get("label", f"VO{len(voiceovers)+1}"),
                           "product": product_tag, "script": script})

    return {
        "title": "SHOOT GUIDE",
        "subtitle": product_name,
        "product_summary": f"{product_name}",
        "heroes": heroes,
        "broll": broll,
        "voiceovers": voiceovers,
    }


def transform_edit_guide(llm_data, product_name, creator_name="Michelle", analysis_count=122):
    """Legacy transform — use transform_to_v3 instead."""
    # Minimal passthrough for backward compat
    return {"creator_name": creator_name, "product_name": product_name,
            "video_count": "", "analysis_count": analysis_count,
            "date": datetime.now().strftime("%B %Y"), "videos": []}
