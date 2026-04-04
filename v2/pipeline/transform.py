"""
LLM Output → Content Contract Transformer
==========================================
Converts raw LLM JSON (from run_product.py Passes 4 and 4b) into the
content.json contract format expected by the v2 document generators.

The LLM outputs:
  - shoot_guide.json with masterShotList.talkingHeadShots, brollShots, voiceoverAudio
  - edit_guide.json with heroVideos, remixVideos, uploadDetails

The v2 generators expect:
  - shoot_guide with heroes, broll, voiceovers
  - edit_guide with heroes, remixes, upload_details

Usage:
    from v2.pipeline.transform import transform_shoot_guide, transform_edit_guide
    shoot = transform_shoot_guide(llm_shoot_data, product_name, creator_name)
    edit = transform_edit_guide(llm_edit_data, product_name, creator_name, analysis_count)
"""

from datetime import datetime


def transform_shoot_guide(llm_data, product_name, creator_name="Michelle"):
    """
    Transform LLM shoot guide output to v2 generator schema.

    Args:
        llm_data: dict from parse_json_from_response (Pass 4)
        product_name: str
        creator_name: str

    Returns:
        dict matching v2/templates/shoot_guide_generator.py CONTENT_SCHEMA
    """
    master = llm_data.get("masterShotList", {})
    ths = master.get("talkingHeadShots", [])
    brolls = master.get("brollShots", [])
    vos = master.get("voiceoverAudio", [])

    # Short product tag for labels (e.g. "GYMREAPERS" or "BOOMBOOM")
    product_tag = product_name.split()[0].upper()

    heroes = []
    for th in ths:
        hero = {
            "code": th.get("label", f"TH{len(heroes)+1}"),
            "product": product_tag,
            "title": th.get("concept", "").replace(f"Hero {len(heroes)+1} — ", ""),
            "content_angle": th.get("content_angle", ""),
            "angle_evidence": th.get("angle_evidence", {}),
            "hook_template": th.get("hook_template", ""),
            "notes": [],
            "lines": [],
        }

        # Convert note to notes list
        note = th.get("note", "")
        if note:
            hero["notes"] = [note]
        if th.get("notes"):
            hero["notes"] = th["notes"] if isinstance(th["notes"], list) else [th["notes"]]

        # Convert lines
        for line in th.get("lines", []):
            hero["lines"].append({
                "code": line.get("id", line.get("code", "")),
                "tag": line.get("type", line.get("tag", "ON CAMERA")),
                "text": line.get("text", ""),
                "is_direction": line.get("is_direction", False),
            })

        heroes.append(hero)

    # Transform b-roll
    broll = []
    for shot in brolls:
        broll.append({
            "code": shot.get("label", shot.get("code", f"B{len(broll)+1}")),
            "product": product_tag,
            "description": shot.get("description", shot.get("shot", "")),
        })

    # Transform voiceovers
    voiceovers = []
    for vo in vos:
        script = vo.get("script", vo.get("text", ""))
        if isinstance(script, list):
            script = " ".join(script)
        voiceovers.append({
            "code": vo.get("label", vo.get("code", f"VO{len(voiceovers)+1}")),
            "product": product_tag,
            "content_angle": vo.get("content_angle", ""),
            "hook_template": vo.get("hook_template", ""),
            "script": script,
        })

    hero_count = len(heroes)
    broll_count = len(broll)
    vo_count = len(voiceovers)
    no_voice_count = max(0, 5 - hero_count - vo_count) if hero_count < 5 else 0

    return {
        "title": "SHOOT GUIDE",
        "subtitle": product_name,
        "product_summary": f"{product_name} — {hero_count} Heroes, {broll_count} B-Roll Clips, {vo_count} VOs",
        "tagline": f"{hero_count + vo_count + no_voice_count} videos from one shoot — here's everything you need to capture",
        "heroes": heroes,
        "broll": broll,
        "voiceovers": voiceovers,
    }


def transform_edit_guide(llm_data, product_name, creator_name="Michelle", analysis_count=122):
    """
    Transform LLM edit guide output to v2 generator schema.

    Args:
        llm_data: dict from parse_json_from_response (Pass 4b)
        product_name: str
        creator_name: str
        analysis_count: int — number of videos in database

    Returns:
        dict matching v2/templates/edit_guide_generator.py CONTENT_SCHEMA
    """
    hero_videos = llm_data.get("heroVideos", [])
    remix_videos = llm_data.get("remixVideos", [])
    upload = llm_data.get("uploadDetails", llm_data.get("upload_details", {}))

    heroes = []
    for i, hv in enumerate(hero_videos):
        # Build timeline from visualTimeline or spokenScript
        timeline = []
        visual_tl = hv.get("visualTimeline", [])
        spoken_tl = hv.get("spokenScript", [])

        if visual_tl:
            for entry in visual_tl:
                timeline.append({
                    "timestamp": f"[{entry.get('time', '')}]" if not entry.get('time', '').startswith('[') else entry.get('time', ''),
                    "shot_ref": entry.get("shot", ""),
                    "content": entry.get("description", ""),
                })
        elif spoken_tl:
            for entry in spoken_tl:
                on_camera = entry.get("onCamera", True)
                prefix = "ON CAMERA: " if on_camera else "VOICEOVER: "
                timeline.append({
                    "timestamp": f"[{entry.get('time', '')}]" if not entry.get('time', '').startswith('[') else entry.get('time', ''),
                    "shot_ref": f"TH{i+1}",
                    "content": f'{prefix}"{entry.get("text", "")}"',
                })

        # Build on-screen text
        onscreen_text = []
        ost_entries = hv.get("onScreenText", hv.get("onscreen_text", []))
        for entry in ost_entries:
            if isinstance(entry, str):
                # Parse "[0:00-0:03] text" format
                import re
                m = re.match(r'\[?(\d+:\d+-\d+:\d+)\]?\s*(.*)', entry)
                if m:
                    onscreen_text.append({
                        "timestamp": f"[{m.group(1)}]",
                        "text": m.group(2),
                    })
                else:
                    onscreen_text.append({"timestamp": "", "text": entry})
            elif isinstance(entry, dict):
                ts = entry.get("timestamp", entry.get("time", ""))
                if ts and not ts.startswith("["):
                    ts = f"[{ts}]"
                onscreen_text.append({
                    "timestamp": ts,
                    "text": entry.get("text", ""),
                    "ost_template": entry.get("ost_template", ""),
                })

        # Extract hook text
        hook_data = hv.get("hook", {})
        if isinstance(hook_data, dict):
            hook_text = hook_data.get("spoken", hook_data.get("text", ""))
        else:
            hook_text = str(hook_data)

        heroes.append({
            "label": f"HERO VIDEO {i+1}",
            "title": hv.get("title", f"Hero {i+1}"),
            "content_angle": hv.get("angle", hv.get("content_angle", "")),
            "hook": hook_text,
            "audio": hv.get("music", "Original sound (no music)"),
            "hook_template": hv.get("hook_template", ""),
            "timeline": timeline,
            "onscreen_text": onscreen_text,
        })

    # Transform remixes
    remixes = []
    for rv in remix_videos:
        # B-roll assembly
        broll_assembly = rv.get("shotAssembly", rv.get("broll_assembly", []))

        # OST script
        ost_script = rv.get("onScreenText", rv.get("onscreen_text_script", []))

        # VO script
        vo_script = rv.get("voiceoverScript", rv.get("voiceover_script"))

        # Info line
        info_line = rv.get("info_line", "")
        if not info_line:
            product_tag = product_name.split()[0].upper()
            fmt = rv.get("format", "")
            if vo_script:
                info_line = f"{product_tag} • VOICEOVER"
            else:
                info_line = f"{product_tag} • NO VOICEOVER — music + text only"

        remixes.append({
            "title": rv.get("title", f"Remix {len(remixes)+1}"),
            "info_line": info_line,
            "content_angle": rv.get("angle", rv.get("content_angle", "")),
            "broll_assembly": broll_assembly,
            "onscreen_text_script": ost_script,
            "voiceover_script": vo_script,
        })

    # Upload details
    upload_details = {
        "hashtags": upload.get("hashtags", []),
        "captions": upload.get("captions", []),
        "schedule": upload.get("schedule", upload.get("notes", [])),
    }

    hero_count = len(heroes)
    remix_count = len(remixes)
    now = datetime.now()

    return {
        "creator_name": f"{creator_name.upper()}'S {product_name.split()[0].upper()} SHOOT",
        "product_summary": f"{product_name} — {hero_count} Heroes, {remix_count} Remixes",
        "video_counts": f"{hero_count} Hero Videos + {remix_count} Remix Videos — Production Ready",
        "analysis_count": analysis_count,
        "date": now.strftime("%B %Y"),
        "heroes": heroes,
        "remixes": remixes,
        "upload_details": upload_details,
    }
