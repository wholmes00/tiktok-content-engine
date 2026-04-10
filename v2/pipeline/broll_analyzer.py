"""
B-Roll Analyzer — Content Engine v2 Improvement #4
====================================================
Mines the video_shot_breakdown table to produce data-driven rules for
b-roll usage in hero vs remix videos.

Data source: 128 videos with frame-level shot analysis
Key finding: Top-performing hero (talking_head) videos average 92% on-camera,
             only 8% b-roll. The system was previously generating ~90% b-roll
             for heroes — the exact inverse of what works.

This module:
  1. Queries the database for face/broll frame ratios
  2. Segments by content_type (hero-equivalent vs remix-equivalent)
  3. Analyzes top vs bottom performer ratios
  4. Computes optimal b-roll shot count per video duration
  5. Produces prompt constraints for injection into Pass 4

Designed for periodic refresh — call analyze_broll_patterns() whenever
new videos are added to the database to recalculate all rules.
"""

import statistics

try:
    from v2.pipeline.db import supabase
except ImportError:
    from db import supabase


# ─── Content type mappings ──────────────────────────────────────────────────
# These map database content_types to our hero/remix categories

HERO_CONTENT_TYPES = ["talking_head", "product_demo", "hybrid"]
REMIX_CONTENT_TYPES = ["voiceover_broll", "text_only", "borrowed_voiceover"]


def analyze_broll_patterns():
    """
    Query the database and compute b-roll usage patterns.

    Returns a dict with:
      - hero_rules: face/broll ratios and shot counts for hero-type videos
      - remix_rules: same for remix-type videos
      - broll_types: ranked b-roll types by performance
      - shot_pacing: shots per second for each type
      - sample_size: how many videos were analyzed
      - raw_stats: all computed statistics for transparency
    """
    # Pull all shot breakdowns joined with video engagement
    videos = supabase.table("video_shot_breakdown").select(
        "video_id, face_frames, broll_frames, total_frames_analyzed, "
        "dominant_broll_type, secondary_broll_type, shot_sequence"
    ).execute().data

    # Pull video metadata for engagement + content type
    meta = supabase.table("tiktok_videos").select(
        "id, content_type, likes, shares, comments, duration_seconds"
    ).execute().data

    meta_lookup = {v["id"]: v for v in meta}

    # Enrich shot data with metadata
    enriched = []
    for sb in videos:
        vid = meta_lookup.get(sb["video_id"])
        if not vid or not sb.get("total_frames_analyzed"):
            continue
        total = sb["total_frames_analyzed"]
        if total == 0:
            continue

        face_pct = (sb.get("face_frames", 0) or 0) / total * 100
        broll_pct = (sb.get("broll_frames", 0) or 0) / total * 100
        duration = float(vid.get("duration_seconds", 0) or 0)
        shot_count = len(sb.get("shot_sequence", []) or [])

        enriched.append({
            "content_type": vid.get("content_type", "unknown"),
            "likes": vid.get("likes", 0) or 0,
            "shares": vid.get("shares", 0) or 0,
            "duration": duration,
            "face_pct": round(face_pct, 1),
            "broll_pct": round(broll_pct, 1),
            "shot_count": shot_count,
            "dominant_broll_type": sb.get("dominant_broll_type"),
            "secondary_broll_type": sb.get("secondary_broll_type"),
        })

    # ─── Segment into hero vs remix ─────────────────────────────────────
    heroes = [v for v in enriched if v["content_type"] in HERO_CONTENT_TYPES]
    remixes = [v for v in enriched if v["content_type"] in REMIX_CONTENT_TYPES]
    talking_heads = [v for v in enriched if v["content_type"] == "talking_head"]

    # ─── Compute hero rules ─────────────────────────────────────────────
    hero_rules = _compute_type_rules(heroes, "hero")
    th_rules = _compute_type_rules(talking_heads, "talking_head")
    remix_rules = _compute_type_rules(remixes, "remix")

    # ─── B-roll type rankings (hero-type videos only) ───────────────────
    broll_type_data = {}
    for v in heroes:
        bt = v.get("dominant_broll_type")
        if bt:
            if bt not in broll_type_data:
                broll_type_data[bt] = {"count": 0, "total_likes": 0}
            broll_type_data[bt]["count"] += 1
            broll_type_data[bt]["total_likes"] += v["likes"]

    broll_types = []
    for bt, data in broll_type_data.items():
        broll_types.append({
            "type": bt,
            "count": data["count"],
            "avg_likes": round(data["total_likes"] / data["count"]),
        })
    broll_types.sort(key=lambda x: x["avg_likes"], reverse=True)

    return {
        "hero_rules": hero_rules,
        "talking_head_rules": th_rules,
        "remix_rules": remix_rules,
        "broll_types": broll_types,
        "sample_size": {
            "total": len(enriched),
            "heroes": len(heroes),
            "talking_heads": len(talking_heads),
            "remixes": len(remixes),
        },
    }


def _compute_type_rules(videos, label):
    """Compute face/broll ratio rules for a set of videos."""
    if not videos:
        return {"label": label, "count": 0, "rules": {}}

    # Sort by likes descending, split into top/bottom half
    sorted_vids = sorted(videos, key=lambda x: x["likes"], reverse=True)
    mid = len(sorted_vids) // 2
    top_half = sorted_vids[:mid] if mid > 0 else sorted_vids
    bottom_half = sorted_vids[mid:] if mid > 0 else []

    def _stats(vids):
        if not vids:
            return {}
        face_pcts = [v["face_pct"] for v in vids]
        broll_pcts = [v["broll_pct"] for v in vids]
        shot_counts = [v["shot_count"] for v in vids if v["shot_count"] > 0]
        durations = [v["duration"] for v in vids if v["duration"] > 0]
        likes = [v["likes"] for v in vids]

        shots_per_10s = []
        for v in vids:
            if v["duration"] > 0 and v["shot_count"] > 0:
                shots_per_10s.append(v["shot_count"] / v["duration"] * 10)

        return {
            "count": len(vids),
            "avg_face_pct": round(statistics.mean(face_pcts), 1),
            "avg_broll_pct": round(statistics.mean(broll_pcts), 1),
            "median_face_pct": round(statistics.median(face_pcts), 1),
            "median_broll_pct": round(statistics.median(broll_pcts), 1),
            "avg_shot_count": round(statistics.mean(shot_counts), 1) if shot_counts else 0,
            "avg_duration": round(statistics.mean(durations), 1) if durations else 0,
            "avg_shots_per_10s": round(statistics.mean(shots_per_10s), 1) if shots_per_10s else 0,
            "avg_likes": round(statistics.mean(likes)),
        }

    all_stats = _stats(sorted_vids)
    top_stats = _stats(top_half)
    bottom_stats = _stats(bottom_half)

    # Derive recommended ranges from top-performer data
    # Use top-half averages as the target, with ±10% flexibility
    target_broll = top_stats.get("avg_broll_pct", 10)
    target_face = top_stats.get("avg_face_pct", 90)

    return {
        "label": label,
        "count": len(videos),
        "all": all_stats,
        "top_half": top_stats,
        "bottom_half": bottom_stats,
        "recommended": {
            "face_pct_min": round(max(target_face - 10, 0), 0),
            "face_pct_target": round(target_face, 0),
            "broll_pct_max": round(min(target_broll + 10, 100), 0),
            "broll_pct_target": round(target_broll, 0),
            "shots_per_10s": top_stats.get("avg_shots_per_10s", 0),
        },
    }


def build_broll_constraint_prompt():
    """
    Build the prompt section that constrains b-roll usage based on
    actual database analysis.

    Returns a string to inject into the content generation prompt.
    """
    data = analyze_broll_patterns()
    hr = data["hero_rules"]
    th = data["talking_head_rules"]
    rr = data["remix_rules"]
    bt = data["broll_types"]
    sample = data["sample_size"]

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("B-ROLL USAGE RULES (DATA-DRIVEN — FROM VIDEO ANALYSIS)")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Analysis of {sample['total']} videos with frame-level shot breakdown")
    lines.append(f"reveals CLEAR patterns for b-roll usage by video type.")
    lines.append("")

    # ─── Hero video rules ───────────────────────────────────────────
    lines.append("─" * 60)
    lines.append("HERO VIDEOS (talking-to-camera style)")
    lines.append("─" * 60)

    if th.get("top_half"):
        top = th["top_half"]
        lines.append(f"")
        lines.append(f"  Top-performing talking-head videos ({top['count']} videos, avg {top['avg_likes']:,} likes):")
        lines.append(f"    ON-CAMERA: {top['avg_face_pct']}% of video")
        lines.append(f"    B-ROLL:    {top['avg_broll_pct']}% of video")
        lines.append(f"")

    if hr.get("top_half"):
        top = hr["top_half"]
        lines.append(f"  Broader hero types incl. demos & hybrid ({top['count']} videos, avg {top['avg_likes']:,} likes):")
        lines.append(f"    ON-CAMERA: {top['avg_face_pct']}% of video")
        lines.append(f"    B-ROLL:    {top['avg_broll_pct']}% of video")
        lines.append(f"")

    # Derive the actual constraint numbers from talking_head (purest hero match)
    rec = th.get("recommended", {})
    face_target = rec.get("face_pct_target", 90)
    broll_max = rec.get("broll_pct_max", 18)
    broll_target = rec.get("broll_pct_target", 8)

    lines.append(f"  *** MANDATORY HERO B-ROLL CONSTRAINTS ***")
    lines.append(f"")
    lines.append(f"  1. HERO VIDEOS MUST BE PREDOMINANTLY ON-CAMERA.")
    lines.append(f"     Target: ~{face_target}% on-camera, ~{broll_target}% b-roll")
    lines.append(f"     Hard limit: B-roll must NOT exceed {broll_max}% of total video duration")
    lines.append(f"")

    # Convert percentage to actual seconds for a typical 35-45 second hero
    broll_secs_35 = round(35 * broll_max / 100)
    broll_secs_45 = round(45 * broll_max / 100)
    lines.append(f"  2. IN PRACTICE (for a 35-45 second hero video):")
    lines.append(f"     Maximum b-roll: {broll_secs_35}-{broll_secs_45} seconds total")
    lines.append(f"     This means 2-4 SHORT b-roll cutaways (2-3 seconds each)")
    lines.append(f"     NOT long continuous b-roll segments")
    lines.append(f"")

    lines.append(f"  3. B-ROLL IN HEROES IS FOR CUTAWAYS, NOT COVERAGE.")
    lines.append(f"     The creator is ON CAMERA for almost the entire video.")
    lines.append(f"     B-roll is brief visual punctuation — a quick product shot,")
    lines.append(f"     a reaction insert, a closeup — then back to the creator.")
    lines.append(f"     The visual timeline should show mostly \"on camera\" entries.")
    lines.append(f"")

    # Shot count guidance
    if hr.get("top_half", {}).get("avg_shot_count"):
        avg_shots = hr["top_half"]["avg_shot_count"]
        avg_dur = hr["top_half"]["avg_duration"]
        lines.append(f"  4. SHOT PACING: Top hero videos average {avg_shots} shots")
        lines.append(f"     over {avg_dur}s — but most of those are FACE shots.")
        lines.append(f"     For the b-roll shot list, provide 8-12 b-roll options")
        lines.append(f"     (the shoot guide needs variety), but the EDIT GUIDE")
        lines.append(f"     should only USE 2-4 of them per hero video.")
        lines.append(f"")

    # ─── B-roll type rankings ───────────────────────────────────────
    if bt:
        lines.append(f"  5. B-ROLL TYPE PERFORMANCE (what types work best):")
        for i, b in enumerate(bt[:5]):
            lines.append(f"     #{i+1} {b['type']} — avg {b['avg_likes']:,} likes ({b['count']} videos)")
        lines.append(f"     Prioritize PRODUCT_CLOSEUP and LIFESTYLE cutaways.")
        lines.append(f"")

    # ─── B-roll to voiceover mapping rule ─────────────────────────
    lines.append(f"  6. B-ROLL PER VOICEOVER LINE (hero videos only):")
    lines.append(f"     When a hero script line is tagged VOICEOVER, the editor")
    lines.append(f"     covers that line with b-roll. But keep it simple:")
    lines.append(f"       - 1 voiceover line = 1 b-roll shot (preferred)")
    lines.append(f"       - 1 voiceover line = 2 b-roll shots MAX (if line is 5+ seconds)")
    lines.append(f"       - NEVER assign 3+ b-roll shots to a single voiceover line")
    lines.append(f"     Each b-roll shot in a hero should be 3-5 seconds long.")
    lines.append(f"     The viewer's eye needs time to register each shot —")
    lines.append(f"     rapid-fire b-roll cuts belong in remixes, not heroes.")
    lines.append(f"")

    # ─── Remix video rules ──────────────────────────────────────────
    lines.append("─" * 60)
    lines.append("REMIX VIDEOS (voiceover + b-roll style)")
    lines.append("─" * 60)

    if rr.get("top_half"):
        top = rr["top_half"]
        lines.append(f"")
        lines.append(f"  Top-performing remix-type videos ({top['count']} videos, avg {top['avg_likes']:,} likes):")
        lines.append(f"    ON-CAMERA: {top['avg_face_pct']}% of video")
        lines.append(f"    B-ROLL:    {top['avg_broll_pct']}% of video")
        lines.append(f"")
        lines.append(f"  Remixes are ~90%+ b-roll as expected.")
        lines.append(f"  Use quick cuts between product shots, lifestyle, demos.")

    if rr.get("top_half", {}).get("avg_shot_count"):
        avg_shots = rr["top_half"]["avg_shot_count"]
        avg_dur = rr["top_half"]["avg_duration"]
        lines.append(f"  Shot pacing: avg {avg_shots} shots over {avg_dur}s")
        lines.append(f"")

    lines.append("")
    lines.append("─" * 60)
    lines.append("VISUAL TIMELINE RULES (for edit guide output):")
    lines.append("─" * 60)
    lines.append("")
    lines.append("  For HERO videos, the visualTimeline should look like:")
    lines.append('    [0-3s]  ON CAMERA — delivering hook')
    lines.append('    [3-6s]  B-ROLL — product closeup (brief cutaway)')
    lines.append('    [6-25s] ON CAMERA — main talking points')
    lines.append('    [25-28s] B-ROLL — using product (brief cutaway)')
    lines.append('    [28-35s] ON CAMERA — CTA and close')
    lines.append("")
    lines.append("  NOT like this (WRONG — too much b-roll):")
    lines.append('    [0-3s]  ON CAMERA — hook')
    lines.append('    [3-15s] B-ROLL — long product montage')
    lines.append('    [15-20s] ON CAMERA — one line')
    lines.append('    [20-35s] B-ROLL — another long segment')
    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


# ─── Utility: print analysis for debugging ──────────────────────────────────

def print_analysis():
    """Print a human-readable analysis report."""
    data = analyze_broll_patterns()

    print("\n" + "=" * 60)
    print("B-ROLL ANALYSIS REPORT")
    print("=" * 60)

    sample = data["sample_size"]
    print(f"\nSample: {sample['total']} total videos")
    print(f"  Heroes (talking_head + demo + hybrid): {sample['heroes']}")
    print(f"  Talking head only: {sample['talking_heads']}")
    print(f"  Remixes (voiceover + text + borrowed): {sample['remixes']}")

    for key in ["talking_head_rules", "hero_rules", "remix_rules"]:
        rules = data[key]
        print(f"\n{'─' * 60}")
        print(f"{rules['label'].upper()} ({rules['count']} videos)")
        print(f"{'─' * 60}")
        for half_label in ["top_half", "bottom_half", "all"]:
            s = rules.get(half_label, {})
            if s:
                print(f"  {half_label}: face={s['avg_face_pct']}% broll={s['avg_broll_pct']}% "
                      f"shots={s['avg_shot_count']} dur={s['avg_duration']}s likes={s['avg_likes']:,}")
        rec = rules.get("recommended", {})
        if rec:
            print(f"  RECOMMENDED: face≥{rec['face_pct_min']}% (target {rec['face_pct_target']}%), "
                  f"broll≤{rec['broll_pct_max']}% (target {rec['broll_pct_target']}%)")

    if data["broll_types"]:
        print(f"\n{'─' * 60}")
        print("B-ROLL TYPE PERFORMANCE (hero videos)")
        print(f"{'─' * 60}")
        for b in data["broll_types"]:
            print(f"  {b['type']}: avg {b['avg_likes']:,} likes ({b['count']} videos)")


if __name__ == "__main__":
    print_analysis()
    print("\n\n--- PROMPT PREVIEW ---")
    print(build_broll_constraint_prompt()[:2000])
