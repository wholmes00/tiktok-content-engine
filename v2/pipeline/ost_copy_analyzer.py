"""
OST Copy Pattern Analyzer — Content Engine v2
==============================================
Mines the ACTUAL LANGUAGE PATTERNS in on-screen text from top-performing
remix videos and provides data-driven copy guidance.

This is distinct from ost_patterns.py which handles STRUCTURAL templates
(timing, persistence, card count). This module analyzes WHAT THE TEXT SAYS —
the language strategies, phrasing patterns, and emotional triggers that
correlate with high engagement.

Data source: video_onscreen_text joined with tiktok_videos
Focus: remix formats (voiceover_broll, text_only, borrowed_voiceover)
"""

import re
from collections import defaultdict

try:
    from v2.pipeline.db import supabase
except ImportError:
    from db import supabase


# ─── Copy Pattern Definitions ────────────────────────────────────────────────
#
# These patterns were identified through manual analysis of the top-performing
# OST entries in our database. Each pattern represents a distinct language
# strategy used in on-screen text.

COPY_PATTERNS = {
    "emoji_led_statement": {
        "name": "Emoji-Led Bold Statement",
        "description": "Opens with 2-4 emojis, followed by a personal/bold claim",
        "regex": r"^[\U0001F600-\U0001FAFF\u2600-\u27BF\u200d\uFE0F\u2764\u2728✨🧼🫧😍💖❤️🧽🥰🥹💕✈️☁️☀️📝💅🍓🍋🦟😩💡🏠😏]{1,}[\s]",
        "examples": [],  # populated dynamically from DB
        "avg_engagement": 0,
        "count": 0,
    },
    "question_hook": {
        "name": "Provocative Question",
        "description": "Asks a question that creates curiosity or implies the viewer is missing out",
        "regex": r"\?[\s\U0001F600-\U0001FAFF\u2600-\u27BF]*$",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
    "warning_dont": {
        "name": "Warning / Don't / Stop",
        "description": "Opens with a warning, prohibition, or urgent command",
        "regex": r"(?i)^(don'?t|stop|warning|be careful|please be careful|never|do not)",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
    "defiant_personal": {
        "name": "Defiant Personal Statement",
        "description": "First-person bold/rebellious claim — 'I refuse...', 'I ignored...'",
        "regex": r"(?i)^i (refuse|ignored|don'?t care|decided|said no|finally|just)",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
    "aspirational_scene": {
        "name": "Aspirational Scene/Vibe",
        "description": "Paints a cozy, dreamy, or aspirational scene in few words",
        "regex": r"(?i)(thunderstorm|cozy|dreamy|aesthetic|vibes|marshmallow|marshmellow|rainy|morning|sunset|golden hour)",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
    "curiosity_gap": {
        "name": "Curiosity Gap",
        "description": "Creates information gap — 'I bet you didn't know...', 'This exists...'",
        "regex": r"(?i)(i bet you|you didn'?t know|you won'?t believe|did you know|wait until you see|have you seen)",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
    "caps_callout": {
        "name": "ALL-CAPS Problem/Product Callout",
        "description": "Ultra-short text, mostly or fully capitalized, names a problem or product",
        "regex": r"^[A-Z\s\?!]{4,}$",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
    "scarcity_fomo": {
        "name": "Scarcity / FOMO / Selling Out",
        "description": "Creates urgency about availability — 'before it sells out', 'too late'",
        "regex": r"(?i)(sell[s]? out|sold out|too late|before (it|they)|limited|while (they|it)|hurry|last chance|don'?t wait|mad at me)",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
    "this_product": {
        "name": "The 'This [product]' Opener",
        "description": "Starts with 'This [noun]' or 'THE [noun]' — simple product spotlight",
        "regex": r"(?i)^(this|the|these)\s+\w+",
        "examples": [],
        "avg_engagement": 0,
        "count": 0,
    },
}


def get_remix_ost_data():
    """
    Query the database for all remix-format OST entries with performance data.
    Excludes 'none' type entries (no creator-added text).
    Returns list of dicts with text, type, engagement, format, angle.
    """
    rows = supabase.table("video_onscreen_text").select(
        "text_content, text_type, timestamp_seconds, is_persistent, video_id"
    ).execute().data

    videos = supabase.table("tiktok_videos").select(
        "id, likes, shares, comments, content_type, content_angle"
    ).execute().data

    vid_map = {}
    for v in videos:
        eng = (
            (v.get("shares", 0) or 0) * 3
            + (v.get("comments", 0) or 0) * 2
            + (v.get("likes", 0) or 0)
        )
        vid_map[v["id"]] = {
            "engagement": eng,
            "content_type": v.get("content_type"),
            "content_angle": v.get("content_angle"),
        }

    remix_types = {"voiceover_broll", "text_only", "borrowed_voiceover"}
    results = []

    for r in rows:
        vid_info = vid_map.get(r.get("video_id"), {})
        ct = vid_info.get("content_type")
        if ct not in remix_types:
            continue
        # Skip "none" type entries (no actual creator text)
        if r.get("text_type") == "none":
            continue

        results.append({
            "text": r.get("text_content", ""),
            "text_type": r.get("text_type", ""),
            "timestamp": float(r.get("timestamp_seconds", 0) or 0),
            "persistent": r.get("is_persistent", False),
            "engagement": vid_info.get("engagement", 0),
            "content_type": ct,
            "content_angle": vid_info.get("content_angle", ""),
        })

    return sorted(results, key=lambda x: x["engagement"], reverse=True)


def classify_ost_text(text):
    """
    Classify a piece of OST text into one or more copy patterns.
    Returns list of pattern IDs that match.
    """
    matches = []
    for pid, pdef in COPY_PATTERNS.items():
        try:
            if re.search(pdef["regex"], text):
                matches.append(pid)
        except re.error:
            continue
    return matches if matches else ["unclassified"]


def analyze_copy_patterns():
    """
    Analyze all remix OST entries and classify them by copy pattern.
    Returns enriched COPY_PATTERNS dict with real examples and engagement data,
    plus a summary of patterns by content angle.
    """
    data = get_remix_ost_data()

    # Reset pattern stats
    for p in COPY_PATTERNS.values():
        p["examples"] = []
        p["avg_engagement"] = 0
        p["count"] = 0

    unclassified = []
    angle_patterns = defaultdict(lambda: defaultdict(list))

    for entry in data:
        text = entry["text"]
        patterns = classify_ost_text(text)

        for pid in patterns:
            if pid == "unclassified":
                unclassified.append(entry)
                continue

            COPY_PATTERNS[pid]["count"] += 1
            COPY_PATTERNS[pid]["examples"].append({
                "text": text,
                "engagement": entry["engagement"],
                "content_type": entry["content_type"],
                "content_angle": entry["content_angle"],
                "persistent": entry["persistent"],
            })

            # Track by angle
            angle_patterns[entry["content_angle"]][pid].append(entry["engagement"])

    # Compute averages and sort examples by engagement
    for p in COPY_PATTERNS.values():
        if p["count"] > 0:
            total_eng = sum(ex["engagement"] for ex in p["examples"])
            p["avg_engagement"] = round(total_eng / p["count"])
            p["examples"] = sorted(
                p["examples"], key=lambda x: x["engagement"], reverse=True
            )

    # Compute angle-level pattern performance
    angle_summary = {}
    for angle, patterns in angle_patterns.items():
        angle_summary[angle] = {}
        for pid, engs in patterns.items():
            angle_summary[angle][pid] = {
                "count": len(engs),
                "avg_engagement": round(sum(engs) / len(engs)) if engs else 0,
            }

    return {
        "patterns": COPY_PATTERNS,
        "angle_summary": angle_summary,
        "unclassified": unclassified,
        "total_analyzed": len(data),
    }


def build_ost_copy_constraint_prompt(content_angles=None):
    """
    Build the prompt section that provides OST copy pattern guidance
    based on database analysis.

    Args:
        content_angles: Optional list of content angles being used,
                       to prioritize relevant patterns.

    Returns:
        String to inject into the script/edit generation prompt.
    """
    analysis = analyze_copy_patterns()
    patterns = analysis["patterns"]

    # Sort patterns by avg engagement (highest first)
    ranked = sorted(
        [(pid, p) for pid, p in patterns.items() if p["count"] > 0],
        key=lambda x: x[1]["avg_engagement"],
        reverse=True,
    )

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("ON-SCREEN TEXT COPY PATTERNS — DATA-DRIVEN")
    lines.append("(What the text actually SAYS — language that works)")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Analysis of {analysis['total_analyzed']} remix OST entries from our database.")
    lines.append("These are the LANGUAGE PATTERNS that correlate with high engagement.")
    lines.append("Use these patterns as models for the on-screen text you write.")
    lines.append("")

    lines.append("RANKED COPY PATTERNS (by avg engagement):")
    lines.append("-" * 50)
    lines.append("")

    for pid, p in ranked:
        lines.append(f"  [{pid.upper()}] {p['name']}")
        lines.append(f"  {p['description']}")
        lines.append(f"  Database: {p['count']} entries | avg {p['avg_engagement']:,} engagement")

        # Show top 3 real examples
        top_examples = p["examples"][:3]
        if top_examples:
            lines.append(f"  Proven examples from top performers:")
            for ex in top_examples:
                eng_k = f"{ex['engagement'] // 1000}K"
                lines.append(f'    → "{ex["text"]}" ({eng_k} eng, {ex["content_type"]})')
        lines.append("")

    # Angle-specific recommendations
    if content_angles and analysis["angle_summary"]:
        lines.append("-" * 50)
        lines.append("ANGLE-SPECIFIC PATTERN RECOMMENDATIONS:")
        lines.append("")
        for angle in content_angles:
            angle_data = analysis["angle_summary"].get(angle, {})
            if angle_data:
                angle_ranked = sorted(
                    angle_data.items(),
                    key=lambda x: x[1]["avg_engagement"],
                    reverse=True,
                )
                top_patterns = [
                    f"{pid} ({d['avg_engagement']:,} avg eng)"
                    for pid, d in angle_ranked[:3]
                ]
                lines.append(f"  {angle}: best patterns → {', '.join(top_patterns)}")
        lines.append("")

    # Key copy rules derived from the data
    lines.append("-" * 50)
    lines.append("KEY COPY RULES (derived from pattern analysis):")
    lines.append("")
    lines.append("1. LEAD WITH EMOTION OR CURIOSITY — the first 3-4 words must stop the scroll.")
    lines.append("   The highest-performing OST text creates an immediate emotional reaction")
    lines.append("   or information gap. Never start with generic product descriptions.")
    lines.append("")
    lines.append("2. KEEP IT SHORT — most top-performing OST is 5-12 words.")
    lines.append("   The text must be readable in under 2 seconds at a glance.")
    lines.append("   If it takes more than one line on screen, it's too long.")
    lines.append("")
    lines.append("3. MATCH THE PATTERN TO THE ANGLE:")
    lines.append("   - lifestyle_aspiration → aspirational_scene, emoji_led_statement")
    lines.append("   - problem_solution → warning_dont, defiant_personal, caps_callout")
    lines.append("   - visual_demo → curiosity_gap, question_hook, this_product")
    lines.append("   - authentic_review → defiant_personal, emoji_led_statement")
    lines.append("")
    lines.append("4. EMOJIS ARE STRATEGIC, NOT DECORATIVE.")
    lines.append("   Leading emojis (before text) correlate with highest engagement.")
    lines.append("   Use 1-3 emojis max. They set the emotional tone before words do.")
    lines.append("")
    lines.append("5. WRITE FOR THE SCROLL, NOT THE WATCH.")
    lines.append("   The OST must make someone STOP scrolling. It's not a caption —")
    lines.append("   it's a billboard at 60mph. Bold, punchy, emotionally charged.")
    lines.append("")

    return "\n".join(lines)


def print_copy_analysis_report():
    """Print a formatted report of OST copy pattern analysis."""
    analysis = analyze_copy_patterns()
    patterns = analysis["patterns"]

    ranked = sorted(
        [(pid, p) for pid, p in patterns.items() if p["count"] > 0],
        key=lambda x: x[1]["avg_engagement"],
        reverse=True,
    )

    print(f"\n{'=' * 60}")
    print(f"  OST COPY PATTERN ANALYSIS REPORT")
    print(f"{'=' * 60}")
    print(f"  Total remix OST entries analyzed: {analysis['total_analyzed']}")
    print(f"  Unclassified entries: {len(analysis['unclassified'])}")
    print()

    for pid, p in ranked:
        print(f"  [{pid}]")
        print(f"    {p['name']}")
        print(f"    Count: {p['count']} | Avg Engagement: {p['avg_engagement']:,}")
        for ex in p["examples"][:2]:
            print(f'    → "{ex["text"]}" ({ex["engagement"]:,})')
        print()

    if analysis["angle_summary"]:
        print(f"  BY CONTENT ANGLE:")
        for angle, pats in sorted(analysis["angle_summary"].items()):
            top = sorted(pats.items(), key=lambda x: x[1]["avg_engagement"], reverse=True)
            top_str = ", ".join(f"{pid}({d['avg_engagement']:,})" for pid, d in top[:3])
            print(f"    {angle:25s}: {top_str}")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    print_copy_analysis_report()
    print("\n--- Copy Constraint Prompt Preview ---")
    prompt = build_ost_copy_constraint_prompt(
        content_angles=["lifestyle_aspiration", "visual_demo", "problem_solution", "authentic_review"]
    )
    print(prompt[:3000])
    print("...")
