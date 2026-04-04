"""
OST Pattern Mining — Content Engine v2 Improvement #3
=====================================================
Mines on-screen text (OST) patterns from the video_onscreen_text table
and provides data-driven constraints for OST generation.

Data source: 182 OST entries from 122-video database
Key finding: Persistent OST outperforms non-persistent by 60%
             (avg 123K vs 77K engagement, 2x shares)

OST has three distinct roles in a TikTok:
  1. HOOK OST (t=0): Reinforces or replaces the spoken hook
  2. NARRATIVE OST (mid): Guides the viewer through the story
  3. CTA OST (late): Drives action (link, shop, comment)

This module decomposes top-performing OST into structural templates
and provides constraints for the edit guide's onscreen_text sections.
"""

from supabase import create_client
from collections import defaultdict

SUPABASE_URL = "https://owklfaoaxdrggmbtcwpn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93a2xmYW9heGRyZ2dtYnRjd3BuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NDQyNjcsImV4cCI6MjA4OTAyMDI2N30.EQkJzeS4MYG4QO6aH9c_zbF7BNuH_bKwZIKQpTXvw1Y"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── OST Templates (decomposed from top performers) ────────────────────────
#
# Each template has:
#   - pattern: Structural template with [SLOTS]
#   - position: Where in the video (hook / narrative / cta)
#   - persistent: Whether it should stay on screen
#   - avg_engagement: From source videos
#   - source_examples: Real OST from the database
#   - when_to_use: Which content angles pair well

OST_TEMPLATES = [
    # ── HOOK POSITION (t=0) ──
    {
        "id": "OST1",
        "name": "The Emoji-Led Statement",
        "pattern": "[EMOJI_CLUSTER] [SHORT_CLAIM_OR_VIBE]",
        "position": "hook",
        "persistent": True,
        "avg_engagement": 312938,
        "source_examples": [
            "😍✨💖 ignored my husband and got the pink comforter anyways",
            "✨🧼🫧 This and never being bent over to clean again",
            "🧽🧼 This scrub brush >>>",
        ],
        "when_to_use": ["lifestyle_aspiration", "shock_curiosity", "social_proof"],
    },
    {
        "id": "OST2",
        "name": "The Comparison/Versus",
        "pattern": "[THING_A] vs. [THING_B] [EMOJI]",
        "position": "hook",
        "persistent": False,
        "avg_engagement": 495763,
        "source_examples": [
            "Fake brush vs. Real brush 😳",
            "Ordinary style / Upgraded model",
            "FULL PRICE ❌ / 70% OFF SALE 🤯",
        ],
        "when_to_use": ["before_after", "value_comparison", "shock_curiosity"],
    },
    {
        "id": "OST3",
        "name": "The Warning/Don't",
        "pattern": "[WARNING_PHRASE] [EMOJI]",
        "position": "hook",
        "persistent": False,
        "avg_engagement": 183397,
        "source_examples": [
            "Don't tell you're landlord about these.. 💡🏠",
            "STOP PAYING CLOSE TO A GRAND ON A WAGON",
            "DO NOT GET THIS ❗",
            "Don't REPLACE your patio cushions just cover them",
            "STOP DONT throw away your cushions just grab these!",
            "Please Be Careful - Scam Alert",
        ],
        "when_to_use": ["fear_urgency", "problem_solution", "warning"],
    },
    {
        "id": "OST4",
        "name": "The Obvious Difference",
        "pattern": "[REACTION_TO_VISUAL] [EMOJI]",
        "position": "hook",
        "persistent": False,
        "avg_engagement": 229206,
        "source_examples": [
            "What an obvious difference 😳",
            "How is this not a foundation?!?!",
            "BOtox in a stick?!",
            "I bet you didn't know this existed 😳",
        ],
        "when_to_use": ["before_after", "shock_curiosity"],
    },
    {
        "id": "OST5",
        "name": "The Scarcity/FOMO",
        "pattern": "[URGENCY_ABOUT_AVAILABILITY] [EMOJI]",
        "position": "hook",
        "persistent": False,
        "avg_engagement": 43846,
        "source_examples": [
            "This sold out in minutes when it launched 🥲",
            "Don't be mad at me if you wait and this is sold out 🥺",
            "I have to show you this before it sells out again🤩🤩🤩",
            "Not to be dramatic but if you don't already have these, it might be too late 😅",
            "Sorry in advance if you miss it!",
        ],
        "when_to_use": ["fear_urgency", "social_proof"],
    },
    {
        "id": "OST6",
        "name": "The Problem Callout",
        "pattern": "[PROBLEM_IN_CAPS]",
        "position": "hook",
        "persistent": True,
        "avg_engagement": 50954,
        "source_examples": [
            "NECK LINES?",
            "DOUBLE CHIN",
            "SAGGY NECK",
            "Pets making a mess everywhere?",
            "MOSQUITO SEASON IS ABOUT TO START AGAIN... 🦟😩",
        ],
        "when_to_use": ["problem_solution", "fear_urgency"],
    },
    # ── NARRATIVE POSITION (mid-video) ──
    {
        "id": "OST7",
        "name": "The Educational Color/Label",
        "pattern": "[COLOR/LABEL] = [WHAT_IT_MEANS]",
        "position": "narrative",
        "persistent": False,
        "avg_engagement": 637026,
        "source_examples": [
            "Black = Industrial & Chemical Pollutants",
            "Dark Brown = Metabolic Waste",
            "Light Brown = Heavy Metals",
            "Green = Air & Water Pollutants",
        ],
        "when_to_use": ["educational", "shock_curiosity", "before_after"],
    },
    {
        "id": "OST8",
        "name": "The Before/After Label",
        "pattern": "BEFORE: [STATE] → AFTER: [STATE]",
        "position": "narrative",
        "persistent": False,
        "avg_engagement": 229206,
        "source_examples": [
            "NO CUTS, NO EDITS",
            "No cuts... full video",
        ],
        "when_to_use": ["before_after", "shock_curiosity"],
    },
    # ── CTA POSITION (late) ──
    {
        "id": "OST9",
        "name": "The Soft CTA",
        "pattern": "[CASUAL_LINK_REFERENCE] [EMOJI]",
        "position": "cta",
        "persistent": False,
        "avg_engagement": 87888,
        "source_examples": [
            "I LINKED THE",
            "Get it on tiktok shop below if they have any left ❤️",
            "Be sure to get these before they sell out again!! 💛❤️",
            "Link below ⬇️",
        ],
        "when_to_use": ["_all"],  # always applicable
    },
]


def get_ost_stats():
    """
    Query the database for OST performance statistics.
    Returns position-level and type-level breakdowns.
    """
    rows = supabase.table("video_onscreen_text").select(
        "text_content, text_type, timestamp_seconds, is_persistent, video_id"
    ).execute().data

    # Get video engagement data
    videos = supabase.table("tiktok_videos").select(
        "id, likes, shares, comments"
    ).execute().data
    vid_eng = {}
    for v in videos:
        eng = (v.get("shares", 0) or 0) * 3 + (v.get("comments", 0) or 0) * 2 + (v.get("likes", 0) or 0)
        vid_eng[v["id"]] = eng

    stats = {
        "total_entries": len(rows),
        "by_position": defaultdict(lambda: {"count": 0, "total_eng": 0, "entries": []}),
        "by_type": defaultdict(lambda: {"count": 0, "total_eng": 0}),
        "persistent_vs_not": {"persistent": {"count": 0, "total_eng": 0}, "transient": {"count": 0, "total_eng": 0}},
    }

    for r in rows:
        ts = float(r.get("timestamp_seconds", 0) or 0)
        text_type = r.get("text_type", "unknown")
        persistent = r.get("is_persistent", False)
        eng = vid_eng.get(r.get("video_id"), 0)

        # Position
        if ts == 0:
            pos = "hook"
        elif ts <= 10:
            pos = "early"
        elif ts <= 20:
            pos = "mid"
        else:
            pos = "late"

        stats["by_position"][pos]["count"] += 1
        stats["by_position"][pos]["total_eng"] += eng
        stats["by_type"][text_type]["count"] += 1
        stats["by_type"][text_type]["total_eng"] += eng

        bucket = "persistent" if persistent else "transient"
        stats["persistent_vs_not"][bucket]["count"] += 1
        stats["persistent_vs_not"][bucket]["total_eng"] += eng

    # Compute averages
    for pos_data in stats["by_position"].values():
        if pos_data["count"] > 0:
            pos_data["avg_eng"] = round(pos_data["total_eng"] / pos_data["count"])
    for type_data in stats["by_type"].values():
        if type_data["count"] > 0:
            type_data["avg_eng"] = round(type_data["total_eng"] / type_data["count"])
    for bucket_data in stats["persistent_vs_not"].values():
        if bucket_data["count"] > 0:
            bucket_data["avg_eng"] = round(bucket_data["total_eng"] / bucket_data["count"])

    return stats


def get_templates_for_position(position):
    """Return OST templates for a given position (hook/narrative/cta)."""
    return sorted(
        [t for t in OST_TEMPLATES if t["position"] == position],
        key=lambda x: x["avg_engagement"],
        reverse=True,
    )


def get_templates_for_angle(content_angle):
    """Return OST templates that pair well with a content angle."""
    return sorted(
        [t for t in OST_TEMPLATES if content_angle in t["when_to_use"] or "_all" in t["when_to_use"]],
        key=lambda x: x["avg_engagement"],
        reverse=True,
    )


def build_ost_constraint_prompt(content_angles=None):
    """
    Build the prompt section that provides OST templates as constraints
    for edit guide generation.

    Args:
        content_angles: Optional list of content angles being used.

    Returns:
        String to inject into the script/edit generation prompt.
    """
    stats = get_ost_stats()
    p_data = stats["persistent_vs_not"]

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("ON-SCREEN TEXT (OST) REQUIREMENTS — DATA-DRIVEN")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Analysis of {stats['total_entries']} OST entries from our video database.")
    lines.append("")

    # Key insight: persistence
    p_avg = p_data["persistent"].get("avg_eng", 0)
    t_avg = p_data["transient"].get("avg_eng", 0)
    lines.append("CRITICAL FINDING — PERSISTENT OST:")
    lines.append(f"  Persistent (stays on screen): avg {p_avg:,} engagement")
    lines.append(f"  Transient (appears/disappears): avg {t_avg:,} engagement")
    if p_avg > t_avg:
        pct = round((p_avg - t_avg) / t_avg * 100) if t_avg > 0 else 0
        lines.append(f"  → Persistent OST outperforms by {pct}%.")
        lines.append(f"  → PREFER persistent text for hook-position OST.")
    lines.append("")

    lines.append("OST STRUCTURAL TEMPLATES:")
    lines.append("=" * 50)
    lines.append("")

    # Group by position
    for position in ["hook", "narrative", "cta"]:
        templates = get_templates_for_position(position)
        if not templates:
            continue

        lines.append(f"── {position.upper()} POSITION ──")
        lines.append("")

        for t in templates:
            persistent_tag = " [PERSISTENT]" if t["persistent"] else ""
            lines.append(f"  [{t['id']}] {t['name']}{persistent_tag}")
            lines.append(f"  Avg Engagement: {t['avg_engagement']:,}")
            lines.append(f"  Pattern: \"{t['pattern']}\"")
            lines.append(f"  Examples:")
            for ex in t["source_examples"][:3]:
                lines.append(f"    → \"{ex}\"")
            lines.append("")

    lines.append("-" * 50)
    lines.append("DATA-DRIVEN RULES FOR OST GENERATION:")
    lines.append("")
    lines.append("CRITICAL INSIGHT: 101 of 122 videos (83%) use just ONE OST entry —")
    lines.append("a single text at the hook position (t=0). That is the dominant pattern.")
    lines.append("Multi-card OST sequences are the exception, not the rule.")
    lines.append("")
    lines.append("1. HERO VIDEOS (talking head):")
    lines.append("   - ONE hook-position OST is the proven pattern (83% of top videos)")
    lines.append("   - Additional mid/late OST is OPTIONAL — only when it genuinely")
    lines.append("     adds value (e.g., a before/after label, a price callout)")
    lines.append("   - Don't add OST just to fill space. Less is more.")
    lines.append("")
    lines.append("2. B-ROLL / NO-VOICE VIDEOS:")
    lines.append("   - These need MORE OST because text carries the narrative")
    lines.append("   - 3-5 text cards is appropriate for no-voice b-roll")
    lines.append("   - Always end with a CTA card (link below)")
    lines.append("")
    lines.append("3. OST must COMPLEMENT the spoken words, not duplicate them.")
    lines.append("   - If the hook is spoken, OST should add context/emotion")
    lines.append("   - If the video is no-voice, OST carries the entire narrative")
    lines.append("")
    lines.append("4. Use emojis strategically — 1-3 per text card max.")
    lines.append("")
    lines.append("5. CAPS for emphasis on 1-2 key words max. Full caps only")
    lines.append("   for very short phrases (≤4 words).")
    lines.append("")

    return "\n".join(lines)


def print_ost_report():
    """Print a formatted OST analysis report."""
    stats = get_ost_stats()

    print(f"\n{'=' * 60}")
    print(f"  OST PATTERN MINING REPORT")
    print(f"{'=' * 60}")
    print(f"  Total OST entries: {stats['total_entries']}")

    print(f"\n  BY POSITION:")
    for pos in ["hook", "early", "mid", "late"]:
        d = stats["by_position"].get(pos, {"count": 0, "avg_eng": 0})
        if d["count"] > 0:
            print(f"    {pos:>6}: {d['count']:3d} entries  |  avg {d.get('avg_eng', 0):>8,} engagement")

    print(f"\n  BY TYPE (top 10):")
    sorted_types = sorted(stats["by_type"].items(), key=lambda x: x[1].get("avg_eng", 0), reverse=True)
    for text_type, d in sorted_types[:10]:
        print(f"    {text_type:<25}: {d['count']:3d} entries  |  avg {d.get('avg_eng', 0):>8,}")

    print(f"\n  PERSISTENT vs TRANSIENT:")
    for bucket, d in stats["persistent_vs_not"].items():
        if d["count"] > 0:
            print(f"    {bucket:>10}: {d['count']:3d} entries  |  avg {d.get('avg_eng', 0):>8,}")

    print(f"\n  TEMPLATES: {len(OST_TEMPLATES)}")
    for t in sorted(OST_TEMPLATES, key=lambda x: x["avg_engagement"], reverse=True):
        p_tag = " [P]" if t["persistent"] else ""
        print(f"    [{t['id']}] {t['name']:<30} | {t['position']:<9} | avg {t['avg_engagement']:>8,}{p_tag}")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    print_ost_report()
    print("\n--- OST Constraint Prompt Preview ---")
    prompt = build_ost_constraint_prompt()
    print(prompt[:2000])
    print("...")
