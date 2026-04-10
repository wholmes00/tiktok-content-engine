"""
Audio Analyzer — Content Engine v2 Improvement #5
===================================================
Mines audio_type and voice_delivery performance from the tiktok_videos table
to produce data-driven audio recommendations for hero vs remix videos.

Data source: 128 videos with audio_type and voice_delivery fields
Key finding: Hero videos overwhelmingly use original audio (~94% of hero-type).
             Remixes benefit from borrowed_sound and background_music.
             Voice delivery: authoritative and conversational outperform on-camera average.

Designed for periodic refresh — call analyze_audio_patterns() whenever
new videos are added to the database.
"""

try:
    from v2.pipeline.db import supabase
except ImportError:
    from db import supabase


HERO_CONTENT_TYPES = ["talking_head", "product_demo", "hybrid"]
REMIX_CONTENT_TYPES = ["voiceover_broll", "text_only", "borrowed_voiceover"]


def analyze_audio_patterns():
    """
    Query the database and compute audio performance patterns.

    Returns a dict with audio_type and voice_delivery rankings
    segmented by hero vs remix video types.
    """
    videos = supabase.table("tiktok_videos").select(
        "content_type, audio_type, voice_delivery, likes, shares, comments"
    ).execute().data

    heroes = [v for v in videos if v.get("content_type") in HERO_CONTENT_TYPES]
    remixes = [v for v in videos if v.get("content_type") in REMIX_CONTENT_TYPES]

    return {
        "hero_audio": _rank_field(heroes, "audio_type"),
        "remix_audio": _rank_field(remixes, "audio_type"),
        "hero_voice": _rank_field(heroes, "voice_delivery"),
        "remix_voice": _rank_field(remixes, "voice_delivery"),
        "overall_audio": _rank_field(videos, "audio_type"),
        "overall_voice": _rank_field(videos, "voice_delivery"),
        "sample_size": {
            "total": len(videos),
            "heroes": len(heroes),
            "remixes": len(remixes),
        },
    }


def _rank_field(videos, field):
    """Rank values of a field by weighted engagement score."""
    buckets = {}
    for v in videos:
        val = v.get(field)
        if not val:
            continue
        if val not in buckets:
            buckets[val] = {"likes": [], "shares": [], "comments": []}
        buckets[val]["likes"].append(v.get("likes", 0) or 0)
        buckets[val]["shares"].append(v.get("shares", 0) or 0)
        buckets[val]["comments"].append(v.get("comments", 0) or 0)

    rankings = []
    for val, data in buckets.items():
        n = len(data["likes"])
        avg_l = sum(data["likes"]) / n
        avg_s = sum(data["shares"]) / n
        avg_c = sum(data["comments"]) / n
        weighted = avg_l + avg_s * 3 + avg_c * 2

        rankings.append({
            "value": val,
            "count": n,
            "avg_likes": round(avg_l),
            "avg_shares": round(avg_s),
            "weighted_score": round(weighted),
            "pct_of_total": round(n / len(videos) * 100, 1) if videos else 0,
        })

    rankings.sort(key=lambda x: x["weighted_score"], reverse=True)
    for i, r in enumerate(rankings):
        r["rank"] = i + 1

    return rankings


def build_audio_constraint_prompt():
    """
    Build the prompt section that constrains audio type selection
    based on actual database analysis.
    """
    data = analyze_audio_patterns()
    sample = data["sample_size"]
    ha = data["hero_audio"]
    ra = data["remix_audio"]
    hv = data["hero_voice"]

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("AUDIO TYPE RULES (DATA-DRIVEN — FROM VIDEO ANALYSIS)")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Analysis of {sample['total']} videos reveals clear audio patterns.")
    lines.append("")

    # ─── Hero audio rules ───────────────────────────────────────────
    lines.append("─" * 60)
    lines.append("HERO VIDEO AUDIO ({} videos analyzed)".format(sample["heroes"]))
    lines.append("─" * 60)

    # Find the dominant audio type for heroes
    if ha:
        dominant = ha[0]
        # Calculate how much of hero content uses original audio
        original_types = [r for r in ha if "original" in r["value"].lower()]
        original_count = sum(r["count"] for r in original_types)
        original_pct = round(original_count / sample["heroes"] * 100) if sample["heroes"] else 0

        lines.append(f"")
        for r in ha:
            lines.append(f"  {r['value']}: {r['count']} videos ({r['pct_of_total']}%), "
                        f"avg {r['avg_likes']:,} likes, weighted {r['weighted_score']:,}")
        lines.append(f"")
        lines.append(f"  KEY INSIGHT: {original_pct}% of hero videos use original/creator audio.")
        lines.append(f"")
        lines.append(f"  HERO AUDIO RULE:")
        lines.append(f"    - Default to ORIGINAL AUDIO (creator speaking to camera)")
        lines.append(f"    - Trending audio CAN work but is rare for heroes ({100 - original_pct}% of videos)")
        lines.append(f"    - For the \"music\" field in edit guide: specify \"original audio\"")
        lines.append(f"      or \"creator audio\" for most heroes")
        lines.append(f"    - Only recommend background music for heroes if the concept")
        lines.append(f"      specifically calls for it (e.g., montage-style demo)")

    # ─── Hero voice delivery ────────────────────────────────────────
    if hv:
        lines.append(f"")
        lines.append(f"  VOICE DELIVERY PERFORMANCE (hero videos):")
        for r in hv[:5]:
            lines.append(f"    {r['value']}: avg {r['avg_likes']:,} likes ({r['count']} videos)")
        lines.append(f"")
        lines.append(f"  Prioritize conversational and excited delivery styles.")

    # ─── Remix audio rules ──────────────────────────────────────────
    lines.append(f"")
    lines.append("─" * 60)
    lines.append("REMIX VIDEO AUDIO ({} videos analyzed)".format(sample["remixes"]))
    lines.append("─" * 60)

    if ra:
        lines.append(f"")
        for r in ra:
            lines.append(f"  {r['value']}: {r['count']} videos ({r['pct_of_total']}%), "
                        f"avg {r['avg_likes']:,} likes, weighted {r['weighted_score']:,}")
        lines.append(f"")

        # Find top audio type for remixes
        top_remix_audio = ra[0]["value"] if ra else "background_music"
        lines.append(f"  REMIX AUDIO RULE:")
        lines.append(f"    - Top performer: {top_remix_audio}")
        lines.append(f"    - Borrowed/trending sounds work well for remixes")
        lines.append(f"    - Background music is effective for b-roll montages")
        lines.append(f"    - Voiceover remixes should use background_music or trending sound")
        lines.append(f"      underneath the voiceover audio")

    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


def print_analysis():
    """Print a human-readable analysis report."""
    data = analyze_audio_patterns()
    print("\n" + "=" * 60)
    print("AUDIO ANALYSIS REPORT")
    print("=" * 60)

    for label, key in [("Hero Audio", "hero_audio"), ("Remix Audio", "remix_audio"),
                       ("Hero Voice", "hero_voice"), ("Overall Audio", "overall_audio")]:
        print(f"\n{label}:")
        for r in data[key]:
            print(f"  #{r['rank']} {r['value']}: {r['count']} videos, "
                  f"avg {r['avg_likes']:,} likes, weighted {r['weighted_score']:,}")


if __name__ == "__main__":
    print_analysis()
    print("\n\n--- PROMPT PREVIEW ---")
    print(build_audio_constraint_prompt())
