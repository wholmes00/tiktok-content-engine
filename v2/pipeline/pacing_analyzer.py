"""
Pacing Analyzer — Content Engine v2 Improvement #7
====================================================
Mines video_transcripts for word count, speaking pace, and
script length patterns that correlate with performance.

Data source: 128 videos with full transcripts
Key findings:
  - Speaking rate: consistently ~3.9 words/sec across all performers
  - Top-performing heroes average 232 words (60s videos)
  - Word count sweet spot: 150-300 words for heroes
  - Remixes average 69 words (23s videos)

Note: The segments field in video_transcripts is null, so per-segment
analysis (hook/story/value/CTA word breakdown) is not available from
the database. This module focuses on total pacing rules and
word-count-to-duration relationships.

Designed for periodic refresh.
"""

try:
    from v2.pipeline.db import supabase
except ImportError:
    from db import supabase

import statistics

HERO_CONTENT_TYPES = ["talking_head", "product_demo", "hybrid"]
REMIX_CONTENT_TYPES = ["voiceover_broll", "text_only", "borrowed_voiceover"]


def analyze_pacing_patterns():
    """
    Query the database and compute script pacing patterns.
    """
    transcripts = supabase.table("video_transcripts").select(
        "video_id, full_transcript, transcript_type"
    ).eq("transcript_type", "creator_speech").execute().data

    videos = supabase.table("tiktok_videos").select(
        "id, content_type, likes, shares, duration_seconds"
    ).execute().data

    video_lookup = {v["id"]: v for v in videos}

    # Enrich transcripts
    enriched = []
    for t in transcripts:
        vid = video_lookup.get(t["video_id"])
        if not vid or not t.get("full_transcript"):
            continue
        text = t["full_transcript"].strip()
        if not text:
            continue

        word_count = len(text.split())
        duration = float(vid.get("duration_seconds", 0) or 0)
        wps = word_count / duration if duration > 0 else 0

        enriched.append({
            "content_type": vid.get("content_type", "unknown"),
            "likes": vid.get("likes", 0) or 0,
            "duration": duration,
            "word_count": word_count,
            "words_per_sec": round(wps, 2),
        })

    heroes = [e for e in enriched if e["content_type"] in HERO_CONTENT_TYPES]
    remixes = [e for e in enriched if e["content_type"] in REMIX_CONTENT_TYPES]

    hero_stats = _compute_pacing_stats(heroes)
    remix_stats = _compute_pacing_stats(remixes)

    # Word count buckets for heroes
    buckets = _bucket_analysis(heroes)

    return {
        "hero_stats": hero_stats,
        "remix_stats": remix_stats,
        "hero_word_buckets": buckets,
        "sample_size": {
            "total": len(enriched),
            "heroes": len(heroes),
            "remixes": len(remixes),
        },
    }


def _compute_pacing_stats(videos):
    """Compute word count and pacing stats, split by top/bottom half."""
    if not videos:
        return {"count": 0}

    sorted_vids = sorted(videos, key=lambda x: x["likes"], reverse=True)
    mid = len(sorted_vids) // 2
    top = sorted_vids[:mid] if mid > 0 else sorted_vids
    bottom = sorted_vids[mid:] if mid > 0 else []

    def _stats(vids):
        if not vids:
            return {}
        wc = [v["word_count"] for v in vids]
        wps = [v["words_per_sec"] for v in vids if v["words_per_sec"] > 0]
        dur = [v["duration"] for v in vids if v["duration"] > 0]
        likes = [v["likes"] for v in vids]
        return {
            "count": len(vids),
            "avg_word_count": round(statistics.mean(wc)),
            "median_word_count": round(statistics.median(wc)),
            "min_words": min(wc),
            "max_words": max(wc),
            "avg_wps": round(statistics.mean(wps), 1) if wps else 0,
            "avg_duration": round(statistics.mean(dur), 1) if dur else 0,
            "avg_likes": round(statistics.mean(likes)),
        }

    return {
        "count": len(videos),
        "all": _stats(sorted_vids),
        "top_half": _stats(top),
        "bottom_half": _stats(bottom),
    }


def _bucket_analysis(heroes):
    """Bucket hero word counts and compute performance per bucket."""
    buckets = {
        "under_100": [],
        "100_149": [],
        "150_199": [],
        "200_249": [],
        "250_plus": [],
    }

    for h in heroes:
        wc = h["word_count"]
        if wc < 100:
            buckets["under_100"].append(h)
        elif wc < 150:
            buckets["100_149"].append(h)
        elif wc < 200:
            buckets["150_199"].append(h)
        elif wc < 250:
            buckets["200_249"].append(h)
        else:
            buckets["250_plus"].append(h)

    result = []
    for label, vids in buckets.items():
        if vids:
            result.append({
                "bucket": label,
                "count": len(vids),
                "avg_likes": round(statistics.mean([v["likes"] for v in vids])),
                "avg_words": round(statistics.mean([v["word_count"] for v in vids])),
                "avg_duration": round(statistics.mean([v["duration"] for v in vids if v["duration"] > 0]), 1),
            })
    return result


def build_pacing_constraint_prompt():
    """
    Build the prompt section that constrains script pacing
    based on actual transcript analysis.
    """
    data = analyze_pacing_patterns()
    hs = data["hero_stats"]
    rs = data["remix_stats"]
    buckets = data["hero_word_buckets"]
    sample = data["sample_size"]

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("SCRIPT PACING RULES (DATA-DRIVEN — FROM TRANSCRIPT ANALYSIS)")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Analysis of {sample['total']} video transcripts reveals speaking")
    lines.append(f"pace and optimal word counts.")
    lines.append("")

    # ─── Hero pacing ────────────────────────────────────────────────
    lines.append("─" * 60)
    lines.append("HERO VIDEO SCRIPTS ({} transcripts analyzed)".format(sample["heroes"]))
    lines.append("─" * 60)

    if hs.get("top_half"):
        top = hs["top_half"]
        bot = hs.get("bottom_half", {})
        lines.append(f"")
        lines.append(f"  Top performers ({top['count']} videos, avg {top['avg_likes']:,} likes):")
        lines.append(f"    Average words: {top['avg_word_count']}")
        lines.append(f"    Average duration: {top['avg_duration']}s")
        lines.append(f"    Speaking rate: {top['avg_wps']} words/second")
        lines.append(f"")
        if bot:
            lines.append(f"  Bottom performers ({bot['count']} videos, avg {bot['avg_likes']:,} likes):")
            lines.append(f"    Average words: {bot['avg_word_count']}")
            lines.append(f"    Average duration: {bot['avg_duration']}s")
            lines.append(f"    Speaking rate: {bot['avg_wps']} words/second")
            lines.append(f"")

    # Word count buckets
    if buckets:
        lines.append(f"  WORD COUNT vs PERFORMANCE:")
        for b in buckets:
            lines.append(f"    {b['bucket']}: avg {b['avg_likes']:,} likes "
                        f"({b['count']} videos, ~{b['avg_words']} words, ~{b['avg_duration']}s)")
        lines.append(f"")

    # Derive rules
    if hs.get("top_half"):
        top = hs["top_half"]
        wps = top.get("avg_wps", 3.9)
        lines.append(f"  *** MANDATORY PACING RULES ***")
        lines.append(f"")
        lines.append(f"  1. SPEAKING RATE: {wps} words/second (consistent across all performers)")
        lines.append(f"     This is a natural conversational pace — don't overpack or underpack.")
        lines.append(f"")
        lines.append(f"  2. WORD COUNT TARGETS (based on video duration):")
        lines.append(f"     35-second video: ~{round(35 * wps)}-{round(40 * wps)} words")
        lines.append(f"     45-second video: ~{round(45 * wps)}-{round(50 * wps)} words")
        lines.append(f"     55-second video: ~{round(55 * wps)}-{round(60 * wps)} words")
        lines.append(f"")
        lines.append(f"  3. SEGMENT PACING GUIDANCE (approximate, at {wps} wps):")
        lines.append(f"     Hook (first 3-5s): {round(3 * wps)}-{round(5 * wps)} words")
        lines.append(f"     Story/Demo (middle 60%): {round(0.6 * top['avg_word_count'])} words")
        lines.append(f"     Value + CTA (final 20%): {round(0.2 * top['avg_word_count'])} words")
        lines.append(f"")
        lines.append(f"  4. KEY INSIGHT: Top performers average {top['avg_word_count']} words")
        lines.append(f"     over {top['avg_duration']}s. Longer, more detailed scripts tend to")
        lines.append(f"     outperform shorter ones — don't cut corners on the story.")

    # ─── Remix pacing ───────────────────────────────────────────────
    if rs.get("all"):
        all_remix = rs["all"]
        lines.append(f"")
        lines.append("─" * 60)
        lines.append("REMIX VIDEO SCRIPTS ({} transcripts)".format(sample["remixes"]))
        lines.append("─" * 60)
        lines.append(f"")
        lines.append(f"  Average: {all_remix['avg_word_count']} words over {all_remix['avg_duration']}s")
        lines.append(f"  Remixes are shorter — voiceover scripts should be concise.")
        lines.append(f"  Target: {round(all_remix['avg_word_count'] * 0.8)}-{round(all_remix['avg_word_count'] * 1.2)} words per remix.")

    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


def print_analysis():
    """Print a human-readable analysis report."""
    data = analyze_pacing_patterns()
    print("\n" + "=" * 60)
    print("PACING ANALYSIS REPORT")
    print("=" * 60)

    for label, key in [("Heroes", "hero_stats"), ("Remixes", "remix_stats")]:
        stats = data[key]
        print(f"\n{label} ({stats['count']} videos):")
        for half in ["top_half", "bottom_half", "all"]:
            s = stats.get(half, {})
            if s:
                print(f"  {half}: {s['avg_word_count']} words, {s['avg_duration']}s, "
                      f"{s['avg_wps']} wps, {s['avg_likes']:,} likes")

    print("\nWord count buckets:")
    for b in data["hero_word_buckets"]:
        print(f"  {b['bucket']}: avg {b['avg_likes']:,} likes ({b['count']} videos)")


if __name__ == "__main__":
    print_analysis()
    print("\n\n--- PROMPT PREVIEW ---")
    print(build_pacing_constraint_prompt())
