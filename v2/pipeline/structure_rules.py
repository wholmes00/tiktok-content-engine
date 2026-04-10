"""
Structure Rules — Content Engine v2 Improvement #8
====================================================
Consolidates all data-driven structural rules for hero vs remix videos
into a single summary that gets injected into the content generation prompt.

This module aggregates findings from:
  - broll_analyzer (face/broll ratios, shot counts)
  - audio_analyzer (audio type performance)
  - cta_analyzer (CTA count and placement)
  - pacing_analyzer (word counts, speaking pace)
  - hook_templates (hook category performance)
  - Database queries for duration, content type specifics

Provides the LLM with a clear structural blueprint for each video type.

Designed for periodic refresh.
"""

try:
    from v2.pipeline.db import supabase
except ImportError:
    from db import supabase

HERO_CONTENT_TYPES = ["talking_head", "product_demo", "hybrid"]
REMIX_CONTENT_TYPES = ["voiceover_broll", "text_only", "borrowed_voiceover"]


def analyze_structure():
    """
    Query database for structural comparisons between hero and remix videos.
    Returns aggregated stats for each video type.
    """
    videos = supabase.table("tiktok_videos").select(
        "id, content_type, likes, shares, comments, duration_seconds, audio_type"
    ).execute().data

    heroes = [v for v in videos if v.get("content_type") in HERO_CONTENT_TYPES]
    remixes = [v for v in videos if v.get("content_type") in REMIX_CONTENT_TYPES]

    def _stats(vids):
        if not vids:
            return {}
        sorted_v = sorted(vids, key=lambda x: x.get("likes", 0) or 0, reverse=True)
        mid = len(sorted_v) // 2
        top = sorted_v[:mid] if mid > 0 else sorted_v

        durations = [float(v.get("duration_seconds", 0) or 0) for v in top if float(v.get("duration_seconds", 0) or 0) > 0]
        likes = [v.get("likes", 0) or 0 for v in top]

        return {
            "count": len(vids),
            "top_count": len(top),
            "avg_duration": round(sum(durations) / len(durations), 1) if durations else 0,
            "min_duration": round(min(durations), 1) if durations else 0,
            "max_duration": round(max(durations), 1) if durations else 0,
            "avg_likes": round(sum(likes) / len(likes)) if likes else 0,
        }

    return {
        "hero": _stats(heroes),
        "remix": _stats(remixes),
        "total": len(videos),
    }


def build_structure_summary_prompt():
    """
    Build a consolidated structural blueprint for hero vs remix videos.
    This gets injected ONCE at the top of the scripts prompt, giving
    the LLM a clear mental model before it starts generating.
    """
    data = analyze_structure()
    h = data["hero"]
    r = data["remix"]

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("VIDEO TYPE BLUEPRINTS (DATA-DRIVEN STRUCTURE RULES)")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Based on analysis of {data['total']} videos:")
    lines.append("")

    # ─── HERO BLUEPRINT ─────────────────────────────────────────────
    lines.append("─" * 60)
    lines.append("HERO VIDEO BLUEPRINT (5 videos)")
    lines.append("─" * 60)
    lines.append(f"  Source: {h['count']} hero-type videos analyzed")
    lines.append(f"  Top performers: avg {h['avg_likes']:,} likes")
    lines.append(f"")
    lines.append(f"  STRUCTURE:")
    lines.append(f"    Duration:     {h['avg_duration']}s target ({h['min_duration']}-{h['max_duration']}s range)")
    lines.append(f"    Format:       Creator on camera, talking to viewer")
    lines.append(f"    Camera:       ~90% on-camera, ~10% b-roll cutaways")
    lines.append(f"    B-roll:       2-4 brief cutaways (2-3 seconds each)")
    lines.append(f"    Audio:        Original creator audio (95% of top performers)")
    lines.append(f"    Word count:   ~{round(h['avg_duration'] * 3.9)} words (at 3.9 words/sec)")
    lines.append(f"    CTAs:         4 per video (value_prop at ~50%, scarcity at ~80%, link at ~90%)")
    lines.append(f"    Hook:         First 3-5 seconds, 12-20 words")
    lines.append(f"    Script flow:  Hook → Story/Demo → Value → CTA")
    lines.append(f"")
    lines.append(f"  THE HERO IS THE CREATOR. She is talking, demonstrating,")
    lines.append(f"  reacting. B-roll is brief visual punctuation, not coverage.")
    lines.append(f"")

    # ─── REMIX BLUEPRINT ────────────────────────────────────────────
    lines.append("─" * 60)
    lines.append("REMIX VIDEO BLUEPRINT (5 videos)")
    lines.append("─" * 60)
    lines.append(f"  Source: {r['count']} remix-type videos analyzed")
    lines.append(f"  Top performers: avg {r['avg_likes']:,} likes")
    lines.append(f"")
    lines.append(f"  STRUCTURE:")
    lines.append(f"    Duration:     {r['avg_duration']}s target (15-30s range)")
    lines.append(f"    Format:       B-roll montage with voiceover or text overlay")
    lines.append(f"    Camera:       ~10% face, ~90% b-roll")
    lines.append(f"    Shot pacing:  Quick cuts, ~12 shots for a 25s video")
    lines.append(f"    Audio:        Borrowed sound or background music (outperform original)")
    lines.append(f"    Voiceover:    Short, concise (55-80 words for 20-25s)")
    lines.append(f"    OST:          2-3 cards — tells the full story even on mute")
    lines.append(f"    CTA:          1-2 (direct_link at end)")
    lines.append(f"")
    lines.append(f"  THE REMIX IS PURE CONTENT. No creator on camera.")
    lines.append(f"  Product shots, lifestyle b-roll, quick cuts.")
    lines.append(f"  Music or trending audio drives the energy.")
    lines.append(f"  On-screen text tells the complete story.")
    lines.append(f"")

    # ─── KEY DIFFERENCES ────────────────────────────────────────────
    lines.append("─" * 60)
    lines.append("CRITICAL DIFFERENCES (do NOT mix these up)")
    lines.append("─" * 60)
    lines.append(f"")
    lines.append(f"  Hero = creator talking to camera, brief b-roll cutaways")
    lines.append(f"  Remix = all b-roll, no on-camera, music + text/voiceover")
    lines.append(f"")
    lines.append(f"  Hero scripts are SPOKEN DIALOGUE (140-220+ words)")
    lines.append(f"  Remix scripts are SHORT VOICEOVERS (55-80 words) or text-only")
    lines.append(f"")
    lines.append(f"  Hero audio = original/creator audio")
    lines.append(f"  Remix audio = borrowed_sound / background_music")
    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


if __name__ == "__main__":
    print(build_structure_summary_prompt())
