"""
Rule Refresh System — Content Engine v2
=========================================
Runs all data-driven analysis modules and reports current rule states.

Use this:
  - After ingesting new videos (every 2 weeks or monthly)
  - To verify all rules are up to date
  - To see how rules have changed with new data
  - As a health check before running the pipeline

All analyzer modules query the database LIVE — they never cache.
This script simply runs them all in sequence and prints a unified report.

Usage:
    python3 -m v2.pipeline.refresh_rules
    python3 v2/pipeline/refresh_rules.py
"""

import json
import os
from datetime import datetime

try:
    from v2.pipeline.broll_analyzer import analyze_broll_patterns
    from v2.pipeline.audio_analyzer import analyze_audio_patterns
    from v2.pipeline.cta_analyzer import analyze_cta_patterns
    from v2.pipeline.pacing_analyzer import analyze_pacing_patterns
    from v2.pipeline.structure_rules import analyze_structure
    from v2.pipeline.angle_scorer import get_angle_rankings
    from v2.pipeline.hook_templates import get_hook_category_rankings
    from v2.pipeline.ost_patterns import get_ost_stats
except ImportError:
    from broll_analyzer import analyze_broll_patterns
    from audio_analyzer import analyze_audio_patterns
    from cta_analyzer import analyze_cta_patterns
    from pacing_analyzer import analyze_pacing_patterns
    from structure_rules import analyze_structure
    from angle_scorer import get_angle_rankings
    from hook_templates import get_hook_category_rankings
    from ost_patterns import get_ost_stats


SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "output", "rule_snapshots")


def refresh_all(save_snapshot=True):
    """
    Run all analyzers and return a unified rules report.
    Optionally saves a timestamped snapshot for comparison.
    """
    print("=" * 60)
    print("CONTENT ENGINE — RULE REFRESH")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    results = {}

    # 1. B-roll rules
    print("\n[1/7] Analyzing b-roll patterns...")
    broll = analyze_broll_patterns()
    th = broll.get("talking_head_rules", {})
    results["broll"] = {
        "sample_size": broll["sample_size"],
        "hero_face_pct": th.get("top_half", {}).get("avg_face_pct", "N/A"),
        "hero_broll_pct": th.get("top_half", {}).get("avg_broll_pct", "N/A"),
        "hero_broll_max": th.get("recommended", {}).get("broll_pct_max", "N/A"),
    }
    print(f"  Hero: {results['broll']['hero_face_pct']}% face / {results['broll']['hero_broll_pct']}% broll")
    print(f"  Hard limit: broll ≤ {results['broll']['hero_broll_max']}%")

    # 2. Audio rules
    print("\n[2/7] Analyzing audio patterns...")
    audio = analyze_audio_patterns()
    hero_audio = audio.get("hero_audio", [])
    results["audio"] = {
        "hero_top_audio": hero_audio[0]["value"] if hero_audio else "N/A",
        "hero_audio_count": len(hero_audio),
        "remix_top_audio": audio.get("remix_audio", [{}])[0].get("value", "N/A") if audio.get("remix_audio") else "N/A",
    }
    print(f"  Hero top audio: {results['audio']['hero_top_audio']}")
    print(f"  Remix top audio: {results['audio']['remix_top_audio']}")

    # 3. CTA rules
    print("\n[3/7] Analyzing CTA patterns...")
    cta = analyze_cta_patterns()
    cp = cta.get("count_performance", [])
    qualified = [c for c in cp if c["video_count"] >= 5]
    best_count = max(qualified, key=lambda x: x["avg_likes"]) if qualified else {"cta_count": 3}
    results["cta"] = {
        "total_ctas": cta["total_ctas"],
        "optimal_count": best_count["cta_count"],
        "mid_zone_avg_pct": cta.get("zone_stats", {}).get("mid", {}).get("avg_pct", "N/A"),
        "late_zone_avg_pct": cta.get("zone_stats", {}).get("late", {}).get("avg_pct", "N/A"),
    }
    print(f"  Optimal CTA count: {results['cta']['optimal_count']}")
    print(f"  Mid zone: ~{results['cta']['mid_zone_avg_pct']}% | Late zone: ~{results['cta']['late_zone_avg_pct']}%")

    # 4. Pacing rules
    print("\n[4/7] Analyzing pacing patterns...")
    pacing = analyze_pacing_patterns()
    hs = pacing.get("hero_stats", {})
    results["pacing"] = {
        "hero_avg_words": hs.get("top_half", {}).get("avg_word_count", "N/A"),
        "hero_avg_duration": hs.get("top_half", {}).get("avg_duration", "N/A"),
        "hero_wps": hs.get("top_half", {}).get("avg_wps", "N/A"),
        "sample_size": pacing["sample_size"],
    }
    print(f"  Hero: {results['pacing']['hero_avg_words']} words at {results['pacing']['hero_wps']} wps over {results['pacing']['hero_avg_duration']}s")

    # 5. Angle rankings
    print("\n[5/7] Refreshing angle rankings...")
    angles = get_angle_rankings()
    results["angles"] = {
        "top_3": [(a["angle"], a["weighted_score"]) for a in angles[:3]],
        "total_angles": len(angles),
    }
    print(f"  Top 3: {', '.join(f'{a[0]} ({a[1]:,})' for a in results['angles']['top_3'])}")

    # 6. Hook categories
    print("\n[6/7] Refreshing hook categories...")
    hook_cats = get_hook_category_rankings()
    results["hooks"] = {
        "top_3": [(h["category"], h["avg_engagement"]) for h in hook_cats[:3]],
        "total_categories": len(hook_cats),
    }
    print(f"  Top 3: {', '.join(f'{h[0]} ({h[1]:,})' for h in results['hooks']['top_3'])}")

    # 7. Structure overview
    print("\n[7/7] Refreshing structure rules...")
    structure = analyze_structure()
    results["structure"] = {
        "hero_count": structure["hero"]["count"],
        "remix_count": structure["remix"]["count"],
        "hero_avg_duration": structure["hero"]["avg_duration"],
        "remix_avg_duration": structure["remix"]["avg_duration"],
        "total_videos": structure["total"],
    }
    print(f"  {structure['total']} total videos: {structure['hero']['count']} heroes, {structure['remix']['count']} remixes")

    # Save snapshot
    if save_snapshot:
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_path = os.path.join(SNAPSHOT_DIR, f"rules_{timestamp}.json")

        snapshot = {
            "timestamp": datetime.now().isoformat(),
            "total_videos": structure["total"],
            "rules": results,
        }

        with open(snapshot_path, "w") as f:
            json.dump(snapshot, f, indent=2, default=str)
        print(f"\n  Snapshot saved: {snapshot_path}")

    # Summary
    print("\n" + "=" * 60)
    print("REFRESH COMPLETE — ALL RULES UPDATED FROM LIVE DATA")
    print("=" * 60)
    print(f"\nTotal videos in database: {structure['total']}")
    print(f"Rules derived from: {structure['hero']['count']} hero + {structure['remix']['count']} remix videos")
    print(f"\nNext refresh recommended after ingesting new videos.")
    print(f"Run: python3 -m v2.pipeline.refresh_rules")

    return results


def compare_snapshots(old_path, new_path=None):
    """
    Compare two rule snapshots to see what changed.
    If new_path is None, runs a fresh analysis for comparison.
    """
    with open(old_path) as f:
        old = json.load(f)

    if new_path:
        with open(new_path) as f:
            new = json.load(f)
    else:
        new = {"rules": refresh_all(save_snapshot=False), "timestamp": datetime.now().isoformat()}

    print(f"\n{'=' * 60}")
    print(f"RULE COMPARISON")
    print(f"Old: {old['timestamp']} ({old.get('total_videos', '?')} videos)")
    print(f"New: {new['timestamp']} ({new.get('total_videos', '?')} videos)")
    print(f"{'=' * 60}")

    old_r = old.get("rules", {})
    new_r = new.get("rules", {})

    changes = []
    for key in set(list(old_r.keys()) + list(new_r.keys())):
        if old_r.get(key) != new_r.get(key):
            changes.append(key)
            print(f"\n  [{key}] CHANGED:")
            print(f"    Old: {old_r.get(key)}")
            print(f"    New: {new_r.get(key)}")

    if not changes:
        print("\n  No changes detected.")

    return changes


if __name__ == "__main__":
    refresh_all()
