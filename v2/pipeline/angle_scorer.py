"""
Angle Scorer — Content Engine v2 Improvement #1
================================================
Queries the database for content angle performance rankings
and produces structured data that constrains hero concept selection.

Every hero video concept must cite a specific content angle
and the engagement data supporting that choice.

Angle Taxonomy (11 categories):
  shock_curiosity     — Surprising/unexpected element hooks viewers
  fear_urgency        — Fear/anxiety drives purchase motivation
  before_after        — Visual transformation is the core hook
  problem_solution    — "I had this problem, product fixed it"
  lifestyle_aspiration — Product as part of desirable lifestyle
  humor_entertainment — Comedy/entertainment is the vehicle
  social_proof        — Leveraging others' experiences/reactions
  visual_demo         — Pure product demonstration / ASMR satisfaction
  educational         — Teaching/informing first, product second
  authentic_review    — Personal testimonial / genuine endorsement
  value_comparison    — Comparing against alternatives/competitors
"""

from supabase import create_client

SUPABASE_URL = "https://owklfaoaxdrggmbtcwpn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93a2xmYW9heGRyZ2dtYnRjd3BuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NDQyNjcsImV4cCI6MjA4OTAyMDI2N30.EQkJzeS4MYG4QO6aH9c_zbF7BNuH_bKwZIKQpTXvw1Y"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── Angle definitions for prompt context ─────────────────────────────────────

ANGLE_DEFINITIONS = {
    "shock_curiosity": {
        "name": "Shock / Curiosity Reveal",
        "description": "An unexpected, surprising, or gross-out element hooks the viewer immediately. The product reveal comes after the shock moment captures attention.",
        "example_patterns": [
            "Gross/unexpected physical reveal (foot pad gunk, mouthwash dark liquid)",
            "Surprising product transformation (before → shocking after)",
            "Visual shock that stops the scroll (unexpected use, color-coded results)",
        ],
    },
    "fear_urgency": {
        "name": "Fear / Urgency / Preparedness",
        "description": "Fear-based or urgency-driven content where the product is positioned as protection, preparation, or insurance against a threat.",
        "example_patterns": [
            "Doomsday/preparedness angle (power outage, emergency, war)",
            "Counterfeit/scam warning (fake vs real product)",
            "Health scare that product addresses",
        ],
    },
    "before_after": {
        "name": "Before / After Transformation",
        "description": "The core hook is a visible transformation. The viewer sees the product's effect in real-time or side-by-side.",
        "example_patterns": [
            "Skincare application with visible results on camera",
            "Cleaning product transforming a dirty surface",
            "Makeup before/after with no filters",
        ],
    },
    "problem_solution": {
        "name": "Problem / Solution",
        "description": "Creator identifies a specific pain point the viewer relates to, then presents the product as the solution.",
        "example_patterns": [
            "Physical discomfort solved (back pain from cleaning, belt digging in)",
            "Inconvenience eliminated (cord too short, messy organization)",
            "Daily frustration removed (scrubbing on knees, tangled cables)",
        ],
    },
    "lifestyle_aspiration": {
        "name": "Lifestyle Aspiration",
        "description": "The product is positioned as part of a desirable lifestyle. The viewer wants the life, not just the product.",
        "example_patterns": [
            "Cozy bedroom/home aesthetic (comforter, lighting, pillows)",
            "Summer-ready lifestyle (outdoor furniture, beach gear)",
            "Self-care/spa vibes (warm towels, skincare rituals)",
        ],
    },
    "humor_entertainment": {
        "name": "Humor / Entertainment",
        "description": "Comedy or entertainment value is the primary vehicle. The product is secondary to the laugh.",
        "example_patterns": [
            "Absurd/unexpected product use",
            "Self-deprecating humor about the purchase",
            "Comedic commentary over product demo",
        ],
    },
    "social_proof": {
        "name": "Social Proof / Reaction",
        "description": "Leveraging others' experiences — stitches, duets, comment replies where the creator validates or builds on someone else's claim.",
        "example_patterns": [
            "Stitch with another creator's results",
            "Comment reply proving the product works",
            "Reaction to viral claims about the product",
        ],
    },
    "visual_demo": {
        "name": "Visual / ASMR Demo",
        "description": "Pure product demonstration where the visual satisfaction is the hook. Mechanism reveals, satisfying textures, clean product photography.",
        "example_patterns": [
            "Mechanism demo (folding, clicking, assembling)",
            "Satisfying textures (squishy, crunchy, smooth)",
            "Clean product unboxing with detailed feature showcase",
        ],
    },
    "educational": {
        "name": "Educational / Authority",
        "description": "Teaching-first approach. Creator educates the viewer on a topic, building credibility before the product reveal.",
        "example_patterns": [
            "Ingredient education (what does kojic acid do?)",
            "Myth-busting or correction (you're using this wrong)",
            "Science/health education with product as the application",
        ],
    },
    "authentic_review": {
        "name": "Authentic Review / Testimonial",
        "description": "Genuine personal endorsement. The creator shares their honest experience, often with vulnerability or specificity.",
        "example_patterns": [
            "Long-term use update (I've been using this for 4 weeks...)",
            "Honest pros and cons",
            "Personal story connecting to the product",
        ],
    },
    "value_comparison": {
        "name": "Value Comparison",
        "description": "Direct or implied comparison against competitors, showing the product offers better value or performance.",
        "example_patterns": [
            "Budget alternative to expensive brand",
            "Side-by-side feature comparison",
            "Reverse psychology return (I'm returning this... to buy it cheaper online)",
        ],
    },
}


def get_angle_rankings():
    """
    Query the database for content angle performance rankings.

    Returns a list of dicts sorted by weighted_score descending:
    [
        {
            "angle": "shock_curiosity",
            "video_count": 7,
            "avg_likes": 189429,
            "avg_shares": 18730,
            "avg_comments": 1499,
            "weighted_score": 248616,
            "median_likes": 207000,
            "max_likes": 413600,
            "rank": 1,
        },
        ...
    ]
    """
    # Pull all videos with their angles and engagement
    videos = supabase.table("tiktok_videos").select(
        "content_angle, likes, shares, comments"
    ).not_.is_("content_angle", "null").execute().data

    # Aggregate by angle
    angle_data = {}
    for v in videos:
        angle = v["content_angle"]
        if angle not in angle_data:
            angle_data[angle] = {"likes": [], "shares": [], "comments": []}
        angle_data[angle]["likes"].append(v.get("likes", 0) or 0)
        angle_data[angle]["shares"].append(v.get("shares", 0) or 0)
        angle_data[angle]["comments"].append(v.get("comments", 0) or 0)

    # Compute rankings
    rankings = []
    for angle, data in angle_data.items():
        n = len(data["likes"])
        avg_l = sum(data["likes"]) / n
        avg_s = sum(data["shares"]) / n
        avg_c = sum(data["comments"]) / n
        # Weighted: shares x3 (distribution), comments x2 (algo signal), likes x1
        weighted = avg_l + avg_s * 3 + avg_c * 2

        sorted_likes = sorted(data["likes"])
        median_l = sorted_likes[n // 2] if n % 2 == 1 else (sorted_likes[n // 2 - 1] + sorted_likes[n // 2]) / 2

        rankings.append({
            "angle": angle,
            "video_count": n,
            "avg_likes": round(avg_l),
            "avg_shares": round(avg_s),
            "avg_comments": round(avg_c),
            "weighted_score": round(weighted),
            "median_likes": round(median_l),
            "max_likes": max(data["likes"]),
        })

    # Sort by weighted score descending and assign ranks
    rankings.sort(key=lambda x: x["weighted_score"], reverse=True)
    for i, r in enumerate(rankings):
        r["rank"] = i + 1

    return rankings


def get_top_angles(n=5):
    """Return the top N angles by weighted score."""
    return get_angle_rankings()[:n]


def build_angle_constraint_prompt(product_brief, top_n=5):
    """
    Build the prompt section that constrains hero concept generation
    to data-backed content angles.

    Returns a string to inject into the content generation prompt.
    """
    rankings = get_angle_rankings()
    top = rankings[:top_n]

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("CONTENT ANGLE REQUIREMENTS (DATA-DRIVEN)")
    lines.append("=" * 60)
    lines.append("")
    lines.append("Our database of 122 analyzed TikTok videos reveals clear")
    lines.append("performance tiers by content angle. You MUST use these")
    lines.append("rankings to guide hero video concept selection.")
    lines.append("")
    lines.append("ANGLE PERFORMANCE RANKINGS (by weighted engagement score):")
    lines.append("-" * 60)

    for r in top:
        defn = ANGLE_DEFINITIONS.get(r["angle"], {})
        name = defn.get("name", r["angle"])
        desc = defn.get("description", "")
        patterns = defn.get("example_patterns", [])

        lines.append(f"")
        lines.append(f"  #{r['rank']}  {name.upper()} ({r['angle']})")
        lines.append(f"      Weighted Score: {r['weighted_score']:,}")
        lines.append(f"      Avg Likes: {r['avg_likes']:,}  |  Avg Shares: {r['avg_shares']:,}  |  Videos: {r['video_count']}")
        lines.append(f"      Max Single Video: {r['max_likes']:,} likes")
        lines.append(f"      What it is: {desc}")
        if patterns:
            lines.append(f"      Patterns that work:")
            for p in patterns:
                lines.append(f"        - {p}")

    # Add the remaining angles for reference
    remaining = rankings[top_n:]
    if remaining:
        lines.append("")
        lines.append("  LOWER-PERFORMING ANGLES (use sparingly, max 1 hero):")
        for r in remaining:
            defn = ANGLE_DEFINITIONS.get(r["angle"], {})
            name = defn.get("name", r["angle"])
            lines.append(f"    #{r['rank']}  {name} — Score: {r['weighted_score']:,} ({r['video_count']} videos)")

    lines.append("")
    lines.append("-" * 60)
    lines.append("MANDATORY RULES FOR HERO CONCEPT SELECTION:")
    lines.append("")
    lines.append("1. AT LEAST 3 of 5 heroes MUST use a Top 5 angle.")
    lines.append("2. AT LEAST 1 hero MUST use the #1 ranked angle (shock_curiosity).")
    lines.append("3. No more than 2 heroes may use the same angle.")
    lines.append("4. Every hero MUST declare its content_angle in the output.")
    lines.append("5. Every hero MUST include angle_evidence with the exact")
    lines.append("   stats from the rankings above (avg_likes, avg_shares,")
    lines.append("   video_count, rank, weighted_score).")
    lines.append("")
    lines.append("HERO OUTPUT FORMAT (required for each hero):")
    lines.append('  "content_angle": "<angle_key>",')
    lines.append('  "angle_evidence": {')
    lines.append('    "rank": <number>,')
    lines.append('    "weighted_score": <number>,')
    lines.append('    "avg_likes": <number>,')
    lines.append('    "avg_shares": <number>,')
    lines.append('    "video_count": <number>')
    lines.append("  }")
    lines.append("")

    return "\n".join(lines)


def validate_angle_citations(content_json):
    """
    Validate that the generated content.json has proper angle citations.

    Returns (is_valid, issues) tuple.
    """
    issues = []
    rankings = {r["angle"]: r for r in get_angle_rankings()}

    heroes = content_json.get("shoot_guide", {}).get("heroes", [])
    if not heroes:
        issues.append("No heroes found in content.json")
        return False, issues

    angles_used = []
    top5_angles = [r["angle"] for r in get_angle_rankings()[:5]]

    for hero in heroes:
        hero_id = hero.get("hero_id", "unknown")
        angle = hero.get("content_angle")
        evidence = hero.get("angle_evidence")

        if not angle:
            issues.append(f"{hero_id}: Missing content_angle field")
            continue

        if angle not in rankings:
            issues.append(f"{hero_id}: Unknown angle '{angle}' — not in taxonomy")
            continue

        angles_used.append(angle)

        if not evidence:
            issues.append(f"{hero_id}: Missing angle_evidence field")
            continue

        # Verify evidence matches database
        db_rank = rankings[angle]
        if evidence.get("rank") != db_rank["rank"]:
            issues.append(
                f"{hero_id}: Evidence rank {evidence.get('rank')} doesn't match "
                f"database rank {db_rank['rank']} for {angle}"
            )

    # Check distribution rules
    top5_count = sum(1 for a in angles_used if a in top5_angles)
    if top5_count < 3:
        issues.append(
            f"Only {top5_count}/5 heroes use a Top 5 angle (minimum 3 required)"
        )

    # Check #1 angle is used at least once
    if top5_angles and top5_angles[0] not in angles_used:
        issues.append(
            f"The #1 ranked angle ({top5_angles[0]}) is not used in any hero"
        )

    # Check no angle used more than twice
    from collections import Counter
    angle_counts = Counter(angles_used)
    for angle, count in angle_counts.items():
        if count > 2:
            issues.append(f"Angle '{angle}' used {count} times (max 2)")

    return len(issues) == 0, issues


def print_angle_report():
    """Print a formatted angle performance report."""
    rankings = get_angle_rankings()

    print("\n" + "=" * 70)
    print("CONTENT ANGLE PERFORMANCE REPORT")
    print("=" * 70)
    print(f"{'Rank':<6}{'Angle':<25}{'Videos':<8}{'Avg Likes':<12}{'Avg Shares':<12}{'Score':<10}")
    print("-" * 70)

    for r in rankings:
        print(
            f"#{r['rank']:<5}{r['angle']:<25}{r['video_count']:<8}"
            f"{r['avg_likes']:>10,}  {r['avg_shares']:>10,}  {r['weighted_score']:>8,}"
        )

    print("-" * 70)
    total = sum(r["video_count"] for r in rankings)
    print(f"{'':6}{'TOTAL':<25}{total:<8}")
    print("=" * 70)


if __name__ == "__main__":
    print_angle_report()
    print("\n--- Constraint Prompt Preview ---")
    print(build_angle_constraint_prompt("BoomBoom Nasal Stick")[:2000])
    print("...")
