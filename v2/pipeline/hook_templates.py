"""
Hook Template Engine — Content Engine v2 Improvement #2
=======================================================
Instead of generating 30 random hooks and scoring after the fact,
this module decomposes top-performing hooks into structural templates
and fills them with product-specific content.

Data source: 94 hooks from hook_patterns table (122-video database)
Top category: warning (9 hooks, avg 97K engagement)

The hook generation prompt receives these templates as constraints,
ensuring generated hooks follow proven structural patterns rather
than being invented from scratch.
"""

try:
    from v2.pipeline.db import supabase
except ImportError:
    from db import supabase


# ─── Hook Templates (decomposed from top performers) ─────────────────────────
#
# Each template has:
#   - pattern: The structural template with [SLOTS] to fill
#   - category: Which hook_category it maps to
#   - fill_instructions: What goes in each slot
#   - avg_engagement: Average engagement of source hooks
#   - source_hooks: The actual hooks this was decomposed from
#   - when_to_use: Which content angles pair well with this template

HOOK_TEMPLATES = [
    {
        "id": "HT1",
        "name": "The Stop/Don't Warning",
        "pattern": "Stop [COMMON_BEHAVIOR]. [REASON_ITS_WRONG].",
        "category": "warning",
        "fill_instructions": {
            "COMMON_BEHAVIOR": "Something the viewer probably does regularly that the product replaces or improves",
            "REASON_ITS_WRONG": "Brief, surprising reason — one sentence max. The 'why' creates the curiosity gap.",
        },
        "avg_engagement": 97799,
        "source_hooks": [
            "stop shaving your tummy and put down the razors.",
            "We really got to stop scrubbing our tubs like this because it's not even going to get them clean",
            "Stop throwing away your patio furniture every single spring. It's literally a waste of money",
        ],
        "when_to_use": ["problem_solution", "fear_urgency", "educational"],
    },
    {
        "id": "HT2",
        "name": "The Careful/Warning",
        "pattern": "[CAREFUL_PHRASE] when you're [ACTION] these [PRODUCT_CATEGORY].",
        "category": "warning",
        "fill_instructions": {
            "CAREFUL_PHRASE": "'Please be careful' or 'You guys, be careful' — casual, like warning a friend",
            "ACTION": "The purchasing/using action (ordering, buying, using, trying)",
            "PRODUCT_CATEGORY": "Generic product category, NOT the brand name",
        },
        "avg_engagement": 97799,
        "source_hooks": [
            "You guys, please be careful when you're ordering these [product]",
            "Please don't be upset at me if this Easter set is sold out for you, but I just got this",
        ],
        "when_to_use": ["fear_urgency", "social_proof", "shock_curiosity"],
    },
    {
        "id": "HT3",
        "name": "The People Don't Realize",
        "pattern": "People don't realize [SURPRISING_FACT_ABOUT_PRODUCT].",
        "category": "warning",
        "fill_instructions": {
            "SURPRISING_FACT_ABOUT_PRODUCT": "A genuine difference, comparison, or truth about the product/category that most people overlook. Must be specific enough to create a curiosity gap.",
        },
        "avg_engagement": 383300,
        "source_hooks": [
            "People don't realize how crazy the difference is between a black lash and a brown lash",
        ],
        "when_to_use": ["shock_curiosity", "educational", "before_after"],
    },
    {
        "id": "HT4",
        "name": "The Physical Reveal",
        "pattern": "This is what [UNEXPECTED_PHYSICAL_OUTCOME] [TIME/CONTEXT].",
        "category": "curiosity",
        "fill_instructions": {
            "UNEXPECTED_PHYSICAL_OUTCOME": "Something surprising, gross, or visually striking that happened — 'came out of my body', 'happened to my face', 'it looks like now'",
            "TIME/CONTEXT": "When/where it happened — 'last night while I was asleep', 'after just one week', 'this morning'",
        },
        "avg_engagement": 406600,
        "source_hooks": [
            "This is what came out of my body last night while I was asleep",
        ],
        "when_to_use": ["shock_curiosity", "before_after"],
    },
    {
        "id": "HT5",
        "name": "The Dread Flip",
        "pattern": "I used to literally [DREAD/HATE] [RELATABLE_TASK].",
        "category": "curiosity",
        "fill_instructions": {
            "DREAD/HATE": "'dread', 'hate', 'avoid', 'put off' — strong negative emotion word",
            "RELATABLE_TASK": "Something the viewer probably also dislikes. Must be common enough that most people nod along.",
        },
        "avg_engagement": 271800,
        "source_hooks": [
            "I used to literally dread cleaning my tub",
        ],
        "when_to_use": ["problem_solution", "before_after"],
    },
    {
        "id": "HT6",
        "name": "The Secret/Insider Tip",
        "pattern": "If you [COMMON_SITUATION], I would definitely [UNEXPECTED_ADVICE].",
        "category": "curiosity",
        "fill_instructions": {
            "COMMON_SITUATION": "A situation the viewer is likely in — 'have a landlord', 'work from home', 'have kids', 'get congested'",
            "UNEXPECTED_ADVICE": "Advice that sounds like an insider secret — 'keep these a secret', 'never tell anyone about this', 'hide this from your roommate'",
        },
        "avg_engagement": 180500,
        "source_hooks": [
            "If you have a landlord, I would definitely keep these lights a secret, all you need to",
            "If you host outside and everyone fights for the shady seats, watch this.",
        ],
        "when_to_use": ["lifestyle_aspiration", "problem_solution", "social_proof"],
    },
    {
        "id": "HT7",
        "name": "The Visual Comparison Question",
        "pattern": "Do you see this? [COMPARISON_REVEAL].",
        "category": "question",
        "fill_instructions": {
            "COMPARISON_REVEAL": "Immediate visual comparison or before/after — 'And do you see THIS?', 'Now look at THIS side', 'That was before. This is after.'",
        },
        "avg_engagement": 173700,
        "source_hooks": [
            "Do you see this? And do you see this? Just wait for the hooded eyes you just take it over",
        ],
        "when_to_use": ["before_after", "shock_curiosity"],
    },
    {
        "id": "HT8",
        "name": "The Pain Point Question",
        "pattern": "What's it worth to you to finally [DESIRED_OUTCOME]?",
        "category": "question",
        "fill_instructions": {
            "DESIRED_OUTCOME": "The specific relief or result the viewer wants — 'breathe clearly again', 'sleep through the night', 'stop the pain in your [body part]'",
        },
        "avg_engagement": 87600,
        "source_hooks": [
            "What's it worth to you to finally save your legs and knees from total agony?",
        ],
        "when_to_use": ["problem_solution", "fear_urgency"],
    },
    {
        "id": "HT9",
        "name": "The Why Didn't Anyone Tell Me",
        "pattern": "Can somebody explain to me why [RELATABLE_GAP]?",
        "category": "question",
        "fill_instructions": {
            "RELATABLE_GAP": "Something that seems obvious in retrospect but nobody talks about — 'we never learned this', 'nobody told us about this', 'this isn't more popular'",
        },
        "avg_engagement": 57400,
        "source_hooks": [
            "Can somebody explain to me why we never learned any of this in school?",
        ],
        "when_to_use": ["educational", "social_proof", "shock_curiosity"],
    },
    {
        "id": "HT10",
        "name": "The Crazy Reveal Invitation",
        "pattern": "You want to see something [INTENSITY_WORD].",
        "category": "shock",
        "fill_instructions": {
            "INTENSITY_WORD": "'crazy', 'wild', 'insane', 'ridiculous' — one strong word that promises a payoff",
        },
        "avg_engagement": 96500,
        "source_hooks": [
            "You want to see something crazy.",
        ],
        "when_to_use": ["shock_curiosity", "before_after", "visual_demo"],
    },
    {
        "id": "HT11",
        "name": "The Stat Lead",
        "pattern": "[IMPRESSIVE_NUMBER]. [IMPRESSIVE_NUMBER]. [IMPRESSIVE_NUMBER].",
        "category": "curiosity",
        "fill_instructions": {
            "IMPRESSIVE_NUMBER": "Three short, punchy stats about the product — can be nutritional, price, performance, or sales figures. Each one is its own sentence fragment. Rhythm matters.",
        },
        "avg_engagement": 105900,
        "source_hooks": [
            "20 grams of protein, 90 calories, no added sugar.",
        ],
        "when_to_use": ["value_comparison", "educational", "social_proof"],
    },
    {
        "id": "HT12",
        "name": "The Scarcity/FOMO",
        "pattern": "I don't know about you but I'm about to [ACTION] [REASON].",
        "category": "warning",
        "fill_instructions": {
            "ACTION": "Something proactive — 'stock up on', 'order ten of', 'be the first to try'",
            "REASON": "The urgency driver — seasonal, selling out, limited time, getting worse",
        },
        "avg_engagement": 97500,
        "source_hooks": [
            "I don't know about you but I'm about to be parent of the year",
        ],
        "when_to_use": ["fear_urgency", "social_proof", "lifestyle_aspiration"],
    },
]


def get_hook_category_rankings():
    """
    Query the database for hook category performance.
    Returns categories ranked by average engagement.
    """
    hooks = supabase.table("hook_patterns").select(
        "hook_category, avg_engagement_rate"
    ).execute().data

    from collections import defaultdict
    by_cat = defaultdict(list)
    for h in hooks:
        cat = h.get("hook_category", "unknown")
        eng = h.get("avg_engagement_rate", 0) or 0
        by_cat[cat].append(eng)

    rankings = []
    for cat, engagements in by_cat.items():
        n = len(engagements)
        avg = sum(engagements) / n if n > 0 else 0
        rankings.append({
            "category": cat,
            "hook_count": n,
            "avg_engagement": round(avg),
            "max_engagement": max(engagements) if engagements else 0,
        })

    rankings.sort(key=lambda x: x["avg_engagement"], reverse=True)
    for i, r in enumerate(rankings):
        r["rank"] = i + 1

    return rankings


def get_templates_for_angle(content_angle):
    """
    Return hook templates that pair well with a given content angle.
    Sorted by avg_engagement descending.
    """
    matching = [
        t for t in HOOK_TEMPLATES
        if content_angle in t["when_to_use"]
    ]
    matching.sort(key=lambda x: x["avg_engagement"], reverse=True)
    return matching


def build_hook_template_prompt(product_brief, content_angles=None):
    """
    Build the prompt section that provides hook templates as constraints
    for hook generation.

    Args:
        product_brief: Product description
        content_angles: Optional list of content angles being used for heroes.
                       If provided, templates are prioritized by angle match.

    Returns:
        String to inject into the hook generation prompt.
    """
    cat_rankings = get_hook_category_rankings()

    lines = []
    lines.append("")
    lines.append("=" * 60)
    lines.append("HOOK TEMPLATE REQUIREMENTS (DATA-DRIVEN)")
    lines.append("=" * 60)
    lines.append("")
    lines.append("Our database of 94 hooks from 122 analyzed videos reveals")
    lines.append("clear structural patterns in top-performing hooks. You MUST")
    lines.append("use these templates as the foundation for hook generation.")
    lines.append("")
    lines.append("HOOK CATEGORY PERFORMANCE:")
    lines.append("-" * 50)
    for r in cat_rankings[:5]:
        lines.append(
            f"  #{r['rank']}  {r['category'].upper():<20} "
            f"avg {r['avg_engagement']:>8,} engagement  |  {r['hook_count']} hooks"
        )
    lines.append("")
    lines.append("PROVEN HOOK TEMPLATES:")
    lines.append("=" * 50)
    lines.append("")

    # If we have content angles, prioritize templates that match
    if content_angles:
        # Collect all matching templates, deduplicated
        used_ids = set()
        prioritized = []
        for angle in content_angles:
            for t in get_templates_for_angle(angle):
                if t["id"] not in used_ids:
                    prioritized.append(t)
                    used_ids.add(t["id"])
        # Add remaining templates
        for t in HOOK_TEMPLATES:
            if t["id"] not in used_ids:
                prioritized.append(t)
                used_ids.add(t["id"])
        templates_to_show = prioritized
    else:
        templates_to_show = sorted(
            HOOK_TEMPLATES, key=lambda x: x["avg_engagement"], reverse=True
        )

    for t in templates_to_show:
        lines.append(f"  [{t['id']}] {t['name'].upper()}")
        lines.append(f"  Category: {t['category']} | Avg Engagement: {t['avg_engagement']:,}")
        lines.append(f"  Pattern: \"{t['pattern']}\"")
        lines.append(f"  Fill slots:")
        for slot, instruction in t["fill_instructions"].items():
            lines.append(f"    [{slot}]: {instruction}")
        if t["source_hooks"]:
            lines.append(f"  Source examples:")
            for ex in t["source_hooks"][:2]:
                lines.append(f"    - \"{ex[:80]}\"")
        lines.append("")

    lines.append("-" * 50)
    lines.append("MANDATORY RULES FOR HOOK GENERATION:")
    lines.append("")
    lines.append("1. AT LEAST 7 of every 10 hooks MUST be based on a template above.")
    lines.append("   Fill the [SLOTS] with product-specific content.")
    lines.append("2. The remaining 3 hooks may be original, but must follow the")
    lines.append("   STRUCTURAL PATTERNS of the top categories (warning, curiosity,")
    lines.append("   question, shock).")
    lines.append("3. NO hooks from low-performing categories (urgency_scarcity,")
    lines.append("   emotional, stitch_reaction) unless specifically relevant.")
    lines.append("4. Every hook MUST declare which template it's based on (e.g. HT1)")
    lines.append("   or 'ORIGINAL' if not template-based.")
    lines.append("5. WARNING-category hooks (avg 97K) should make up AT LEAST 2")
    lines.append("   of every 10 hooks generated.")
    lines.append("")

    return "\n".join(lines)


def print_template_report():
    """Print a formatted template report."""
    print("\n" + "=" * 70)
    print("HOOK TEMPLATE REPORT")
    print("=" * 70)

    cat_rankings = get_hook_category_rankings()
    print("\nCategory Rankings:")
    for r in cat_rankings:
        print(
            f"  #{r['rank']:<3} {r['category']:<25} "
            f"avg {r['avg_engagement']:>8,}  |  {r['hook_count']} hooks"
        )

    print(f"\nTemplates: {len(HOOK_TEMPLATES)}")
    for t in sorted(HOOK_TEMPLATES, key=lambda x: x["avg_engagement"], reverse=True):
        print(f"  [{t['id']}] {t['name']:<30} | {t['category']:<10} | avg {t['avg_engagement']:>8,}")
        print(f"         Pattern: \"{t['pattern'][:70]}\"")

    print("=" * 70)


# ─── Template Auto-Refresh ──────────────────────────────────────────────────
#
# Monthly scan: queries all hooks from the database, attempts to match each
# to an existing template, and identifies high-performing "orphan" hooks
# that don't fit any template. These become candidates for new templates.
#
# This runs on a schedule (monthly) and produces a report. New templates
# are NOT auto-added — they're surfaced as candidates for human review.

# Simple structural patterns we look for when matching hooks to templates
TEMPLATE_SIGNATURES = {
    "HT1": [r"^stop\b", r"^don'?t\b"],
    "HT2": [r"be careful", r"please be careful", r"careful when"],
    "HT3": [r"people don'?t realize", r"nobody realizes", r"most people don'?t know"],
    "HT4": [r"^this is what", r"this is what .* (came|happened|looks)"],
    "HT5": [r"^i used to (literally )?(dread|hate|avoid)", r"i literally (dread|hate)"],
    "HT6": [r"^if you (have|get|are|work|live)", r"i would definitely"],
    "HT7": [r"^do you see", r"see this\?"],
    "HT8": [r"^what'?s it worth", r"worth .* to (you|finally)"],
    "HT9": [r"^can (somebody|someone) explain", r"why (did|didn'?t) (nobody|anyone)"],
    "HT10": [r"^you (want|wanna) (to )?see something"],
    "HT11": [r"^\d+\s*(grams?|calories|mg|%|million|k\b)", r"^\$?\d+"],
    "HT12": [r"^i don'?t know about you", r"i'?m about to"],
}


def match_hook_to_template(hook_text):
    """
    Try to match a hook to an existing template by structural pattern.
    Returns template ID if matched, None if orphan.
    """
    import re
    text = hook_text.lower().strip()
    for tmpl_id, patterns in TEMPLATE_SIGNATURES.items():
        for pattern in patterns:
            if re.search(pattern, text):
                return tmpl_id
    return None


def refresh_templates(min_engagement=50000, verbose=True):
    """
    Scan the database for hooks that don't match existing templates.

    Steps:
    1. Pull all hooks from hook_patterns table
    2. Match each to existing templates via structural patterns
    3. Identify high-performing orphans (no template match + above threshold)
    4. Cluster orphans by structural similarity
    5. Return report with candidate new templates

    Args:
        min_engagement: Minimum avg engagement for an orphan to be considered
        verbose: Print progress and results

    Returns:
        dict with:
            - total_hooks: int
            - matched: list of {hook, template_id, engagement}
            - orphans: list of {hook, engagement, category}
            - candidates: list of {pattern, hooks, avg_engagement}
            - coverage_pct: float (% of hooks matched to templates)
    """
    # Pull all hooks
    hooks = supabase.table("hook_patterns").select(
        "hook_text, hook_category, avg_engagement_rate"
    ).execute().data

    if verbose:
        print(f"\n{'=' * 60}")
        print(f"  HOOK TEMPLATE REFRESH SCAN")
        print(f"{'=' * 60}")
        print(f"  Total hooks in database: {len(hooks)}")
        print(f"  Existing templates: {len(HOOK_TEMPLATES)}")
        print(f"  Min engagement for candidates: {min_engagement:,}")

    matched = []
    orphans = []

    for h in hooks:
        text = h.get("hook_text", "")
        eng = h.get("avg_engagement_rate", 0) or 0
        cat = h.get("hook_category", "unknown")

        tmpl_id = match_hook_to_template(text)
        if tmpl_id:
            matched.append({"hook": text, "template_id": tmpl_id, "engagement": eng})
        else:
            orphans.append({"hook": text, "engagement": eng, "category": cat})

    # Sort orphans by engagement
    orphans.sort(key=lambda x: x["engagement"], reverse=True)

    # High-performing orphans are template candidates
    high_orphans = [o for o in orphans if o["engagement"] >= min_engagement]

    # Cluster high orphans by category
    from collections import defaultdict
    by_cat = defaultdict(list)
    for o in high_orphans:
        by_cat[o["category"]].append(o)

    candidates = []
    for cat, cat_orphans in sorted(by_cat.items(), key=lambda x: -max(o["engagement"] for o in x[1])):
        avg_eng = sum(o["engagement"] for o in cat_orphans) / len(cat_orphans)
        candidates.append({
            "category": cat,
            "hooks": [o["hook"] for o in cat_orphans],
            "avg_engagement": round(avg_eng),
            "count": len(cat_orphans),
            "top_engagement": max(o["engagement"] for o in cat_orphans),
        })

    coverage_pct = (len(matched) / len(hooks) * 100) if hooks else 0

    if verbose:
        print(f"\n  TEMPLATE COVERAGE:")
        print(f"  {'─' * 50}")
        print(f"  Matched to templates: {len(matched)}/{len(hooks)} ({coverage_pct:.1f}%)")
        print(f"  Orphans (no template): {len(orphans)}")
        print(f"  High-performing orphans (>{min_engagement:,}): {len(high_orphans)}")

        if matched:
            from collections import Counter
            tmpl_counts = Counter(m["template_id"] for m in matched)
            print(f"\n  TEMPLATE USAGE:")
            for tmpl_id, count in tmpl_counts.most_common():
                tmpl_name = next((t["name"] for t in HOOK_TEMPLATES if t["id"] == tmpl_id), "?")
                print(f"    {tmpl_id} ({tmpl_name}): {count} hooks matched")

        if candidates:
            print(f"\n  NEW TEMPLATE CANDIDATES:")
            print(f"  {'─' * 50}")
            for c in candidates:
                print(f"  Category: {c['category']} | {c['count']} hooks | avg {c['avg_engagement']:,} | top {c['top_engagement']:,}")
                for hook in c["hooks"][:3]:
                    print(f"    → \"{hook[:70]}\"")
                if c["count"] > 3:
                    print(f"    ... and {c['count'] - 3} more")
        else:
            print(f"\n  No new template candidates found above {min_engagement:,} engagement.")
            print(f"  All high-performing hooks are covered by existing templates.")

        print(f"\n{'=' * 60}")

    return {
        "total_hooks": len(hooks),
        "matched": matched,
        "orphans": orphans,
        "candidates": candidates,
        "coverage_pct": round(coverage_pct, 1),
    }


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "refresh":
        refresh_templates()
    else:
        print_template_report()
        print("\n--- Template Prompt Preview ---")
        prompt = build_hook_template_prompt(
            "BoomBoom Nasal Stick",
            content_angles=["shock_curiosity", "fear_urgency", "before_after", "problem_solution"]
        )
        print(prompt[:3000])
        print("...")
