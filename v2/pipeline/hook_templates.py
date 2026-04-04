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

from supabase import create_client

SUPABASE_URL = "https://owklfaoaxdrggmbtcwpn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93a2xmYW9heGRyZ2dtYnRjd3BuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NDQyNjcsImV4cCI6MjA4OTAyMDI2N30.EQkJzeS4MYG4QO6aH9c_zbF7BNuH_bKwZIKQpTXvw1Y"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


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


if __name__ == "__main__":
    print_template_report()
    print("\n--- Template Prompt Preview ---")
    prompt = build_hook_template_prompt(
        "BoomBoom Nasal Stick",
        content_angles=["shock_curiosity", "fear_urgency", "before_after", "problem_solution"]
    )
    print(prompt[:3000])
    print("...")
