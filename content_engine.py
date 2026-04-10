"""
TikTok Affiliate Content Generation Engine
============================================
Takes a product brief + patterns from the research database
and generates video concepts, hooks, script outlines, shot lists, and CTAs.
"""

import json
import os
from supabase import create_client

# --- Configuration (env vars override defaults) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://owklfaoaxdrggmbtcwpn.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93a2xmYW9heGRyZ2dtYnRjd3BuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NDQyNjcsImV4cCI6MjA4OTAyMDI2N30.EQkJzeS4MYG4QO6aH9c_zbF7BNuH_bKwZIKQpTXvw1Y")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_research_data(product_category=None, limit=200):
    """Pull all research data from the database for content generation."""

    # Get top-performing videos
    query = supabase.table("tiktok_videos").select("*").order("likes", desc=True).limit(limit)
    if product_category:
        query = query.eq("product_category", product_category)
    videos = query.execute().data

    # Get all transcripts (batch query, then index by video_id)
    video_ids = [v["id"] for v in videos]
    vid_id_set = set(video_ids)
    transcripts = {}
    all_transcripts_raw = supabase.table("video_transcripts").select("*").execute().data
    for t in all_transcripts_raw:
        if t.get("video_id") in vid_id_set:
            transcripts[t["video_id"]] = t

    # Get all CTAs (batch)
    all_ctas_raw = supabase.table("video_ctas").select("*").execute().data
    all_ctas = [c for c in all_ctas_raw if c.get("video_id") in vid_id_set]

    # Get on-screen text (batch)
    all_onscreen_raw = supabase.table("video_onscreen_text").select("*").execute().data
    all_onscreen = [o for o in all_onscreen_raw if o.get("video_id") in vid_id_set]

    # Get visual notes (batch)
    all_visuals_raw = supabase.table("video_visual_notes").select("*").execute().data
    all_visuals = [v for v in all_visuals_raw if v.get("video_id") in vid_id_set]

    # Get hook patterns (sorted by engagement)
    hooks = supabase.table("hook_patterns").select("*").order("avg_engagement_rate", desc=True).limit(20).execute().data

    # Get CTA patterns (sorted by engagement)
    cta_patterns = supabase.table("cta_patterns").select("*").order("avg_engagement_rate", desc=True).limit(20).execute().data

    # Get shot breakdowns (b-roll classification data) — batch query
    all_shot_breakdowns = supabase.table("video_shot_breakdown").select("*").execute().data
    # Filter to only videos in our set
    vid_id_set = set(video_ids)
    all_shot_breakdowns = [sb for sb in all_shot_breakdowns if sb.get("video_id") in vid_id_set]

    return {
        "videos": videos,
        "transcripts": transcripts,
        "ctas": all_ctas,
        "onscreen_text": all_onscreen,
        "visual_notes": all_visuals,
        "hook_patterns": hooks,
        "cta_patterns": cta_patterns,
        "shot_breakdowns": all_shot_breakdowns,
    }


def analyze_patterns(research_data):
    """Analyze research data to identify winning patterns."""

    videos = research_data["videos"]
    ctas = research_data["ctas"]
    hooks = research_data["hook_patterns"]
    visuals = research_data["visual_notes"]

    # --- Hook analysis ---
    hook_categories = {}
    for h in hooks:
        cat = h.get("hook_category", "unknown")
        if cat not in hook_categories:
            hook_categories[cat] = {"count": 0, "total_engagement": 0, "examples": []}
        hook_categories[cat]["count"] += 1
        hook_categories[cat]["total_engagement"] += (h.get("avg_engagement_rate") or 0)
        hook_categories[cat]["examples"].append(h["hook_text"])

    # Sort by average engagement
    for cat in hook_categories:
        hook_categories[cat]["avg_engagement"] = (
            hook_categories[cat]["total_engagement"] / hook_categories[cat]["count"]
            if hook_categories[cat]["count"] > 0 else 0
        )

    # --- CTA analysis ---
    cta_types = {}
    for c in ctas:
        ctype = c.get("cta_type", "unknown")
        if ctype not in cta_types:
            cta_types[ctype] = {"count": 0, "positions": [], "examples": []}
        cta_types[ctype]["count"] += 1
        cta_types[ctype]["positions"].append(c.get("cta_position", "closing"))
        cta_types[ctype]["examples"].append(c["cta_text"])

    # --- Format analysis ---
    format_counts = {}
    hook_style_counts = {}
    for v in visuals:
        fmt = v.get("format_type", "unknown")
        format_counts[fmt] = format_counts.get(fmt, 0) + 1
        hs = v.get("hook_style", "unknown")
        hook_style_counts[hs] = hook_style_counts.get(hs, 0) + 1

    # --- Engagement benchmarks ---
    if videos:
        avg_likes = sum(v.get("likes", 0) for v in videos) / len(videos)
        avg_shares = sum(v.get("shares", 0) for v in videos) / len(videos)
        avg_duration = sum(v.get("duration_seconds", 0) or 0 for v in videos) / len(videos)
    else:
        avg_likes = avg_shares = avg_duration = 0

    return {
        "hook_categories": hook_categories,
        "cta_types": cta_types,
        "format_counts": format_counts,
        "hook_style_counts": hook_style_counts,
        "benchmarks": {
            "avg_likes": avg_likes,
            "avg_shares": avg_shares,
            "avg_duration_seconds": avg_duration,
            "total_videos_analyzed": len(videos),
        }
    }


def derive_use_case_rules(product_brief):
    """
    PRODUCT USE CASE LAYER: Ensures scripts reflect real, natural scenarios.

    Analyzes the product brief to understand:
    - HOW the product is physically used (worn, held, placed, etc.)
    - WHERE the user must be relative to the product (present, remote, etc.)
    - WHAT the product actually captures/does from the user's perspective
    - WHY someone would realistically use it in daily life

    Returns mandatory use case rules that prevent scripts from describing
    impossible, implausible, or inappropriate scenarios.
    """
    rules = []
    rules.append("")
    rules.append("═" * 50)
    rules.append("PRODUCT USE CASE RULES (mandatory for every script)")
    rules.append("These rules ensure scripts describe REAL scenarios.")
    rules.append("═" * 50)

    # ──────────────────────────────────────────────
    # Detect product type from brief
    # ──────────────────────────────────────────────
    brief_lower = product_brief.lower()

    # Wearable camera detection
    is_wearable = any(w in brief_lower for w in ["glasses", "wearable", "wear", "worn", "ring", "watch", "bracelet", "necklace", "pendant"])
    is_camera = any(w in brief_lower for w in ["camera", "record", "video", "photo", "8mp", "1080p", "4k", "capture", "film"])
    is_glasses = any(w in brief_lower for w in ["glasses", "sunglasses", "eyewear", "frames", "lenses"])
    is_audio = any(w in brief_lower for w in ["speaker", "headphone", "earbuds", "audio", "music", "calls", "bluetooth"])
    is_ai = any(w in brief_lower for w in [" ai ", "ai-", "assistant", "smart", "voice command"])
    is_handheld = any(w in brief_lower for w in ["handheld", "grip", "stick", "tripod"]) and not is_wearable

    # Build use case rules based on product type
    if is_wearable and is_camera:
        rules.append(f"""
USE CASE RULE 1 — PHYSICAL REALITY
  This is a WEARABLE CAMERA product. Fundamental constraints:
  - The user WEARS the product on their body ({"on their face as glasses" if is_glasses else "on their person"})
  - The user MUST BE PHYSICALLY PRESENT to use it — it is NOT a security camera, nanny cam, or surveillance device
  - It captures the user's FIRST-PERSON PERSPECTIVE — what they see with their own eyes
  - It records WHILE the user is doing something else with their hands free
  - It does NOT record when the user is absent, sleeping, or elsewhere
  - It cannot be left somewhere to monitor a room or person remotely

  INVALID SCENARIO PATTERNS (never use these):
  ✗ "See what happens when I'm not there" — you MUST be there to wear it
  ✗ Monitoring children, spouse, or anyone without being present
  ✗ Surveillance framing ("catch them," "find out what they're doing," "spy on")
  ✗ Recording someone in a private/intimate setting (bathroom, shower, changing)
  ✗ Recording situations where a camera would be unwelcome or creepy
  ✗ Leaving the glasses somewhere to record on their own
  ✗ Recording activities that would make zero sense as POV (e.g., your own commute — boring and nobody would choose to document this)

  VALID SCENARIO PATTERNS (lean into these):
  ✓ Capturing kids' milestones WHILE being present and participating (soccer game, school play, birthday party, first bike ride, helping with homework, playing in the park)
  ✓ Hands-free recording of activities where holding a phone is impractical (cooking a recipe, doing crafts, playing with kids, carrying groceries, walking through a market)
  ✓ Capturing genuine reactions from people you're interacting with face-to-face (friend seeing a surprise, kid opening a gift, family dinner conversation)
  ✓ POV lifestyle content (exploring a neighborhood, morning routine, day-at-the-park)
  ✓ Being PRESENT in the moment instead of behind a phone screen (the "put the phone down" angle)
  ✓ Discreet/natural recording where pulling out a phone would change the moment (candid family moments, genuine laughter, kids being kids)
  ✓ Content creation without a camera crew (POV tutorials, day-in-life vlogs, try-on hauls from YOUR perspective)""")

        rules.append(f"""
USE CASE RULE 2 — THE "WOULD A REAL PERSON DO THIS?" TEST
  Before writing any scenario into a script, apply this test:
  1. Would a real person in the creator's life actually DO this activity while wearing these?
  2. Is the scenario something that would make a viewer say "oh that's smart, I'd use it for that too"?
  3. Does the scenario show the product solving a REAL problem the target audience has?
  4. Would recording this moment from first-person POV actually produce INTERESTING footage?
  5. Is the creator physically present and actively participating (not observing from afar)?

  If ANY answer is "no," rewrite the scenario.

  EXAMPLES OF THE TEST:
  ✓ "Recording my daughter's soccer game while cheering" → YES. Hands busy clapping, can't hold phone, captures the action from the sideline
  ✓ "Capturing my kids' faces when they see the birthday cake" → YES. Your hands are holding the cake, they're looking at YOU, perfect POV
  ✓ "Filming a cooking tutorial without a tripod" → YES. Both hands on ingredients, POV of the process, natural and practical
  ✗ "Recording my commute home" → NO. Nobody would choose to document their daily commute. It's boring. Not a real use case.
  ✗ "Seeing what my kids do when I leave the room" → NO. You have to be IN the room wearing them. This is surveillance framing.
  ✗ "Recording my daughter singing in the shower" → NO. Inappropriate and invasive. Nobody would film this.""")

        if is_glasses:
            rules.append(f"""
USE CASE RULE 3 — GLASSES-SPECIFIC CONTEXT
  Smart glasses have a unique advantage: they look like normal sunglasses/eyewear.
  Use this in scripts, but responsibly:
  ✓ "Nobody even knows I'm recording" — in the context of capturing genuine candid moments (kids playing, friend reactions)
  ✗ "Nobody even knows I'm recording" — in the context of spying, monitoring, or catching someone

  The "invisible camera" angle should emphasize AUTHENTICITY of captured moments, NOT covert surveillance.
  Frame it as: "I can be present in the moment AND have it captured forever" — not "I can watch people without them knowing."

  NATURAL GLASSES BEHAVIORS to reference in scripts:
  - Putting them on in the morning like any sunglasses
  - Wearing them outside with the kids naturally
  - Having them on at family gatherings
  - Wearing them while doing activities (cooking, walking, playing)
  - People complimenting the frames (they look like regular glasses)
  - Forgetting you're even wearing them (lightweight, comfortable)""")

    elif is_wearable and not is_camera:
        rules.append(f"""
USE CASE RULE 1 — PHYSICAL REALITY
  This is a WEARABLE product (non-camera). The user wears it on their body.
  All scenarios must show the user actively wearing and benefiting from the product.
  Scenarios must reflect real daily activities where the product adds value.""")

    elif is_camera and not is_wearable:
        rules.append(f"""
USE CASE RULE 1 — PHYSICAL REALITY
  This is a CAMERA product (not body-worn). Scenarios should reflect how a standard
  camera is used — the user holds it, mounts it, or places it deliberately.
  Consider the product's actual form factor when describing use cases.""")

    # ──────────────────────────────────────────────
    # Food / Snack / Beverage detection
    # ──────────────────────────────────────────────
    is_food = any(w in brief_lower for w in [
        "snack", "food", "fruit", "strawberr", "dried", "freeze-dried", "freeze dried",
        "chips", "candy", "chocolate", "gummy", "protein bar", "jerky", "nuts",
        "cereal", "granola", "oatmeal", "cookie", "cracker", "popcorn",
        "beverage", "drink", "juice", "tea", "coffee", "powder", "supplement",
        "vitamin", "collagen powder", "superfood", "organic",
    ])
    is_skincare = any(w in brief_lower for w in [
        "skincare", "serum", "cream", "moisturizer", "cleanser", "toner",
        "eye cream", "eye patch", "mask", "sunscreen", "spf", "retinol",
        "hyaluronic", "niacinamide", "snail mucin", "peptide",
    ])

    if is_food and not is_skincare and not (is_wearable or is_camera):
        rules.append(f"""
USE CASE RULE 1 — NO BASELESS HEALTH CLAIMS (MANDATORY)
  This is a FOOD / SNACK product. Strict rules about what scripts can and cannot claim:

  NEVER make health claims the brand does NOT explicitly make. This includes:
  ✗ Dental health claims ("my dentist noticed," "my teeth look better," "good for your teeth")
  ✗ Skin/nail/hair improvement claims ("my nails are healthier," "my skin is glowing because of this")
  ✗ Weight loss claims ("I lost weight eating these," "helps you slim down")
  ✗ Medical/clinical claims ("clinically proven," "doctors recommend," "treats/cures/prevents")
  ✗ Sleep improvement claims ("I sleep better now")
  ✗ Energy/performance claims ("gives me more energy," "I perform better") — unless the brand explicitly claims this
  ✗ Any claim that implies this product TREATS, CURES, or PREVENTS a health condition
  ✗ Any claim that a health professional (dentist, doctor, trainer, nutritionist) endorsed or noticed results

  WHAT YOU CAN CLAIM (based on product facts only):
  ✓ Taste and sensory experience ("the crunch," "it's sweet," "the texture," "it tastes amazing")
  ✓ Ingredient transparency ("no added sugar," "organic," "just one ingredient," "no preservatives")
  ✓ Convenience ("easy snack," "throw it in my bag," "my kids love it," "add to yogurt/cereal")
  ✓ Certifications that ARE on the label (USDA Organic, Non-GMO, third-party tested, allergen-free)
  ✓ Nutritional FACTS from the label (vitamin C content, fiber, antioxidants) — but frame as facts, NOT as health promises
  ✓ Value/price comparisons ("bigger bag," "lasts longer," "cheaper per ounce")
  ✓ Trend/virality ("everyone's been eating these," "this went viral," "TikTok made me try it")
  ✓ Replacing less healthy snacks ("instead of chips," "my kids eat this instead of candy")
  ✓ Real experience ("I can't stop eating these," "I'm obsessed," "my kids love them")

  THE RULE: If the brand's packaging, listing, or marketing materials don't make a specific claim,
  the script CANNOT make that claim. Stick to taste, texture, convenience, value, and verified facts.

USE CASE RULE 2 — FOOD CONTENT IS ABOUT THE EXPERIENCE
  Food content on TikTok performs when it's about:
  - The VISUAL (bright colors, satisfying textures, ASMR crunch)
  - The TASTE (genuine reactions, describing flavors)
  - The HACK (unexpected ways to eat it — cereal, yogurt topping, baking, smoothie)
  - The VALUE (price comparison, bag size comparison, how long it lasts)
  - The SWAP (replacing unhealthy snacks with this, "instead of" framing)
  - The TREND (riding a TikTok food trend, "I finally tried the thing everyone's been eating")
  - The FAMILY (kids love it, packing school lunches, road trip snack)

  It does NOT perform when it's about:
  ✗ Health lectures ("here's why this is good for you" — boring, preachy)
  ✗ Pseudo-science ("the antioxidants in this will transform your body")
  ✗ Professional endorsements that didn't happen
  ✗ Before/after body transformation claims""")

    elif not (is_wearable or is_camera):
        rules.append(f"""
USE CASE RULE 1 — PHYSICAL REALITY
  All scenarios in scripts must reflect how this product is ACTUALLY used in real life.
  Before writing any scenario, ask: "Would a real person do this? Would they use this product this way?"
  If the answer isn't obviously yes, choose a different scenario.""")

    # Universal rule for all products
    rules.append(f"""
USE CASE RULE — SCENARIO DIVERSITY
  Each of the 5 hero videos must showcase a DIFFERENT real use case.
  Don't repeat the same scenario with slight variations. Each video should make a
  viewer think "oh I hadn't thought of using it for THAT" — expanding their mental
  model of why they need the product.

  For each hero video, the use case should map to a different life moment:
  - Hero 1: An emotional/personal moment (family, kids, milestones)
  - Hero 2: A practical/functional moment (activity where hands-free matters)
  - Hero 3: A social moment (with friends, family gatherings, events)
  - Hero 4: A value/discovery moment (price shock, unboxing, first impressions)
  - Hero 5: A lifestyle/daily routine moment (integrated into everyday life)

  CRITICAL: Every scenario must pass the "would a real person do this?" test above.""")

    return "\n".join(rules)


def derive_structural_rules(research_data, patterns):
    """
    THE RULES ENGINE: Converts raw pattern analysis into explicit structural rules.

    This function bridges the gap between "here's what the data shows" and
    "here's exactly what you MUST do in every script." Rules are derived
    dynamically from whatever data is in the database, so they evolve as
    more videos are added.

    Returns a formatted string of mandatory rules for the content generation prompt.
    """
    videos = research_data["videos"]
    ctas = research_data["ctas"]
    hooks = research_data["hook_patterns"]

    rules = []
    rules.append("═" * 50)
    rules.append("MANDATORY STRUCTURAL RULES (derived from database analysis)")
    rules.append("These are NOT suggestions. Every script MUST follow these rules.")
    rules.append("═" * 50)

    # ──────────────────────────────────────────────
    # RULE 1: SCRIPT STRUCTURE TEMPLATE
    # Derived from transcript analysis of top performers
    # ──────────────────────────────────────────────

    # Calculate duration sweet spot
    durations = [(v.get("duration_seconds") or 0, v.get("likes", 0)) for v in videos if v.get("duration_seconds")]
    if durations:
        # Weighted average duration by likes
        total_weight = sum(d[1] for d in durations)
        if total_weight > 0:
            weighted_avg = sum(d[0] * d[1] for d in durations) / total_weight
        else:
            weighted_avg = sum(d[0] for d in durations) / len(durations)

        # Duration buckets
        short = [d for d in durations if d[0] <= 30]
        medium = [d for d in durations if 30 < d[0] <= 60]
        long_v = [d for d in durations if d[0] > 60]

        short_avg = sum(d[1] for d in short) / len(short) if short else 0
        medium_avg = sum(d[1] for d in medium) / len(medium) if medium else 0
        long_avg = sum(d[1] for d in long_v) / len(long_v) if long_v else 0

        best_bucket = max(
            [("15-30s", short_avg, len(short)), ("35-55s", medium_avg, len(medium)), ("60s+", long_avg, len(long_v))],
            key=lambda x: x[1]
        )
    else:
        weighted_avg = 45
        best_bucket = ("35-55s", 0, 0)

    rules.append(f"""
RULE 1 — SCRIPT DURATION & WORD COUNT
  Weighted avg duration (by likes): {weighted_avg:.0f}s
  Best-performing duration bucket: {best_bucket[0]} ({best_bucket[1]:,.0f} avg likes, {best_bucket[2]} videos)
  HERO VIDEOS: Target 35-55 seconds of spoken content (140-220 words at conversational pace)
  REMIX VIDEOS: Target 15-25 seconds (60-100 words)
  Each hero script MUST have 6-8 spoken lines minimum. Not 3-4.""")

    # ──────────────────────────────────────────────
    # RULE 2: SCRIPT SEGMENT STRUCTURE
    # Derived from transcript thirds analysis
    # ──────────────────────────────────────────────
    rules.append(f"""
RULE 2 — SCRIPT SEGMENT STRUCTURE (mandatory for every hero video)
  Every hero video script MUST follow this 4-segment structure:

  SEGMENT 1 — HOOK (first 3-5 seconds, ~10-15 words)
    Purpose: Stop the scroll. Create curiosity or emotional reaction.
    Rules: ZERO product mentions. ZERO features. Pure attention capture.
    The hook must work on its own without context.

  SEGMENT 2 — STORY / PROBLEM (5-15 seconds, ~40-60 words)
    Purpose: Build emotional investment before revealing the product.
    Rules: Personal experience, pain point, or relatable scenario.
    Introduce the product as the SOLUTION, not the subject.
    Data shows: Top performer (293K likes) delayed product reveal for 48 seconds.
    The longer you earn attention before pitching, the better.

  SEGMENT 3 — PRODUCT VALUE (15-35 seconds, ~60-100 words)
    Purpose: Demonstrate features through benefits, not specs.
    Rules: Mix of on-camera and voiceover lines.
    VOICEOVER lines go here (editor covers with b-roll product shots).
    Include at least ONE social proof beat ("my husband/sister/friend...").
    Include at least ONE value_proposition CTA in this segment.
    Data shows: Middle-positioned CTAs are 100% value_proposition type.

  SEGMENT 4 — CTA CLOSE (last 8-12 seconds, ~30-45 words)
    Purpose: Drive the click. This is where the conversion happens.
    Rules: MANDATORY — see Rule 3 for exact CTA requirements.
    This segment must be SPOKEN, not just on-screen text.
    The creator must SAY the call to action out loud.""")

    # ──────────────────────────────────────────────
    # RULE 3: CTA RULES
    # Derived from CTA position/type analysis
    # ──────────────────────────────────────────────

    # Calculate CTA stats
    total_ctas = len(ctas)
    if total_ctas > 0:
        position_counts = {}
        type_counts = {}
        for c in ctas:
            pos = c.get("cta_position", "unknown")
            position_counts[pos] = position_counts.get(pos, 0) + 1
            ctype = c.get("cta_type", "unknown")
            type_counts[ctype] = type_counts.get(ctype, 0) + 1

        closing_pct = position_counts.get("closing", 0) / total_ctas * 100
        middle_pct = position_counts.get("middle", 0) / total_ctas * 100

        # Most common closing CTA types
        closing_types = []
        for c in ctas:
            if c.get("cta_position") == "closing":
                closing_types.append(c.get("cta_type", "unknown"))

        # Top closing CTA examples
        closing_examples = []
        for c in ctas:
            if c.get("cta_position") == "closing" and c.get("cta_type") in ("direct_link", "urgency", "scarcity"):
                closing_examples.append(f'  [{c["cta_type"]}] "{c["cta_text"][:80]}"')

        # CTA count vs performance
        cta_by_video = {}
        for c in ctas:
            vid = c.get("video_id")
            cta_by_video[vid] = cta_by_video.get(vid, 0) + 1

        if videos:
            avg_ctas = total_ctas / len(videos)
            top_half = videos[:len(videos)//2]
            bottom_half = videos[len(videos)//2:]
            top_avg_ctas = sum(cta_by_video.get(v["id"], 0) for v in top_half) / max(len(top_half), 1)
            bottom_avg_ctas = sum(cta_by_video.get(v["id"], 0) for v in bottom_half) / max(len(bottom_half), 1)
        else:
            avg_ctas = top_avg_ctas = bottom_avg_ctas = 0
    else:
        closing_pct = middle_pct = avg_ctas = top_avg_ctas = bottom_avg_ctas = 0
        closing_examples = []

    rules.append(f"""
RULE 3 — CTA REQUIREMENTS (mandatory for every hero video)
  Database analysis: {total_ctas} CTAs across {len(videos)} videos = {avg_ctas:.1f} avg CTAs per video
  Top-performing half averages {top_avg_ctas:.1f} CTAs vs {bottom_avg_ctas:.1f} for bottom half.
  CTA position distribution: {closing_pct:.0f}% closing, {middle_pct:.0f}% middle.

  EVERY hero video script MUST include at minimum:
    1. ONE mid-video value_proposition CTA (spoken, in Segment 3)
       Purpose: Plant the seed. Frame the product's value.
       Example tone: "What's it worth to you to finally [solve pain point]?"
    2. ONE direct_link CTA (spoken, in Segment 4)
       Purpose: Tell them WHERE to buy. Must reference the link explicitly.
       Example: "I'm gonna link them right here in this video"
    3. ONE urgency or scarcity CTA (spoken, in Segment 4, within 5 seconds of direct_link)
       Purpose: Tell them WHY to buy NOW. Create time pressure.
       Example: "They can't keep these in stock" / "I don't know how long that price is gonna last"

  MINIMUM 3 spoken CTAs per hero video. Target 4-5 for maximum conversion.

  CTA language from top performers (use these as templates, adapt to creator voice):
{chr(10).join(closing_examples[:8])}

  CRITICAL: CTAs must be SPOKEN by the creator, not just on-screen text.
  On-screen text CTAs are IN ADDITION TO spoken CTAs, never instead of.""")

    # ──────────────────────────────────────────────
    # RULE 4: HOOK RULES (v4 — lean, show-don't-teach)
    #
    # CHANGE LOG:
    #   v1: Category-based (forced variety — 9x performance dilution)
    #   v2: Curiosity-gap concept (too abstract — hooks still polished)
    #   v3: Archetype-driven (too verbose — 53K prompt, energy diluted by explanation)
    #   v4: Lean version. Raw data + short commands. No essays. Let the hooks speak.
    # ──────────────────────────────────────────────

    # Sort ALL hooks by engagement
    sorted_hooks = sorted(hooks, key=lambda h: (h.get("avg_engagement_rate") or 0), reverse=True)

    # Build the full ranked hook list — just the data, no commentary
    all_hook_lines = []
    for i, h in enumerate(sorted_hooks):
        eng = h.get("avg_engagement_rate") or 0
        all_hook_lines.append(f'    #{i+1} ({eng:,.0f}): "{h["hook_text"]}"')

    rules.append(f"""
RULE 4 — HOOKS

  Here are the hooks from our database, ranked by engagement. These are real.
  Read every single one. Your hooks must match this energy.

{chr(10).join(all_hook_lines)}

  Now look at the top 5 again:
    #1 (406,600): "This is what came out of my body last night while I was asleep"
    #2 (293,700): "Simpsons predicted what was going to happen in Iran."
    #3 (153,500): "Poke it, loop it, strap it."
    #4 (87,600): "What's it worth to you to finally save your legs and knees from total agony?"
    #5 (73,600): "The treadmill?"

  These are raw. Messy. Weird. Blurted out. Incomplete. They sound like a person,
  not a copywriter. #1 is borderline gross. #2 has NOTHING to do with the product.
  #3 is three words. #5 is TWO WORDS. That's why they work.

  YOUR HOOKS MUST:
    - Be under 12 words. Ideally under 8. The shorter, the more powerful.
    - Sound like something blurted out mid-conversation, NOT a written sentence.
    - Leave a massive unanswered question — the viewer CANNOT figure out what's going on.
    - Have ZERO product mentions, ZERO feature descriptions, ZERO brand names.
    - Be SPECIFIC, not generic. "Why is nobody talking about this?" could be about ANYTHING — that's lazy.
      "My mom asked why my eyes looked different and I" is specific to a real moment. Specificity wins.
    - Reference a REAL SCENARIO the creator would actually experience with this product.
      Think about: what moment would make someone text their friend about this?
    - Use a different APPROACH for each hero (pick 5 different ones):
        * Visceral/shocking result ("This is what came out of...")
        * Unrelated pattern interrupt ("Simpsons predicted...")
        * Ultra-short mystery ("Poke it, loop it, strap it" / "The treadmill?")
        * Emotional gut punch ("Your child has a whole world in their head")
        * Warning/pseudo-negative ("Be careful ordering these" / "I'm returning this")
        * Authority challenge ("Can somebody explain why we never learned this?")
        * Hidden truth ("They told you to take pills but never told you about this")
        * Price shock that feels impossible ("STOP PAYING CLOSE TO A GRAND")
        * Reverse psychology ("Unfortunately I will be returning my...")

  YOUR HOOKS MUST NOT:
    - Be a complete thought. If the hook makes sense on its own, it's not a hook.
    - Describe what the product does. That's an ad, not a hook.
    - Sound polished, crafted, or written. These should feel SPOKEN.
    - Start with "So," "You know what," "What if," or "Have you ever" — those are soft openings, not hooks.
    - Be predictable. If I can guess the next line, the hook failed.

  WRITE YOUR 5 HOOKS FIRST. Then check each one against the top 5 above.
  Ask: does my hook have the same raw, weird, incomplete, scroll-stopping energy?
  If not, throw it out and try again. Do not settle for a "pretty good" hook.""")

    # ──────────────────────────────────────────────
    # RULE 5: FORMAT & VOICE DELIVERY RULES
    # Derived from content type and voice delivery analysis
    # ──────────────────────────────────────────────

    # Content type performance
    ct_perf = {}
    for v in videos:
        ct = v.get("content_type", "unknown")
        if ct not in ct_perf:
            ct_perf[ct] = {"count": 0, "total_likes": 0}
        ct_perf[ct]["count"] += 1
        ct_perf[ct]["total_likes"] += v.get("likes", 0)

    ct_ranked = sorted(ct_perf.items(), key=lambda x: x[1]["total_likes"]/max(x[1]["count"],1), reverse=True)

    # Voice delivery performance
    vd_perf = {}
    for v in videos:
        vd = v.get("voice_delivery", "on_camera")
        if vd not in vd_perf:
            vd_perf[vd] = {"count": 0, "total_likes": 0}
        vd_perf[vd]["count"] += 1
        vd_perf[vd]["total_likes"] += v.get("likes", 0)

    vd_ranked = sorted(vd_perf.items(), key=lambda x: x[1]["total_likes"]/max(x[1]["count"],1), reverse=True)

    ct_lines = []
    for ct, data in ct_ranked:
        avg = data["total_likes"] / data["count"]
        ct_lines.append(f"    {ct}: {avg:,.0f} avg likes ({data['count']} videos)")

    vd_lines = []
    for vd, data in vd_ranked:
        avg = data["total_likes"] / data["count"]
        vd_lines.append(f"    {vd}: {avg:,.0f} avg likes ({data['count']} videos)")

    best_ct = ct_ranked[0][0] if ct_ranked else "voiceover_broll"
    best_vd = vd_ranked[0][0] if vd_ranked else "voiceover_offscreen"

    rules.append(f"""
RULE 5 — FORMAT & VOICE DELIVERY MIX
  Content type performance (ranked):
{chr(10).join(ct_lines)}

  Voice delivery performance (ranked):
{chr(10).join(vd_lines)}

  REQUIRED MIX for 5 hero videos:
    - At least 2 heroes MUST use the top-performing format ({best_ct})
    - At least 2 heroes MUST use {best_vd} voice delivery
    - At least 1 hero must be on_camera talking_head (for creator connection)
    - The remaining heroes should vary formats for testing
    - Every hero must have a MIX of on-camera and voiceover lines
      (data shows pure on-camera underperforms mixed delivery)

  REQUIRED MIX for 5 remix videos:
    - At least 2 text-only (no voice, music + on-screen text)
    - At least 2 voiceover + b-roll
    - 1 hybrid (mix of text and short voiceover)""")

    # ──────────────────────────────────────────────
    # RULE 6: VARIETY MECHANISM
    # Variety through ANGLE and EXECUTION, not by forcing weaker strategies
    # ──────────────────────────────────────────────
    rules.append("""
RULE 6 — VARIETY THROUGH ANGLE & EXECUTION
  IMPORTANT: Variety must NEVER come at the cost of performance.
  Do NOT use a weaker hook category, CTA type, or format just to be "different."
  Instead, create variety through ANGLE, STORY, and EXECUTION.

  ANGLE VARIETY (each hero approaches the product from a different life angle):
    - Angle 1: Personal story / emotional connection
    - Angle 2: Discovery / "I can't believe this exists"
    - Angle 3: Problem → Solution (pain point focus)
    - Angle 4: Value / price shock
    - Angle 5: Lifestyle integration / daily routine

  STORY VARIETY (each hero tells a different story even if the hook style is similar):
    - Different opening scenarios (morning routine, out with kids, at home, etc.)
    - Different social proof characters (husband, sister, friend, mom, neighbor)
    - Different pain points emphasized per video
    - Different "moment of discovery" for the product

  CTA LANGUAGE ROTATION (same CTA types, different words):
    - Rotate link reference: "link right here" / "orange cart" / "linked below" / "TikTok Shop link"
    - Rotate urgency flavor: scarcity ("selling out") / time ("won't last") / social proof ("everyone's grabbing these")
    - Never repeat the exact same CTA sentence across two videos

  TONE VARIETY (energy shifts across the 5 heroes):
    - 2 high energy (excited, amazed, can't-believe-it)
    - 2 conversational (warm, friend-to-friend, relaxed)
    - 1 serious/emotional (concern, safety, heartfelt)

  WHAT NOT TO VARY: Hook strategy. CTA structure. Segment timing. Format mix.
  These are performance-driven and should follow what the data says works.""")

    # ──────────────────────────────────────────────
    # RULE 7: ON-SCREEN TEXT RULES
    # ──────────────────────────────────────────────
    rules.append("""
RULE 7 — ON-SCREEN TEXT REQUIREMENTS
  Every hero video MUST include these on-screen text elements:
    1. HOOK TEXT (0-3s): Attention-grabbing text that reinforces the spoken hook
    2. FEATURE CALLOUTS (throughout): Technical specs, numbers, or claims as text overlays
       (especially important for specs the creator shouldn't say aloud)
    3. PRICE DISPLAY (last 25% of video): Show the price point prominently
    4. CTA TEXT (closing): "Link in bio" / "TikTok Shop" / arrow pointing to cart

  On-screen text must tell a COMPLETE story even with sound off.
  Data shows top performers layer spoken + text for dual engagement.""")

    # ──────────────────────────────────────────────
    # RULE 8: B-ROLL SHOT DISTRIBUTION RULES
    # Derived from frame-by-frame visual classification of 100 videos
    # ──────────────────────────────────────────────
    shot_breakdowns = research_data.get("shot_breakdowns", [])
    if shot_breakdowns:
        import json as _json

        # Build video lookup for performance correlation
        vid_map = {v["id"]: v for v in videos}

        total_frames_all = 0
        total_face_all = 0
        total_broll_all = 0
        broll_type_frames = {}
        dominant_type_perf = {}  # dominant_type -> [likes]

        # Face ratio buckets for performance analysis
        high_face_likes = []   # 70%+ face
        medium_face_likes = [] # 40-70% face
        low_face_likes = []    # <40% face

        for sb in shot_breakdowns:
            tf = sb.get("total_frames_analyzed", 0)
            ff = sb.get("face_frames", 0)
            bf = sb.get("broll_frames", 0)
            total_frames_all += tf
            total_face_all += ff
            total_broll_all += bf

            # Shot type frame counts
            sc = sb.get("shot_counts", {})
            if isinstance(sc, str):
                sc = _json.loads(sc)
            for k, v in sc.items():
                if k != "FACE":
                    broll_type_frames[k] = broll_type_frames.get(k, 0) + v

            # Performance by dominant broll type
            dt = sb.get("dominant_broll_type") or "none"
            vid = vid_map.get(sb["video_id"])
            likes = vid.get("likes", 0) if vid else 0
            if dt not in dominant_type_perf:
                dominant_type_perf[dt] = []
            dominant_type_perf[dt].append(likes)

            # Face ratio buckets
            if tf > 0:
                face_pct = ff / tf * 100
                if face_pct >= 70:
                    high_face_likes.append(likes)
                elif face_pct >= 40:
                    medium_face_likes.append(likes)
                else:
                    low_face_likes.append(likes)

        def _avg(lst):
            return sum(lst) / len(lst) if lst else 0

        face_pct_overall = total_face_all / total_frames_all * 100 if total_frames_all else 0
        broll_pct_overall = total_broll_all / total_frames_all * 100 if total_frames_all else 0

        # Build b-roll type breakdown lines
        broll_lines = []
        for btype, frames in sorted(broll_type_frames.items(), key=lambda x: x[1], reverse=True):
            pct = frames / total_broll_all * 100 if total_broll_all else 0
            broll_lines.append(f"    {btype}: {pct:.0f}% of b-roll ({frames} frames)")

        # Build performance-by-type lines
        type_perf_lines = []
        for dt, likes_list in sorted(dominant_type_perf.items(), key=lambda x: _avg(x[1]), reverse=True):
            type_perf_lines.append(f"    {dt}: {_avg(likes_list):,.0f} avg likes ({len(likes_list)} videos)")

        rules.append(f"""
RULE 8 — B-ROLL SHOT DISTRIBUTION (derived from frame-by-frame analysis of {len(shot_breakdowns)} videos)
  Overall composition: {face_pct_overall:.0f}% on-camera face | {broll_pct_overall:.0f}% b-roll cutaways
  This means top creators spend roughly HALF their video on camera talking and HALF on product/demo b-roll.

  B-ROLL TYPE BREAKDOWN (what the cutaway shots actually show):
{chr(10).join(broll_lines)}

  PERFORMANCE BY FACE-TO-BROLL RATIO:
    High face (70%+): {_avg(high_face_likes):,.0f} avg likes ({len(high_face_likes)} videos) — talking heads
    Medium face (40-70%): {_avg(medium_face_likes):,.0f} avg likes ({len(medium_face_likes)} videos) — mixed delivery
    Low face (<40%): {_avg(low_face_likes):,.0f} avg likes ({len(low_face_likes)} videos) — b-roll heavy

  PERFORMANCE BY DOMINANT B-ROLL TYPE:
{chr(10).join(type_perf_lines)}

  SHOOT GUIDE B-ROLL (filmable shots only — what the camera captures):
    - 50-65% PRODUCT CLOSEUP shots (product on surface, packaging, texture, label details)
    - 25-35% HANDS-ON DEMO shots (creator physically using/applying/holding the product)
    - 5-10% LIFESTYLE shots (product in natural setting — bathroom shelf, kitchen counter)
    - Every VOICEOVER line in a hero script should have a matching b-roll direction
    - B-roll shots must be filmable with ONE unit of the product
    - Do NOT include text overlays, graphics, split-screens, or animations in the b-roll shot list
      Those are EDIT elements, not shoot elements

  EDIT GUIDE ELEMENTS (post-production — NOT in the shoot guide):
    TEXT_CARD analysis shows {broll_type_frames.get("TEXT_CARD", 0)} frames across all videos.
    These are EDITOR-CREATED elements, not filmed shots. They include:
    - Price comparison graphics (e.g., "$23 vs $250")
    - Feature/ingredient callout text overlays
    - Before/after split-screen composites
    - TikTok Shop cart icon animations
    - Duration/value text cards (e.g., "Lasts 6-8 weeks")
    - Any on-screen text that isn't physically in the room
    These go EXCLUSIVELY in the Edit Guide, never in the Shoot Guide

  B-ROLL PACING RULE:
    Do NOT cluster all b-roll together. Top performers interleave face → b-roll → face.
    A typical hero video pattern: FACE (hook) → FACE (story) → B-ROLL (product reveal) →
    FACE (reaction) → B-ROLL (demo) → FACE (CTA close).
    The editor uses b-roll cuts to cover VOICEOVER lines — plan accordingly.""")

    return "\n".join(rules)


def build_research_context(research_data, patterns):
    """Build a rich context string from research data for the content generation prompt."""

    context_parts = []

    # Benchmarks
    b = patterns["benchmarks"]
    context_parts.append(f"""
=== PERFORMANCE BENCHMARKS ===
Total videos analyzed: {b['total_videos_analyzed']}
Average likes: {b['avg_likes']:,.0f}
Average shares: {b['avg_shares']:,.0f}
Average video duration: {b['avg_duration_seconds']:.0f} seconds
""")

    # Top hooks by engagement
    context_parts.append("=== TOP-PERFORMING HOOK PATTERNS ===")
    for cat, data in sorted(patterns["hook_categories"].items(),
                            key=lambda x: x[1]["avg_engagement"], reverse=True):
        context_parts.append(f"\nHook Style: {cat.upper()} (avg engagement: {data['avg_engagement']:,.0f})")
        for ex in data["examples"][:3]:
            context_parts.append(f'  Example: "{ex}"')

    # CTA patterns
    context_parts.append("\n=== CTA PATTERNS ===")
    for ctype, data in sorted(patterns["cta_types"].items(),
                              key=lambda x: x[1]["count"], reverse=True):
        most_common_pos = max(set(data["positions"]), key=data["positions"].count)
        context_parts.append(f"\nCTA Type: {ctype} (used {data['count']}x, typically in {most_common_pos})")
        for ex in data["examples"][:2]:
            context_parts.append(f'  Example: "{ex}"')

    # Video formats
    context_parts.append("\n=== VIDEO FORMATS ===")
    for fmt, count in sorted(patterns["format_counts"].items(), key=lambda x: x[1], reverse=True):
        context_parts.append(f"  {fmt}: {count} videos")

    # Full transcripts of top videos for reference
    context_parts.append("\n=== TOP VIDEO TRANSCRIPTS (for reference) ===")
    for video in research_data["videos"][:5]:
        vid = video["id"]
        if vid in research_data["transcripts"]:
            t = research_data["transcripts"][vid]
            context_parts.append(f"\n--- @{video['creator_username']} | {video.get('likes',0):,} likes ---")
            context_parts.append(f"Product: {video.get('product_name', 'Unknown')}")
            context_parts.append(f"Script: {t['full_transcript'][:500]}...")

    # Performance insights - why each video worked
    context_parts.append("\n=== WHY THESE VIDEOS PERFORMED (AI Analysis) ===")
    for video in research_data["videos"][:10]:
        insight = video.get("performance_insight", "")
        if insight:
            context_parts.append(f"\n@{video['creator_username']} ({video.get('likes',0):,} likes):")
            context_parts.append(f"  {insight}")

    # Visual notes
    context_parts.append("\n=== VISUAL STRATEGIES ===")
    for vis in research_data["visual_notes"][:5]:
        context_parts.append(f"\nFormat: {vis.get('format_type')} | Hook style: {vis.get('hook_style')}")
        context_parts.append(f"Notes: {vis.get('overall_notes', '')[:200]}")

    # On-screen text strategies
    context_parts.append("\n=== ON-SCREEN TEXT STRATEGIES ===")
    for ost in research_data["onscreen_text"][:10]:
        context_parts.append(f"  [{ost.get('text_type')}] {ost['text_content']}")

    # Hook format analysis (comment_reply, standard, etc.)
    context_parts.append("\n=== HOOK FORMAT ANALYSIS ===")
    hook_formats = {}
    for v in research_data["videos"]:
        hf = v.get("hook_format", "standard")
        if hf not in hook_formats:
            hook_formats[hf] = {"count": 0, "total_likes": 0, "examples": []}
        hook_formats[hf]["count"] += 1
        hook_formats[hf]["total_likes"] += v.get("likes", 0)
        if len(hook_formats[hf]["examples"]) < 3:
            hook_formats[hf]["examples"].append(f"@{v.get('creator_username')} - {v.get('product_name', 'Unknown')} ({v.get('likes',0):,} likes)")
    for fmt, data in sorted(hook_formats.items(), key=lambda x: x[1]["total_likes"], reverse=True):
        avg = data["total_likes"] / data["count"] if data["count"] > 0 else 0
        context_parts.append(f"\nFormat: {fmt} ({data['count']} videos, avg {avg:,.0f} likes)")
        for ex in data["examples"]:
            context_parts.append(f"  {ex}")

    # Content type distribution with engagement data
    context_parts.append("\n=== CONTENT TYPE PERFORMANCE ===")
    content_types = {}
    for v in research_data["videos"]:
        ct = v.get("content_type", "voiceover")
        if ct not in content_types:
            content_types[ct] = {"count": 0, "total_likes": 0, "total_shares": 0, "examples": []}
        content_types[ct]["count"] += 1
        content_types[ct]["total_likes"] += v.get("likes", 0)
        content_types[ct]["total_shares"] += v.get("shares", 0)
        if len(content_types[ct]["examples"]) < 2:
            content_types[ct]["examples"].append(f"@{v.get('creator_username')} - {v.get('product_name', 'Unknown')} ({v.get('likes',0):,} likes)")
    for ct, data in sorted(content_types.items(), key=lambda x: x[1]["total_likes"], reverse=True):
        avg_likes = data["total_likes"] / data["count"] if data["count"] > 0 else 0
        avg_shares = data["total_shares"] / data["count"] if data["count"] > 0 else 0
        context_parts.append(f"\n  {ct}: {data['count']} videos | avg {avg_likes:,.0f} likes | avg {avg_shares:,.0f} shares")
        for ex in data["examples"]:
            context_parts.append(f"    {ex}")

    # Voice delivery breakdown — how creators deliver their voice relative to visuals
    context_parts.append("\n=== VOICE DELIVERY STYLES ===")
    voice_styles = {}
    for v in research_data["videos"]:
        vd = v.get("voice_delivery", "on_camera")
        if vd not in voice_styles:
            voice_styles[vd] = {"count": 0, "total_likes": 0, "total_shares": 0}
        voice_styles[vd]["count"] += 1
        voice_styles[vd]["total_likes"] += v.get("likes", 0)
        voice_styles[vd]["total_shares"] += v.get("shares", 0)
    for vd, data in sorted(voice_styles.items(), key=lambda x: x[1]["total_likes"], reverse=True):
        avg_likes = data["total_likes"] / data["count"] if data["count"] > 0 else 0
        label = {
            "on_camera": "ON CAMERA (creator speaks directly to lens)",
            "voiceover_offscreen": "VOICEOVER OFFSCREEN (narrates over b-roll, not on camera)",
            "no_voice": "NO VOICE (text only with background music)",
        }.get(vd, vd)
        context_parts.append(f"  {label}: {data['count']} videos | avg {avg_likes:,.0f} likes")

    # B-roll shot composition from frame-by-frame analysis
    shot_breakdowns = research_data.get("shot_breakdowns", [])
    if shot_breakdowns:
        import json as _json
        context_parts.append("\n=== B-ROLL VISUAL COMPOSITION (frame-by-frame analysis) ===")
        context_parts.append(f"Analyzed {len(shot_breakdowns)} videos frame-by-frame with OpenCV visual classification.")

        # Show top 5 videos' shot composition for reference
        vid_map = {v["id"]: v for v in research_data["videos"]}
        sb_with_perf = []
        for sb in shot_breakdowns:
            vid = vid_map.get(sb["video_id"])
            if vid:
                sb_with_perf.append((sb, vid.get("likes", 0), vid.get("creator_username", "unknown")))
        sb_with_perf.sort(key=lambda x: x[1], reverse=True)

        context_parts.append("\nTop 5 videos — visual breakdown:")
        for sb, likes, username in sb_with_perf[:5]:
            tf = sb.get("total_frames_analyzed", 1)
            ff = sb.get("face_frames", 0)
            face_pct = ff / tf * 100 if tf else 0
            dom = sb.get("dominant_broll_type") or "N/A"
            sec = sb.get("secondary_broll_type") or "N/A"
            context_parts.append(
                f"  @{username} ({likes:,} likes): {face_pct:.0f}% face, "
                f"dominant b-roll={dom}, secondary={sec}"
            )

    return "\n".join(context_parts)


def generate_content_plan_prompt(product_brief, research_context, structural_rules="", web_research="", persona_context=""):
    """Build the prompt for the content generation engine."""

    prompt = f"""You are the world's most successful TikTok affiliate content strategist. Your mission is to create content plans that generate maximum engagement and conversions on TikTok Shop.

You have access to a database of top-performing TikTok affiliate videos that have been analyzed in detail. The STRUCTURAL RULES below were derived directly from this data and are MANDATORY — every script you write must follow them exactly.

{persona_context}

{structural_rules}

=== RESEARCH DATA FROM TOP-PERFORMING AFFILIATE VIDEOS ===
{research_context}

=== PRODUCT INFORMATION ===
{product_brief}

{f"=== ADDITIONAL PRODUCT RESEARCH FROM WEB ===" + chr(10) + web_research if web_research else ""}

=== YOUR TASK ===
Based on the STRUCTURAL RULES and research data above, generate a TikTok affiliate content plan designed to maximize content output from a SINGLE SHOOT SESSION.

CRITICAL: Before writing any script, verify it against the STRUCTURAL RULES above. Every hero video MUST have all 4 segments (Hook → Story → Product Value → CTA Close). Every hero video MUST contain at minimum 3 spoken CTAs (1 value_proposition in middle + 1 direct_link + 1 urgency in closing). If a script doesn't meet these requirements, rewrite it before outputting.

The output will be split into THREE DOCUMENTS, so structure your response accordingly:

════════════════════════════════════════
DOCUMENT 1: SHOOT GUIDE (for the model/talent)
════════════════════════════════════════
This is a clean, simple reference the model takes to the shoot. She needs to know EXACTLY what to capture. Nothing about editing, timing, strategy, or CTAs. Just what to film and say.

Structure it as:

A) MASTER SHOT LIST
   A consolidated list of EVERY unique shot needed across all 10 videos. Group by type:

   - TALKING HEAD SHOTS (TH1, TH2, etc.): Each TH corresponds to one hero video's full script. But DON'T write it as one long paragraph. Break each script into bite-sized LINES (TH1-a, TH1-b, TH1-c, etc.) — each line is 1-2 sentences max. She films each line as its own short take, and the editor stitches them together.
     Tag each line as either "ON CAMERA" (she's looking at camera) or "VOICEOVER" (she says it but the editor covers it with b-roll — she can read this one).
     Also include a short setup note for each TH (where to sit, energy level, what to hold).

   - B-ROLL SHOTS (B1, B2, etc.): FILMABLE shots only — things the camera captures. SHORT descriptions (one line each). Don't over-direct — give the key action/framing and let the creator interpret. E.g., "B3: Slow-mo pillow fluff. Get 5+ takes."
     CRITICAL: Do NOT include text overlays, price graphics, split-screen composites, animations, or any post-production elements in this section. Those are EDIT elements that go in the Edit Guide only. Every b-roll shot must be something the model can physically film with a camera.

   - VOICEOVER AUDIO (VO1, VO2, etc.): Short audio-only lines for remix videos. Record at end of shoot.

B) PROPS & SETUP
   - What products/items to have on hand
   - Locations/settings needed (bathroom, bedroom, kitchen, etc.)
   - Wardrobe/look notes if relevant

Keep this CONCISE. The model should be able to scan it in 2 minutes and know exactly what to do.

════════════════════════════════════════
DOCUMENT 2: EDIT GUIDE (for the video editors)
════════════════════════════════════════
This is the full assembly document. The editor uses this to build all 10 videos from the captured footage.

SECTION A: HERO VIDEOS (5 videos)

CRITICAL RULE FOR HERO VIDEOS: Each hero video MUST have a FULL SPOKEN SCRIPT that runs 30-50+ seconds. This is the #1 priority. The research data shows top-performing videos have continuous voiceover or on-camera dialogue throughout. A hero video is NOT a 3-second hook followed by silent b-roll. The creator should be TALKING for the vast majority of the video — explaining, demonstrating, reacting, persuading.

For each hero video, provide TWO SEPARATE sections:

1) SPOKEN SCRIPT (this is what the creator SAYS — the full voiceover/on-camera dialogue):
   - Write the COMPLETE spoken script as a continuous piece of dialogue, timestamped
   - This should be 30-50+ seconds of actual speech
   - It should include: the hook (first 3s), product explanation, personal experience/story, feature highlights, social proof, and CTA
   - Write it the way a real person talks — conversational, not robotic
   - Reference which segments are on-camera (TH labels) vs voiceover
   - The spoken script should tell a complete story on its own

2) VISUAL TIMELINE (this is what the EDITOR sees on screen at each moment):
   - Timestamped assembly showing which shot (TH1, B3, etc.) plays at each moment
   - This is the edit assembly guide — what footage goes where
   - Mark where on-screen text overlays appear

Also provide:
- Concept name and angle
- Format (talking head, POV, product demo, etc.)
- Duration (30-55 seconds — match the research data)
- Music suggestion
- Hook: Exact spoken opening line (first 3 seconds) + on-screen text
- CTA strategy: All CTAs with timestamps, types, and exact wording
- On-screen text plan: Every text overlay with timestamp and placement

DO NOT confuse the spoken script with the visual timeline. The spoken script is what comes out of the creator's mouth. The visual timeline is what the editor puts on screen. They run in parallel.

SECTION B: B-ROLL REMIX VIDEOS (5 videos)
These are ADDITIONAL videos built from the same b-roll footage shot during the hero videos. Shorter, simpler, easy to edit.

Each remix video can be:
- Text-only (music + on-screen text, no voiceover)
- Simple voiceover (short audio recorded at end of shoot)
- Hybrid (music + minimal voiceover + on-screen text)

Choose the format that the research data suggests will perform best for each angle.

For each remix video, provide:
- Concept name and angle (different from hero videos)
- Format: text-only, simple voiceover, or hybrid
- Duration: SHORT (15-30 seconds ideal)
- Music suggestion
- Shot assembly: Reference specific shot labels from the master list (e.g., "B1, B5, B3 — in that order"). Include the sequence.
- On-screen text script: Full timestamped text. This IS the video's message.
- Voiceover script (if applicable): Reference VO labels from the master list
- CTA: Simple closing CTA

IMPORTANT for remix videos:
- They must NOT require any NEW footage — only reuse shots from the master list
- Keep them punchy and fast-paced (quick cuts)
- Each should take a DIFFERENT angle than its source hero video
- The on-screen text should tell a complete story even with sound off

SECTION C: UPLOAD DETAILS
- Recommended hashtags (mix and match 8-12 per video)
- Suggested captions/titles (5-8 options)
- Posting schedule notes

════════════════════════════════════════
DOCUMENT 3: DIRECTOR'S OVERVIEW (1-2 page quick-scan for the creative director)
════════════════════════════════════════
This is a CONCISE strategic overview. The reader needs to scan it in 60 seconds and understand the plan. NO long paragraphs. NO essays. Keep it tight.

Structure:
1) STRATEGY — 4-5 bullet points covering: what angles we're using, why, shoot structure, how hero vs remix work together
2) POSTING CADENCE — One or two sentences: when to post what, what order, best times
3) HERO VIDEOS — For each of the 5 hero videos:
   - Title, duration, format (e.g., "Problem → Solution")
   - 2-3 SHORT bullet points: why this angle, what data supports it, expected performance
4) REMIX VIDEOS — Same format, 2 bullets each

KEEP IT SHORT. Each bullet should be 1-2 sentences max. The entire document should fit on 1-2 pages when printed. Reference specific data points (creator names, engagement numbers, patterns) but don't write paragraphs about them.

Be specific, actionable, and grounded in the actual research data. Reference specific patterns you observed. The goal is to create content that will outperform the competition and drive maximum affiliate conversions. Keep all three documents CONCISE and production-ready — no fluff, just what's needed to shoot, edit, and understand the strategy.

═══════════════════════════════════════
JSON OUTPUT SCHEMAS (follow these EXACTLY)
═══════════════════════════════════════

OUTPUT YOUR RESPONSE AS THREE JSON BLOCKS, clearly labeled.

SHOOT GUIDE JSON:
{{"productName":"...","productSubtitle":"...","totalVideos":10,
"masterShotList":{{
  "talkingHeadShots":[
    {{"label":"TH1","concept":"Hero 1 — Concept Name","note":"Setup/energy note",
      "lines":[
        {{"id":"TH1-a","type":"ON CAMERA","text":"First line she says..."}},
        {{"id":"TH1-b","type":"VOICEOVER","text":"Second line (editor covers with b-roll)..."}},
        {{"id":"TH1-c","type":"ON CAMERA","text":"Third line..."}}
      ]
    }}
  ],
  "brollShots":[{{"label":"B1","description":"Short one-line direction."}}],
  "voiceoverAudio":[{{"label":"VO1","concept":"Remix concept","script":"Line to record.","context":"Tone note."}}]
}},
"propsAndSetup":{{"products":["..."],"locations":["..."],"wardrobe":["..."]}}
}}

EDIT GUIDE JSON:
{{"productName":"...","productSubtitle":"...",
"heroVideos":[
  {{"title":"Hero 1 — Concept Name","angle":"...","format":"...","duration":"45s","music":"...",
    "hook":{{"spoken":"Opening line...","onScreen":"Text overlay..."}},
    "spokenScript":[
      {{"time":"0-4s","text":"Spoken line here...","onCamera":true}},
      {{"time":"4-10s","text":"Next spoken line...","onCamera":false}}
    ],
    "visualTimeline":[
      {{"time":"0-4s","shot":"TH1","description":"On camera delivering hook"}},
      {{"time":"4-10s","shot":"B3","description":"Product demo b-roll"}}
    ],
    "ctas":["[30s] Soft CTA: Link below..."],
    "onScreenText":["[0s] Hook text overlay","[30s] Price + CTA"]
  }}
],
"remixVideos":[
  {{"title":"Remix 1 — Concept","angle":"...","format":"text-only","duration":"20s","music":"...",
    "shotAssembly":["B3 → B2 → B5 (quick cuts)"],
    "onScreenText":[{{"time":"0-3s","text":"Hook text..."}},{{"time":"3-8s","text":"Feature..."}}],
    "voiceover":"VO1 (if applicable)",
    "cta":"Simple closing CTA"
  }}
],
"uploadDetails":{{"hashtags":["#tag1","#tag2"],"captions":["Caption option 1..."],"postingNotes":"Schedule notes..."}}
}}

DIRECTOR'S OVERVIEW JSON:
{{"productName":"...","productSubtitle":"...",
"strategy":["Bullet 1...","Bullet 2...","Bullet 3...","Bullet 4..."],
"postingCadence":"One or two sentences...",
"heroVideos":[{{"title":"Hero 1 — Name","duration":"45s","format":"Problem → Solution","rationale":["Short bullet 1","Short bullet 2","Short bullet 3"]}}],
"remixVideos":[{{"title":"Remix 1 — Name","duration":"20s","format":"Text-only","rationale":["Short bullet 1","Short bullet 2"]}}]
}}
"""
    return prompt


def get_creator_persona(creator_name=None, creator_id=None):
    """Load a creator persona from the database."""
    if creator_id:
        result = supabase.table("creators").select("*").eq("id", creator_id).execute()
    elif creator_name:
        result = supabase.table("creators").select("*").ilike("name", creator_name).execute()
    else:
        return None

    if result.data:
        return result.data[0]
    return None


def build_persona_context(persona):
    """Build a persona context string to inject into the content generation prompt."""
    if not persona:
        return ""

    parts = []
    parts.append("=== CREATOR PERSONA ===")
    parts.append(f"Name: {persona.get('name', 'Unknown')}")
    parts.append(f"Age: {persona.get('age_range', 'N/A')}")
    parts.append(f"Background: {persona.get('ethnicity', 'N/A')}")

    if persona.get("is_parent"):
        parts.append(f"Family: Mom — {persona.get('kids_description', 'has kids')}")

    parts.append(f"\nOn-Camera Style: {persona.get('on_camera_style', 'N/A')}")
    parts.append(f"Tone: {persona.get('tone', 'N/A')}")
    parts.append(f"Energy: {persona.get('energy_level', 'N/A')}")

    if persona.get("identity_angles"):
        parts.append(f"Identity angles: {', '.join(persona['identity_angles'])}")

    if persona.get("language_notes"):
        parts.append(f"\nLanguage & Script Constraints:\n{persona.get('language_notes')}")

    if persona.get("lifestyle_notes"):
        parts.append(f"Lifestyle: {persona.get('lifestyle_notes')}")

    if persona.get("target_audience"):
        parts.append(f"Target audience: {persona.get('target_audience')}")

    if persona.get("account_stage"):
        parts.append(f"Account stage: {persona.get('account_stage')}")

    if persona.get("additional_notes"):
        parts.append(f"\nAdditional notes: {persona.get('additional_notes')}")

    # Production constraints (budget, props, equipment)
    if persona.get("production_tier") and persona["production_tier"] != "standard":
        constraints = persona.get("production_constraints", {})
        parts.append(f"\nProduction tier: {persona['production_tier'].upper()}")

        if constraints.get("rule"):
            parts.append(f"Production rule: {constraints['rule']}")

        if constraints.get("tier_1_free"):
            parts.append(f"FREE props (always available): {', '.join(constraints['tier_1_free'])}")
        if constraints.get("tier_2_likely_has"):
            parts.append(f"LIKELY HAS (no purchase needed): {', '.join(constraints['tier_2_likely_has'])}")
        if constraints.get("tier_3_requires_purchase"):
            max_items = constraints.get("max_purchased_props_per_shoot", 2)
            parts.append(f"REQUIRES PURCHASE (limit {max_items} per shoot): {', '.join(constraints['tier_3_requires_purchase'])}")

        if constraints.get("notes"):
            parts.append(f"Budget notes: {constraints['notes']}")

        parts.append("")
        parts.append("B-ROLL BUDGET RULE (MANDATORY):")
        parts.append("All b-roll shots should be achievable with the product itself + items the creator already owns.")
        parts.append("If a shot requires purchasing an additional prop, note it in the props list — but keep purchased items to a minimum.")
        parts.append(f"Max purchased props per shoot: {constraints.get('max_purchased_props_per_shoot', 2)}")
        parts.append("Do NOT tag every shot with [FREE] or any other prefix — just describe what to film.")

    parts.append("")
    parts.append("IMPORTANT: All scripts, hooks, CTAs, and creative decisions MUST be written FOR this specific creator.")
    parts.append("- Match her tone and energy — don't write scripts that sound like a different person")
    parts.append("- Consider what angles feel authentic for her (her background, life stage, identity)")
    parts.append("- The scripts should sound like words that would naturally come out of HER mouth")
    parts.append("- Factor in her account stage when recommending strategies")
    if persona.get("production_tier") == "budget":
        parts.append("- Keep b-roll PRACTICAL and AFFORDABLE — prioritize the product itself, close-ups, textures, and items she already has")

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# HOLIDAY / SEASONAL AWARENESS
# ═══════════════════════════════════════════════════════════════

def get_upcoming_holidays(lookahead_days=42):
    """
    Check the holiday_calendar for any holidays whose script window
    includes today. Returns a list of holiday dicts with all context.

    The script_window is when content should be PRODUCED (4-6 weeks
    before the holiday). If today falls in that window, the holiday
    is active and should influence content generation.

    Args:
        lookahead_days: Not used directly — the script_window dates
                        in the DB already encode the timing. This is
                        a fallback for future use.
    """
    from datetime import date
    today = date.today().isoformat()

    result = supabase.table("holiday_calendar").select("*") \
        .lte("script_window_start", today) \
        .gte("script_window_end", today) \
        .order("holiday_date") \
        .execute()

    return result.data if result.data else []


def build_holiday_context(holidays, product_brief=""):
    """
    Build a prompt injection block for active holidays.

    This is NOT forced into every script. It provides context so the
    model can decide whether the product-holiday fit makes sense.
    The model is explicitly told: only lean into holiday angles when
    the product naturally fits. Evergreen content is always the default.

    Args:
        holidays: List of holiday dicts from get_upcoming_holidays()
        product_brief: Product description (used for fit assessment)

    Returns:
        String block to inject into prompts, or empty string if no holidays.
    """
    if not holidays:
        return ""

    holiday_blocks = []
    for h in holidays:
        gift_cats = ", ".join(h.get("gift_categories", []))
        content_angles = ", ".join(h.get("content_angles", []))
        seasonal = ", ".join(h.get("seasonal_angles", []))
        notes = h.get("notes", "")

        holiday_blocks.append(f"""  • {h['holiday_name']} — {h['holiday_date']}
    Gift categories: {gift_cats}
    Content angles: {content_angles}
    Seasonal vibes: {seasonal}
    Notes: {notes}""")

    holidays_list = "\n".join(holiday_blocks)

    return f"""
═══════════════════════════════════════════════════
HOLIDAY / SEASONAL AWARENESS (data-driven)
═══════════════════════════════════════════════════
The following holidays are approaching and we are currently in their content production window.
If (and ONLY if) the product is a natural fit for the holiday, lean into it.

ACTIVE HOLIDAYS:
{holidays_list}

HOW TO USE THIS:
- If the product fits a holiday gifting angle, make 1-2 of the 5 hero scripts holiday-themed
  and 1-2 remix videos holiday-themed. The rest should be EVERGREEN (no holiday reference).
- If the product does NOT naturally fit any active holiday, IGNORE this section entirely.
  A protein powder does not need a Valentine's Day angle. A fidget toy set absolutely
  needs an Easter basket angle.
- Holiday-themed scripts should weave the occasion in NATURALLY — not slap a holiday
  label on generic content. Study how top creators tie products to moments:
  "I have to show you this before my kids come downstairs — this is their Easter present"
  NOT: "This would make a great Easter gift!"
- Scarcity and urgency are REAL during holiday windows. "It's back in stock" and
  "grab it before it sells out" hit different when there's a deadline.
- For gifting angles: the BUYER and the RECIPIENT are often different people.
  Scripts should speak to the buyer ("parent of the year" energy) not the end user.
- DO NOT force holiday content. If the fit isn't there, 5/5 scripts should be evergreen.

SPLIT GUIDELINE (when holiday fits):
- 2-3 hero scripts: EVERGREEN (work year-round)
- 1-2 hero scripts: HOLIDAY-THEMED (tied to the active holiday)
- 2-3 remixes: EVERGREEN
- 1-2 remixes: HOLIDAY-THEMED
"""


def get_database_stats():
    """Get a quick summary of what's in the research database."""
    videos = supabase.table("tiktok_videos").select("id, likes, creator_username").execute().data
    hooks = supabase.table("hook_patterns").select("id").execute().data
    ctas = supabase.table("cta_patterns").select("id").execute().data

    total_likes = sum(v.get("likes", 0) for v in videos)
    unique_creators = len(set(v.get("creator_username") for v in videos))

    return {
        "total_videos": len(videos),
        "total_hooks": len(hooks),
        "total_cta_patterns": len(ctas),
        "total_likes_across_db": total_likes,
        "unique_creators": unique_creators,
    }


# ═══════════════════════════════════════════════════════════════
# PASS 0: PRODUCT RESEARCH
# ═══════════════════════════════════════════════════════════════

def build_product_research_prompt(raw_research_text):
    """
    PASS 0: Takes raw web research and generates a structured, comprehensive
    product brief optimized for TikTok affiliate content generation.

    This prompt tells the model to synthesize messy web data into a
    dense, actionable product brief that downstream passes can use.
    """

    prompt = f"""You are a product research analyst building a comprehensive product brief
for a TikTok affiliate content team. Your job is to take raw web research and distill it
into the most useful, dense, actionable product brief possible.

The content team will use this brief to write TikTok scripts. They need:
- SPECIFIC details they can reference in scripts (not vague marketing claims)
- Real results/timelines users have reported (e.g., "visible results in 2 days")
- Ingredients that sound impressive when spoken aloud (the "science" hook)
- Price comparisons and value angles
- What makes this product DIFFERENT from competitors
- What real people actually SAY about it (the language they use)
- Any controversy, hype, or viral moments
- Sensory details (texture, smell, feel, appearance)
- EXACT NUTRITION FACTS if it's a food/supplement/snack (calories per serving, serving size, macro breakdown, sugar content)
- EXACT PACKAGE SIZE and supply duration (how many servings per bag/bottle, how long it lasts at typical usage)
- Certifications and sourcing that are ACTUALLY ON THE LABEL (organic, non-GMO, vegan, gluten-free — only include what's real)
- KEY INGREDIENT LIST — what's actually in it, sourced from the label, not marketing copy

═══════════════════════════════════════
RAW WEB RESEARCH
═══════════════════════════════════════
{raw_research_text}

═══════════════════════════════════════
YOUR TASK
═══════════════════════════════════════
Synthesize the above into a structured product brief. Use this EXACT format:

PRODUCT: [Full product name]
BRAND: [Brand name + one-line brand context]
PRICE: [Price + any deals/bundles + value comparison to competitors]
CATEGORY: [Product category for TikTok audience]

WHAT IT IS (1-2 sentences, plain English):
[Describe what this product actually IS in the simplest terms]

PACKAGE DETAILS (exact facts from the listing/label):
- Size: [Exact weight/volume — e.g., "6.4 oz / 181g"]
- Servings: [Number of servings per package — e.g., "About 6 servings"]
- Serving size: [e.g., "1 oz (28g)" or "2 patches"]
- Supply duration: [How long one package lasts at typical use — e.g., "~1 month daily" or "~36 days"]
- Form factor: [Bag, bottle, jar, box, pouch — what it looks like physically]

NUTRITION FACTS (if food/supplement — copy EXACTLY from label, skip if not applicable):
- Calories per serving: [e.g., "50 calories"]
- Key macros: [Fat, carbs, protein, sugar, fiber — only the notable ones]
- Standout nutrition angle: [The 1-2 facts a creator would actually mention — e.g., "only 50 cals and basically no fat" or "12g protein per serving"]

HERO INGREDIENTS (the "science" that sounds impressive):
- [Ingredient 1]: [What it does in plain English + any specific % or concentration]
- [Ingredient 2]: [Same]
- [Continue for all noteworthy ingredients]

WHAT IT ACTUALLY DOES (specific claims with evidence):
- [Claim 1]: [Evidence — user reports, clinical data, timelines]
- [Claim 2]: [Same]
- [Continue]

REAL USER RESULTS (what actual people said — their words, not marketing):
- "[Paraphrase of real user feedback]" — [Source/context]
- "[Another]" — [Source/context]
- [Continue for all notable user feedback]

SENSORY DETAILS (what the creator can describe on camera):
- Texture: [What it feels like]
- Appearance: [What it looks like in the package, on skin, etc.]
- Application: [How it goes on — easy, messy, satisfying, etc.]
- Timeline: [When do you notice anything — immediately, next morning, 2 weeks, etc.]

COMPETITOR COMPARISON (the "why THIS one" angle):
- vs [Competitor 1]: [Price/quality comparison]
- vs [Competitor 2]: [Same]
- [The one-line killer comparison for scripts]

VIRAL/HYPE CONTEXT (what's making this product buzz right now):
- [Any TikTok trends, viral moments, celebrity mentions, sold-out history]
- [Why NOW is the time to talk about it]

BUNDLE/DEAL DETAILS (for CTA scripting):
- [Exactly what's included]
- [Any limited-time aspects]
- [Any free gifts or bonuses]

VALUE PROPOSITIONS (ranked by script-worthiness):
1. [Most compelling angle for a TikTok video]
2. [Second most compelling]
3. [Third]
4. [Continue]

POTENTIAL SCRIPT ANGLES (specific scenarios a creator could film):
1. [Angle — e.g., "morning routine reveal where the patches are the star"]
2. [Angle — specific, filmable, not generic]
3. [Continue]

Output the brief. Be DENSE — every line should contain usable information.
Do NOT pad with filler or repeat the same point in different words.
Do NOT include any text outside the structured format above.

ACCURACY IS CRITICAL:
- Only include nutrition facts, certifications, and claims that are ACTUALLY in the source data.
- If the research doesn't contain exact calories or nutrition info, say "NOT FOUND IN RESEARCH — verify from label."
- If supply duration is estimated, label it as estimated and show your math (e.g., "~36 days based on 6 servings at one per week").
- NEVER fabricate nutrition numbers, certifications, or clinical results.
- When in doubt, flag it with "[VERIFY]" so the content team knows to double-check."""

    return prompt


def generate_product_research(product_name, search_results_text):
    """
    PASS 0 entry point.

    Takes a product name and pre-fetched web search results text,
    returns the prompt to send to Claude for synthesis into a structured brief.

    The actual web searching happens OUTSIDE this function (in the pipeline runner
    or manually) because web search requires async/API calls that vary by environment.

    Args:
        product_name: The product name/title
        search_results_text: Concatenated text from web research
            (product pages, reviews, press releases, social media, etc.)

    Returns:
        dict with:
            - research_prompt: The prompt to send to Claude
            - raw_research: The raw text that was provided
    """
    print("=" * 60)
    print("  PASS 0: Product Research")
    print("=" * 60)
    print(f"  Product: {product_name}")
    print(f"  Raw research: {len(search_results_text)} chars from web sources")

    prompt = build_product_research_prompt(search_results_text)
    print(f"  Research synthesis prompt ready ({len(prompt)} chars)")

    return {
        "stage": "research_needed",
        "research_prompt": prompt,
        "raw_research": search_results_text,
        "product_name": product_name,
    }


def generate_hooks_prompt(product_brief, research_data, persona_context="", hook_template_constraints=""):
    """
    PASS 2: Isolated hook generation.

    This prompt does ONE thing: generate scroll-stopping hooks.
    It gets the full database of real hooks for reference but NOTHING ELSE —
    no script structure, no CTAs, no b-roll, no edit guide schemas.
    The model's entire focus is on hooks.

    If hook_template_constraints is provided (Improvement #2), hooks are
    generated from proven structural templates rather than invented from scratch.
    """

    # Get all hooks sorted by engagement
    hooks = research_data["hook_patterns"]
    sorted_hooks = sorted(hooks, key=lambda h: (h.get("avg_engagement_rate") or 0), reverse=True)

    hook_lines = []
    for i, h in enumerate(sorted_hooks):
        eng = h.get("avg_engagement_rate") or 0
        hook_lines.append(f'  #{i+1} ({eng:,.0f}): "{h["hook_text"]}"')

    # Build the output format section — changes based on whether templates are active
    if hook_template_constraints:
        output_rules = """OUTPUT FORMAT (exactly this, nothing else):

HOOK 1: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 2: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 3: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 4: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 5: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 6: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 7: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 8: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 9: [hook text] | TEMPLATE: [HT# or ORIGINAL]
HOOK 10: [hook text] | TEMPLATE: [HT# or ORIGINAL]

That's it. Ten hooks with template IDs. No commentary. No explanations."""
    else:
        output_rules = """OUTPUT FORMAT (exactly this, nothing else):

HOOK 1: [hook text]
HOOK 2: [hook text]
HOOK 3: [hook text]
HOOK 4: [hook text]
HOOK 5: [hook text]
HOOK 6: [hook text]
HOOK 7: [hook text]
HOOK 8: [hook text]
HOOK 9: [hook text]
HOOK 10: [hook text]

That's it. Ten hooks. No commentary. No explanations. Just raw hooks."""

    prompt = f"""You write TikTok hooks. That's all you do. You are the best in the world at it.

Below are real hooks from top-performing TikTok affiliate videos in our database, ranked by engagement. Study every single one.

{chr(10).join(hook_lines)}

Look at what performs. These hooks are raw, messy, weird, blurted out, incomplete.
They sound like a real person, not a copywriter. Some are gross. Some have nothing
to do with the product. Some are 2-3 words. THAT is why they work.

{persona_context}

PRODUCT (for context only — do NOT mention in hooks):
{product_brief}
{hook_template_constraints}
YOUR TASK:
Write 10 hooks for this product. Not 5. TEN. We'll pick the best 5.

RULES:
- Under 12 words each. Ideally under 8. Some should be 3-5 words.
- ZERO mention of the product, brand, category, features, or what it does.
- Each hook must leave a MASSIVE unanswered question. Incomplete. Unresolved.
- They must sound SPOKEN — like someone blurted it out, not typed it.
- They must be raw, not polished. Messy > clean. Weird > safe.
- The hooks must follow the STRUCTURAL PATTERNS of top-performing categories.
- TEST EACH HOOK: Read it out loud. If it doesn't make a listener say "wait, WHAT?" — throw it out and write a new one.
- DO NOT explain, justify, or annotate the hooks. Just write them.

{output_rules}
"""
    return prompt


def generate_hooks(product_brief, product_category=None, creator_name=None, creator_id=None):
    """
    PASS 2 entry point: Generate hooks in isolation.

    Returns the hooks-only prompt ready to pass to Claude.
    """
    print("=" * 60)
    print("  TikTok Hook Generator (Isolated Pass)")
    print("=" * 60)

    # Load persona
    persona_context = ""
    if creator_name or creator_id:
        persona = get_creator_persona(creator_name=creator_name, creator_id=creator_id)
        if persona:
            persona_context = f"CREATOR: {persona.get('name', 'Unknown')}, {persona.get('age_range', 'N/A')}, {persona.get('tone', 'N/A')} tone. Hooks should sound like they'd come out of her mouth."
            print(f"  Persona: {persona['name']}")

    # Pull research data (we only need hooks, but pull all for consistency)
    research_data = get_research_data(product_category)
    print(f"  Loaded {len(research_data['hook_patterns'])} hooks from database")

    # Build hook template constraints (Improvement #2)
    try:
        from tiktok_engine.v2.pipeline.hook_templates import build_hook_template_prompt
    except ImportError:
        from v2.pipeline.hook_templates import build_hook_template_prompt
    hook_template_constraints = build_hook_template_prompt(product_brief)
    print(f"  Hook template constraints ready ({len(hook_template_constraints)} chars)")

    prompt = generate_hooks_prompt(product_brief, research_data, persona_context,
                                   hook_template_constraints=hook_template_constraints)
    print(f"  Hook prompt ready ({len(prompt)} chars)")

    return {"prompt": prompt, "research_data": research_data}


def score_hook(hook_text, product_brief, db_hooks):
    """
    PASS 3: Automated hook scoring.

    Scores a generated hook against measurable patterns from top-performing
    database hooks. Returns a score from 0-100. Higher = better.

    Scoring criteria derived from database analysis:
    - Top performers avg 11.9 words (shorter = better)
    - 2-word hook got 73K, 6-word hooks got 30K-153K
    - Bottom 2 hooks (under 1K eng) are 25 and 17 words — long and descriptive
    - Product mentions correlate with low engagement
    - Questions, fragments, and incomplete thoughts appear more in top half
    - References to other people appear in 38% of top performers
    """
    import re

    text = hook_text.strip().strip('"').strip("'")
    text_lower = text.lower()
    words = text.split()
    word_count = len(words)

    score = 0

    # ── WORD COUNT (max 30 points) ──
    # Sweet spot: 2-12 words. Top performers cluster here.
    # Penalties ramp up after 12, severe after 18.
    if word_count <= 5:
        score += 30  # Ultra-short = maximum mystery
    elif word_count <= 8:
        score += 27
    elif word_count <= 12:
        score += 22
    elif word_count <= 15:
        score += 12
    elif word_count <= 18:
        score += 5
    else:
        score += 0  # 19+ words = too long, bottom performer territory

    # ── PRODUCT MENTION PENALTY (max -30 points) ──
    # Extract product keywords from the brief — covers ALL product categories
    brief_lower = product_brief.lower()
    product_keywords = []

    # Universal product-type words (gadgets, beauty, food, home, fashion, etc.)
    all_product_words = [
        # Tech/gadgets
        "glasses", "sunglasses", "camera", "recording", "filming",
        "video", "photo", "hands-free", "hands free", "8mp",
        "megapixel", "hd", "bluetooth", "ai assistant", "smart",
        "wearable", "uv protection", "touch control",
        # Beauty/skincare
        "serum", "cream", "moisturizer", "cleanser", "toner",
        "eye stick", "balm", "skincare", "anti-aging", "retinol",
        "sunscreen", "spf", "collagen", "hyaluronic",
        # Food
        "snack", "organic", "freeze-dried", "supplement", "vitamin",
        # Home
        "dispenser", "purifier", "vacuum", "mop",
    ]
    for kw in all_product_words:
        if kw in brief_lower:
            product_keywords.append(kw)

    # Also check for brand name
    brand_patterns = re.findall(r'PRODUCT:\s*(\w+)', product_brief, re.IGNORECASE)
    product_keywords.extend([b.lower() for b in brand_patterns])

    # Extract additional brand/product name words from the brief
    name_patterns = re.findall(r'(?:PRODUCT|Product|product)[:\s]+([^\n]+)', product_brief)
    for name in name_patterns:
        for word in name.lower().split():
            if len(word) > 3 and word not in {"this", "that", "with", "from", "your", "their"}:
                product_keywords.append(word)

    product_penalty = 0
    for kw in product_keywords:
        if kw in text_lower:
            product_penalty += 10
    product_penalty = min(product_penalty, 30)  # Cap at -30
    score -= product_penalty

    # ── INCOMPLETENESS BONUS (max 15 points) ──
    # Top hooks feel incomplete — fragments, ellipsis, no resolution
    incompleteness = 0
    if "..." in text:
        incompleteness += 5
    if text.endswith(".") and word_count <= 8:
        incompleteness += 3  # Short declarative fragment
    if not any(text.endswith(c) for c in [".", "!", "?"]):
        incompleteness += 4  # No punctuation = trails off
    # Check if it's a sentence fragment (no common verbs for short hooks)
    if word_count <= 6:
        incompleteness += 3
    score += min(incompleteness, 15)

    # ── SOCIAL/REACTION BONUS (max 10 points) ──
    # References to other people's reactions = social proof in the hook
    person_words = ["husband", "wife", "sister", "brother", "mom", "dad",
                    "friend", "kids", "son", "daughter", "neighbor",
                    "somebody", "someone", "they", "he ", "she ",
                    "my ", "police", "security", "mailman", "stranger"]
    person_count = sum(1 for pw in person_words if pw in text_lower)
    if person_count >= 2:
        score += 10
    elif person_count >= 1:
        score += 7

    # ── TENSION/CONFLICT BONUS (max 10 points) ──
    # Words that signal drama, conflict, something wrong
    tension_words = ["but", "except", "unfortunately", "returning",
                     "careful", "warning", "banned", "caught", "called",
                     "asked", "found", "left", "stopped", "can't",
                     "won't", "shouldn't", "deleting", "incident",
                     "nobody", "never", "not", "why"]
    tension_count = sum(1 for tw in tension_words if tw in text_lower)
    score += min(tension_count * 3, 10)

    # ── QUESTION BONUS (5 points) ──
    if "?" in text:
        score += 5

    # ── COMPLETENESS PENALTY (max -15 points) ──
    # If the hook tells a complete story with no gap, penalize
    # Detection: hook contains both a setup AND resolution
    resolution_signals = ["because", "that's why", "so i", "which is why",
                         "and it", "and they", "it works", "it does",
                         "you can", "it lets you", "allows you"]
    for rs in resolution_signals:
        if rs in text_lower:
            score -= 8
            break

    # ── AD COPY PENALTY (max -15 points) ──
    # If it sounds like marketing, penalize
    ad_signals = ["this changes", "game changer", "you need this",
                  "best thing", "life changing", "must have", "trust me",
                  "you won't regret", "highly recommend", "obsessed with",
                  "changed my life", "you need to try"]
    for ad in ad_signals:
        if ad in text_lower:
            score -= 10
            break

    # ── SOFT OPENER PENALTY (-5 points) ──
    soft_openers = ["so ", "you know what", "what if ", "have you ever",
                    "okay so", "ok so", "honestly ", "literally "]
    for so in soft_openers:
        if text_lower.startswith(so):
            score -= 5
            break

    # ── EMOTIONAL SPECIFICITY BONUS (max 10 points) ──
    # Hooks with specific, vivid details perform better than vague ones.
    # "My mom asked why I looked so tired" > "Why is nobody talking about this?"
    # Specificity signals: numbers, body parts, named people, strong verbs
    specificity = 0
    # Contains a specific number
    if re.search(r'\d', text):
        specificity += 3
    # Contains a strong action verb (not just state verbs)
    action_verbs = ["came", "left", "stopped", "threw", "grabbed", "pulled",
                    "dropped", "ran", "cried", "screamed", "called", "caught",
                    "broke", "burned", "melted", "disappeared", "showed",
                    "asked", "told", "said", "stole", "touched", "glided"]
    if any(v in text_lower for v in action_verbs):
        specificity += 3
    # Contains a body reference (physical/sensory)
    body_words = ["face", "eyes", "skin", "hands", "body", "legs", "stomach",
                  "mouth", "hair", "teeth", "nails", "lips", "back"]
    if any(b in text_lower for b in body_words):
        specificity += 2
    # Has a narrative setup (someone did something)
    if re.search(r'\b(my|her|his|their)\b.*\b(said|asked|told|thought|noticed|saw)\b', text_lower):
        specificity += 2
    score += min(specificity, 10)

    # ── GENERIC HOOK PENALTY (max -10 points) ──
    # Penalize hooks that could apply to literally any product
    generic_patterns = [
        "why is nobody talking about this",
        "nobody is talking about this",
        "you need to try this",
        "this changed everything",
        "i can't believe this",
        "wait until you see this",
        "this is a game changer",
    ]
    for gp in generic_patterns:
        if gp in text_lower:
            score -= 10
            break

    # Clamp to 0-100
    score = max(0, min(100, score))

    return score


def _hook_similarity(hook_a, hook_b):
    """
    Calculate word overlap between two hooks. Returns a ratio 0-1.
    Used to detect hooks that are too similar (e.g., three "crunch" hooks).
    """
    # Normalize: lowercase, strip punctuation, split into words
    import re
    def normalize(text):
        text = re.sub(r'[^\w\s]', '', text.lower())
        # Remove common filler words that shouldn't count as similarity
        stop_words = {"the", "a", "an", "is", "it", "my", "i", "me", "this", "that",
                       "so", "like", "just", "but", "and", "or", "in", "on", "of", "to",
                       "for", "with", "not", "don't", "can't", "actually", "literally",
                       "really", "honestly", "about", "how", "why", "what", "when"}
        words = set(text.split()) - stop_words
        return words

    words_a = normalize(hook_a)
    words_b = normalize(hook_b)

    if not words_a or not words_b:
        return 0.0

    overlap = words_a & words_b
    # Jaccard similarity
    union = words_a | words_b
    return len(overlap) / len(union) if union else 0.0


def select_diverse_top_n(scored_hooks, n=5, similarity_threshold=0.3,
                         hook_template_map=None):
    """
    Select top N hooks with diversity enforcement.

    Uses a greedy approach with both text similarity AND category diversity:
    1. Pick the highest-scored hook
    2. For each subsequent pick, skip if:
       - Too similar to an already-selected hook (Jaccard > threshold)
       - Would result in >2 hooks from the same template category
    3. Continue until N hooks are selected

    Category enforcement (data-driven):
      - Max 2 hooks from the same category (warning, curiosity, etc.)
      - This ensures the 5 selected hooks span at least 3 categories

    Args:
        scored_hooks: list of {"text": ..., "score": ...} sorted by score desc
        n: number of hooks to select (default 5)
        similarity_threshold: max word overlap allowed (default 0.3 = 30%)
        hook_template_map: optional dict mapping hook text -> template_id (e.g. "HT1")

    Returns:
        list of selected hook dicts (in selection order)
    """
    # Build template -> category lookup
    category_lookup = {}
    if hook_template_map:
        try:
            from v2.pipeline.hook_templates import HOOK_TEMPLATES
        except ImportError:
            try:
                from tiktok_engine.v2.pipeline.hook_templates import HOOK_TEMPLATES
            except ImportError:
                HOOK_TEMPLATES = []
        tmpl_to_cat = {t["id"]: t["category"] for t in HOOK_TEMPLATES}
        for text, tmpl_id in hook_template_map.items():
            category_lookup[text] = tmpl_to_cat.get(tmpl_id, "original")

    selected = []
    category_counts = {}
    MAX_PER_CATEGORY = 2  # Data-driven: ensures min 3 categories in top 5

    for hook in scored_hooks:
        if len(selected) >= n:
            break

        # Check similarity against all already-selected hooks
        too_similar = False
        for chosen in selected:
            sim = _hook_similarity(hook["text"], chosen["text"])
            if sim > similarity_threshold:
                too_similar = True
                break

        if too_similar:
            continue

        # Check category cap (if category data available)
        if category_lookup:
            cat = category_lookup.get(hook["text"], "original")
            if category_counts.get(cat, 0) >= MAX_PER_CATEGORY:
                continue
            category_counts[cat] = category_counts.get(cat, 0) + 1

        selected.append(hook)

    # If we couldn't find enough diverse hooks, fall back to filling from top scores
    if len(selected) < n:
        for hook in scored_hooks:
            if hook not in selected:
                selected.append(hook)
            if len(selected) >= n:
                break

    return selected


def score_and_rank_hooks(hook_texts, product_brief, db_hooks):
    """
    Score a list of generated hooks and return them ranked best to worst.
    Includes diversity-aware selection for the top 5.

    Args:
        hook_texts: list of hook strings
        product_brief: product brief text
        db_hooks: list of database hook patterns for reference

    Returns:
        list of dicts: [{"text": "...", "score": 85, "rank": 1}, ...]
        The list is ordered by score, but the top 5 are diversity-checked.
    """
    scored = []
    for text in hook_texts:
        s = score_hook(text, product_brief, db_hooks)
        scored.append({"text": text, "score": s})

    scored.sort(key=lambda x: -x["score"])
    for i, item in enumerate(scored):
        item["rank"] = i + 1

    return scored


def generate_scripts_prompt(product_brief, research_context, structural_rules,
                            use_case_rules, locked_hooks, persona_context="",
                            web_research="", product_research="",
                            angle_constraints="", ost_constraints="",
                            broll_constraints="", audio_constraints="",
                            cta_constraints="", pacing_constraints="",
                            structure_summary=""):
    """
    PASS 4: Generate full scripts with PRE-LOCKED hooks.

    The hooks are already decided. This prompt builds scripts AROUND them.
    The model cannot change, rephrase, or "improve" the hooks.
    """

    # Support both plain strings and dicts with template_id
    hooks_lines = []
    for i, h in enumerate(locked_hooks):
        if isinstance(h, dict):
            hooks_lines.append(f"  HERO {i+1} HOOK (LOCKED — use EXACTLY): \"{h['text']}\" | hook_template: \"{h['template_id']}\"")
        else:
            hooks_lines.append(f"  HERO {i+1} HOOK (LOCKED — use EXACTLY): \"{h}\" | hook_template: \"ORIGINAL\"")
    hooks_block = "\n".join(hooks_lines)

    # Build the product knowledge section — use Pass 0 research if available, fall back to brief + web_research
    product_section = f"=== PRODUCT INFORMATION ===\n{product_brief}"
    if product_research:
        product_section += f"""

═══════════════════════════════════════════════════
DEEP PRODUCT RESEARCH (from Pass 0 — use this heavily)
═══════════════════════════════════════════════════
The following is comprehensive product research synthesized from multiple web sources.
USE THIS in your scripts. Reference specific ingredients, real user results, price comparisons,
sensory details, and value propositions. The scripts should demonstrate genuine product knowledge —
the kind that comes from someone who actually RESEARCHED the product, not just read the box.

When writing CTAs and value proposition lines, pull SPECIFIC details from this research:
- Name actual ingredients and what they do (in plain English)
- Reference real results timelines ("visible results in X days")
- Use specific price comparisons ("vs $60 for [competitor]")
- Mention the bundle deal / free gifts if applicable
- Describe what it actually looks/feels like (sensory details)

{product_research}"""
    elif web_research:
        product_section += f"\n\n=== ADDITIONAL PRODUCT RESEARCH FROM WEB ===\n{web_research}"

    prompt = f"""You are the world's most successful TikTok affiliate content strategist.

{persona_context}

═══════════════════════════════════════════════════
PERSONA ENFORCEMENT — NON-NEGOTIABLE (read FIRST)
═══════════════════════════════════════════════════
Before writing a single line of script, internalize who this creator IS:

VOCABULARY CEILING: The creator speaks at a normal-person level. She does NOT know:
  - Luxury brand prices (La Mer, SK-II, Tatcha — she has never bought these and wouldn't reference them)
  - Scientific ingredient names (adenosine, glutathione, niacinamide — she'd say "the anti-wrinkle ingredient" or "the brightening stuff")
  - Industry jargon ("K-beauty," "actives," "barrier repair," "retinoid" — she doesn't speak like a skincare blogger)
  - Clinical claims ("clinically proven," "dermatologist-tested" — she'd say "it actually works" or "my skin looks different")

HOW SHE DISCOVERS THINGS: She found this product because someone sent it to her, she saw it on TikTok, or she stumbled on it. She is NOT an expert. She tried it, she noticed something, and now she's telling her friends about it. That's the energy.

HOW SHE TALKS ABOUT RESULTS: She describes what she SEES and FEELS, not what ingredients DO:
  ✓ "my under-eyes don't look as dark" (what she sees)
  ✗ "the adenosine reduces fine lines" (what an ingredient does)
  ✓ "it feels really smooth when I put it on" (what she feels)
  ✗ "the snow mushroom provides 500x hydration" (marketing stat she'd never say)
  ✓ "my concealer goes on way better now" (practical result)
  ✗ "it creates a smooth base for makeup application" (beauty blogger language)

PRICE COMPARISONS: She can compare to things SHE has bought:
  ✓ "I've spent way more on stuff that didn't work" (vague, relatable)
  ✓ "this is like twenty-three bucks and it lasts forever" (her own experience)
  ✗ "La Mer costs two hundred fifty dollars" (she doesn't know this)
  ✗ "SK-II is ninety-five" (she has never looked this up)

INGREDIENT REFERENCES: If the script needs to mention ingredients at all:
  ✓ "there's this ingredient in it that's supposed to help with wrinkles — I can't even pronounce it"
  ✓ "it's got collagen and some kind of mushroom extract in it" (casual, imprecise)
  ✗ "adenosine is the clinical anti-wrinkle ingredient" (she's not a chemist)
  ✗ "glutathione for brightening dark circles" (she doesn't know what glutathione is)

THE GOLDEN TEST: Before writing any line, ask: "Would this person actually say this to her friend while making coffee?" If the answer is no, rewrite it.

{structural_rules}

{use_case_rules}

{angle_constraints}

{ost_constraints}

═══════════════════════════════════════
PRE-LOCKED HOOKS (DO NOT CHANGE THESE)
═══════════════════════════════════════
The following hooks have been selected through a data-driven scoring process.
Each hero video MUST open with its assigned hook EXACTLY as written.
Do NOT rephrase, improve, shorten, lengthen, or modify these hooks in any way.
Your job is to build compelling scripts AROUND these hooks.

{hooks_block}

For each hero, the hook is line 1 (TH[X]-a). Build the rest of the script
to RESOLVE the curiosity gap the hook creates. The viewer stopped scrolling
because of the hook — now deliver on the promise it implied.

CRITICAL — RIDE THE HOOK:
After the hook, do NOT immediately explain or backpedal. If the hook is "This came out of my eye,"
the NEXT line should lean INTO the weirdness — not say "Okay so these aren't literally coming out of
your eye." That kills the energy. Stay in the mystery for 2-3 more lines before revealing what
the product actually is. The hook earned attention — don't waste it by immediately explaining.

═══════════════════════════════════════
VOICE NATURALNESS RULE (v2)
═══════════════════════════════════════
These scripts will be READ ALOUD by a real person on camera. They must sound SPOKEN, not written.

HOW REAL PEOPLE TALK ON TIKTOK:
- They trail off mid-thought ("and I was like... wait")
- They self-correct ("not the cheap kind, like the actual—you know what I mean")
- They use filler naturally ("like," "honestly," "literally," "you know")
- They address the viewer directly and casually ("okay hear me out," "no but seriously")
- They react to their own story ("which is WILD," "and I'm sitting there like...")
- They speak in fragments, not full sentences ("Day two. TWO. That's all it took.")
- They use contractions always (it's, don't, I'm, they're — never "it is" or "do not")
- They reference their real life casually ("while my kids were eating breakfast," "in the bathroom mirror at like 6am")

NATURAL NUMBERS AND SPEECH (MANDATORY):
When scripts include numbers, measurements, prices, certifications, or technical terms,
they MUST be written the way a real person would SAY them out loud — not how they appear
on a label or product listing. This is critical for sounding human vs. sounding like an ad.

RULES:
- ROUND MEASUREMENTS to how people actually talk:
  ✗ "6.4 ounces" → ✓ "like six and a half ounces"
  ✗ "3.52 oz" → ✓ "about three and a half ounces"
  ✗ "236ml" → ✓ "like a full cup"
  ✗ "1.69 fl oz" → ✓ "this little bottle"
- ROUND PRICES to casual speech:
  ✗ "$28.95" → ✓ "like thirty bucks"
  ✗ "$23.00" → ✓ "twenty-three dollars" or "under twenty-five bucks"
  ✗ "$7.99" → ✓ "eight bucks"
  ✗ "$149.99" → ✓ "a hundred and fifty dollars"
- DROP FORMAL CERTIFICATIONS — say the plain version:
  ✗ "USDA organic" → ✓ "organic"
  ✗ "FDA-approved" → ✓ "actually approved" (or just skip it)
  ✗ "third-party tested" → ✓ just don't say it — nobody talks like that
  ✗ "dermatologist-recommended" → ✓ "dermatologists actually back this" or skip
  ✗ "GMP-certified facility" → ✓ never say this on TikTok, ever
- HUMANIZE COUNTS AND STATS:
  ✗ "36-day supply" → ✓ "this lasts like a month"
  ✗ "Contains 120 patches" → ✓ "you get like a hundred and twenty of these"
  ✗ "4.8 out of 5 stars" → ✓ "basically five stars"
  ✗ "Over 10,000 reviews" → ✓ "like ten thousand people reviewed this"
  ✗ "50 calories per serving" → ✓ "fifty calories" (calories are fine as-is since people say them naturally)
- NEVER read a nutrition label out loud:
  ✗ "1g total fat, 0g saturated fat, 12g total carbohydrate, 1g dietary fiber, 8g total sugars"
  ✓ Pick ONE or TWO standout facts: "fifty calories and basically no fat"
- WRITE NUMBERS AS WORDS when spoken (except very large numbers):
  ✗ "5 sources of PDRN" → ✓ "five different sources"
  ✗ "2 days" → ✓ "two days"
  ✗ "Use for 10 minutes" → ✓ "ten minutes"

WHAT TO AVOID — these kill naturalness:
✗ "Your under-eye area deserves better than just concealer" — nobody talks like this. Say "why are we still just covering this up with concealer"
✗ "It's honestly insane" as a bridge — overused, generic
✗ "Trust me" as a closer — sounds like a used car salesman
✗ Any line that starts with "What's it worth to you" — rhetorical marketing question
✗ "Do it for the [X]. Do it for the [Y]. Do it because [Z]." — triple structure is ad copy
✗ "Seriously, grab them today" — generic push CTA
✗ Explaining what an ingredient IS right after naming it in the same breath — break it up naturally
✗ Perfect grammar in spoken lines — real people say "me and my sister" not "my sister and I"
✗ Addressing the audience with "If you [condition], then [product is for you]" — classic ad framing
✗ Any sentence that could appear in a magazine ad or product listing
✗ Reading off a product label or listing — paraphrase everything into casual speech

THE TEST: Read every line out loud. If it sounds like something a copywriter typed,
rewrite it as something the creator would actually SAY to a friend over coffee.
If any number sounds like it was copied from an Amazon listing, round it and reword it.

{structure_summary}

=== RESEARCH DATA FROM TOP-PERFORMING AFFILIATE VIDEOS ===
{research_context}

{product_section}

{broll_constraints}

{audio_constraints}

{cta_constraints}

{pacing_constraints}

=== YOUR TASK ===
Generate the SHOOT GUIDE JSON only. Structure:

{{"productName":"...","productSubtitle":"...","totalVideos":10,
"masterShotList":{{
  "talkingHeadShots":[
    {{"label":"TH1","concept":"Hero 1 — Concept Name",
      "content_angle":"<angle_key from rankings>",
      "hook_template":"<HT# from the locked hook above — MUST include this>",
      "angle_evidence":{{"rank":<n>,"weighted_score":<n>,"avg_likes":<n>,"avg_shares":<n>,"video_count":<n>}},
      "note":"Setup/energy note",
      "lines":[
        {{"id":"TH1-a","type":"ON CAMERA","text":"[LOCKED HOOK — copy exactly]"}},
        {{"id":"TH1-b","type":"ON CAMERA","text":"Second line..."}},
        {{"id":"TH1-c","type":"VOICEOVER","text":"Third line..."}}
      ]
    }}
  ],
  "brollShots":[{{"label":"B1","description":"Short direction."}}],
  "voiceoverAudio":[{{"label":"VO1","concept":"Remix concept","script":"Line.","context":"Tone."}}]
}},
"propsAndSetup":{{"products":["..."],"locations":["..."],"wardrobe":["..."]}}
}}

REQUIREMENTS:
- 5 hero scripts (TH1-TH5), each starting with its LOCKED hook as TH[X]-a
- 7-8 lines per script, 140-220 words, mix of ON CAMERA and VOICEOVER
- The script must RESOLVE the hook's curiosity gap naturally
- Minimum 3 spoken CTAs per hero (value_proposition mid + direct_link + urgency close)
- 8-12 b-roll shots for the master shot list (FILMABLE ONLY — no text overlays, no graphics, no post-production elements), but each HERO video's visual timeline should only USE 2-4 of them as brief cutaways (see B-ROLL USAGE RULES above)
- 5 voiceover clips for remixes
- All use cases must be real (creator physically present, using the product)
- Keep shoot practical — mostly filmable at home/nearby locations
- B-roll descriptions should be plain and direct — no [FREE] or [PURCHASE] tags, just what to film

SINGLE PRODUCT RULE (MANDATORY):
Assume the creator has exactly ONE unit of the product. This is a test product —
they ordered one to try it and create content around it. Every b-roll shot and
script must be filmable with just that one unit. Specifically:
  ✗ NO shots showing multiple units, stacking, stockpiling, or bulk quantities
  ✗ NO "pantry full of these" or "I ordered five more" visuals (saying it is fine, showing it is not)
  ✗ NO comparison shots requiring the creator to also purchase competitor products
  ✓ All b-roll must be achievable with ONE product unit in hand
  ✓ Scripts CAN reference buying more, ordering again, etc. — just the VISUALS must work with one
  ✓ If a shot needs the product in multiple states (open + closed), that's fine — it's still one unit

Output ONLY valid JSON. No commentary before or after."""

    return prompt


def generate_content_plan_v2(product_brief, product_category=None, web_research="",
                             creator_name=None, creator_id=None, hook_responses=None,
                             product_research=""):
    """
    MULTI-PASS CONTENT ENGINE (v2)

    Automated pipeline:
      Pass 0: Product Research (deep web research → structured brief) — run BEFORE this function
      Pass 1: Pull data + analyze patterns + derive rules
      Pass 2: Generate hooks (isolated, focused prompt)
      Pass 3: Score hooks automatically (no human in the loop)
      Pass 4: Build scripts around top-scoring hooks

    ═══════════════════════════════════════════════════════════════
    NON-NEGOTIABLE PIPELINE INTEGRITY RULE
    ═══════════════════════════════════════════════════════════════
    Every pass in this pipeline MUST run independently and deliberately.
    Each pass MUST be executed as a separate, focused LLM call — never
    collapsed, rushed, or combined with other passes.

    Specifically:
      - Pass 0 (Product Research) MUST be a standalone call that produces
        a comprehensive structured brief BEFORE any other pass runs.
      - Pass 2 (Hook Generation) MUST run as 3 SEPARATE rounds of 10 hooks
        each, producing 30 total candidates. Each round is its own call.
      - Pass 3 (Hook Scoring) MUST use the actual scoring algorithm in this
        engine — never approximated or skipped.
      - Pass 4 (Script Generation) MUST receive the full structural rules,
        b-roll data, persona context, holiday context, and locked hooks
        as a complete prompt. Scripts are generated in a single focused call.

    NO SHORTCUTS. If the API is unavailable, each pass must still be
    executed with the same rigor — the caller is responsible for running
    each pass at full quality. The output quality depends on pass isolation.
    ═══════════════════════════════════════════════════════════════

    Args:
        product_brief: Product description (basic or enhanced from Pass 0)
        product_category: Optional category filter
        web_research: Optional additional raw web research (legacy — use product_research instead)
        creator_name: Optional creator persona name
        creator_id: Optional creator persona ID
        hook_responses: Optional list of raw hook response strings from Claude
                        (if None, returns the hooks prompt for Pass 2)
        product_research: Structured product research from Pass 0
                          (comprehensive brief with ingredients, results, comparisons, etc.)

    Returns:
        If hook_responses is None: dict with hooks_prompt (caller runs Pass 2)
        If hook_responses provided: dict with scripts_prompt + scored_hooks (caller runs Pass 4)
    """
    print("=" * 60)
    print("  TikTok Content Engine v2 (Multi-Pass Pipeline)")
    print("=" * 60)

    # ── PASS 1: Data + Rules ──
    print("\n[PASS 1] Loading data and deriving rules...")

    persona = None
    persona_context = ""
    if creator_name or creator_id:
        persona = get_creator_persona(creator_name=creator_name, creator_id=creator_id)
        if persona:
            persona_context = build_persona_context(persona)
            print(f"  Persona: {persona['name']}")

    # Holiday awareness — check what's in the script window
    upcoming_holidays = get_upcoming_holidays()
    holiday_context = ""
    if upcoming_holidays:
        holiday_names = [h["holiday_name"] for h in upcoming_holidays]
        print(f"  Holidays in script window: {', '.join(holiday_names)}")
        holiday_context = build_holiday_context(upcoming_holidays, product_brief)
    else:
        print("  No holidays in current script window — all evergreen")

    research_data = get_research_data(product_category)
    print(f"  Loaded {len(research_data['videos'])} videos, {len(research_data['hook_patterns'])} hooks")

    patterns = analyze_patterns(research_data)
    structural_rules = derive_structural_rules(research_data, patterns)
    use_case_rules = derive_use_case_rules(product_brief)
    research_context = build_research_context(research_data, patterns)
    print(f"  Rules: {len(structural_rules.splitlines())} structural + {len(use_case_rules.splitlines())} use case")

    # ── PASS 2: Generate Hooks ──
    if hook_responses is None:
        print("\n[PASS 2] Generating hooks prompt (isolated)...")
        persona_hook_ctx = ""
        if persona:
            persona_hook_ctx = f"CREATOR: {persona.get('name', 'Unknown')}, {persona.get('age_range', 'N/A')}, {persona.get('tone', 'N/A')} tone. Hooks should sound like they'd come out of her mouth."

        # Add holiday hint to hooks prompt if active
        holiday_hook_hint = ""
        if upcoming_holidays:
            holiday_names = [h["holiday_name"] for h in upcoming_holidays]
            holiday_hook_hint = f"\nHOLIDAY CONTEXT: {', '.join(holiday_names)} is approaching. If the product fits, 2-3 of your 10 hooks can lean into the holiday/gifting/seasonal angle. The rest should be evergreen. Only do this if the product-holiday fit is natural — don't force it."

        # Build hook template constraints (Improvement #2)
        try:
            from tiktok_engine.v2.pipeline.hook_templates import build_hook_template_prompt
        except ImportError:
            from v2.pipeline.hook_templates import build_hook_template_prompt
        hook_template_constraints = build_hook_template_prompt(product_brief)
        print(f"  Hook template constraints ready ({len(hook_template_constraints)} chars)")

        hooks_prompt = generate_hooks_prompt(
            product_brief, research_data,
            persona_context=persona_hook_ctx + holiday_hook_hint,
            hook_template_constraints=hook_template_constraints,
        )
        print(f"  Hook prompt ready ({len(hooks_prompt)} chars)")
        print(f"\n  >> Send this prompt to Claude, then call again with hook_responses")

        return {
            "stage": "hooks_needed",
            "hooks_prompt": hooks_prompt,
            "product_brief": product_brief,
            "product_category": product_category,
            "web_research": web_research,
            "creator_name": creator_name,
            "creator_id": creator_id,
            "active_holidays": [h["holiday_name"] for h in upcoming_holidays] if upcoming_holidays else [],
        }

    # ── PASS 3: Score Hooks ──
    print("\n[PASS 3] Scoring hooks...")

    # Parse hook texts from responses (handle multiple rounds)
    # Handles both old format "HOOK N: text" and template format "HOOK N: text | TEMPLATE: HT#"
    import re
    all_hooks = []
    hook_template_map = {}  # track which template each hook came from
    for response in hook_responses:
        lines = response.strip().split("\n")
        for line in lines:
            # Match "HOOK N: text" or "HOOK N: text | TEMPLATE: HT#"
            match = re.match(r'(?:HOOK\s*\d+\s*:\s*)(.*)', line, re.IGNORECASE)
            if match:
                raw = match.group(1).strip().strip('"').strip("'")
                # Strip template annotation if present
                template_id = None
                tmpl_match = re.search(r'\|\s*TEMPLATE:\s*(HT\d+|ORIGINAL)\s*$', raw, re.IGNORECASE)
                if tmpl_match:
                    template_id = tmpl_match.group(1).upper()
                    raw = raw[:tmpl_match.start()].strip().strip('"').strip("'")
                hook_text = raw
                if hook_text:
                    all_hooks.append(hook_text)
                    if template_id:
                        hook_template_map[hook_text] = template_id

    print(f"  Parsed {len(all_hooks)} hooks from {len(hook_responses)} response(s)")

    # Log template coverage (Improvement #2)
    if hook_template_map:
        template_count = sum(1 for v in hook_template_map.values() if v != "ORIGINAL")
        original_count = sum(1 for v in hook_template_map.values() if v == "ORIGINAL")
        untagged = len(all_hooks) - len(hook_template_map)
        print(f"  Template coverage: {template_count} template-based, {original_count} original, {untagged} untagged")
        # Show which templates were used
        from collections import Counter
        tmpl_usage = Counter(v for v in hook_template_map.values() if v != "ORIGINAL")
        if tmpl_usage:
            print(f"  Templates used: {dict(tmpl_usage)}")

    # Score them
    db_hooks = research_data["hook_patterns"]
    ranked = score_and_rank_hooks(all_hooks, product_brief, db_hooks)

    print(f"\n  HOOK RANKINGS:")
    for item in ranked:
        marker = " <<<" if item["rank"] <= 5 else ""
        print(f"    #{item['rank']:2d} (score {item['score']:3d}): \"{item['text'][:60]}\"{marker}")

    # Pick top 5 with diversity enforcement
    diverse_top = select_diverse_top_n(ranked, n=5, similarity_threshold=0.3,
                                       hook_template_map=hook_template_map)
    top_5 = [item["text"] for item in diverse_top]
    # Build top_5 with template IDs attached for downstream tracking
    top_5_with_templates = []
    for item in diverse_top:
        tmpl = hook_template_map.get(item["text"], "ORIGINAL")
        top_5_with_templates.append({"text": item["text"], "template_id": tmpl})
    print(f"\n  TOP 5 HOOKS (diversity-checked):")
    for i, h in enumerate(top_5_with_templates):
        orig_rank = next(r["rank"] for r in ranked if r["text"] == h["text"])
        score = next(r["score"] for r in ranked if r["text"] == h["text"])
        print(f"    Hero {i+1} (rank #{orig_rank}, score {score}, {h['template_id']}): \"{h['text']}\"")

    # ── PASS 4: Generate Scripts ──
    print("\n[PASS 4] Building scripts prompt with locked hooks...")

    # Merge holiday context into structural rules so it flows into the scripts prompt
    if holiday_context:
        structural_rules = structural_rules + "\n" + holiday_context

    # Build angle constraints from database (Improvement #1)
    try:
        from tiktok_engine.v2.pipeline.angle_scorer import build_angle_constraint_prompt, get_angle_rankings
    except ImportError:
        from v2.pipeline.angle_scorer import build_angle_constraint_prompt, get_angle_rankings
    angle_constraints = build_angle_constraint_prompt(product_brief)
    angle_rankings = get_angle_rankings()
    print(f"  Angle rankings loaded — top angle: {angle_rankings[0]['angle']} (score {angle_rankings[0]['weighted_score']:,})")

    # Build OST constraints from database (Improvement #3)
    try:
        from tiktok_engine.v2.pipeline.ost_patterns import build_ost_constraint_prompt
    except ImportError:
        from v2.pipeline.ost_patterns import build_ost_constraint_prompt
    ost_constraints = build_ost_constraint_prompt()
    print(f"  OST constraints ready ({len(ost_constraints)} chars)")

    # Build B-roll constraints from database (Improvement #4)
    try:
        from tiktok_engine.v2.pipeline.broll_analyzer import build_broll_constraint_prompt
    except ImportError:
        from v2.pipeline.broll_analyzer import build_broll_constraint_prompt
    broll_constraints = build_broll_constraint_prompt()
    print(f"  B-roll constraints ready ({len(broll_constraints)} chars)")

    # Build Audio constraints from database (Improvement #5)
    try:
        from tiktok_engine.v2.pipeline.audio_analyzer import build_audio_constraint_prompt
    except ImportError:
        from v2.pipeline.audio_analyzer import build_audio_constraint_prompt
    audio_constraints = build_audio_constraint_prompt()
    print(f"  Audio constraints ready ({len(audio_constraints)} chars)")

    # Build CTA placement constraints from database (Improvement #6)
    try:
        from tiktok_engine.v2.pipeline.cta_analyzer import build_cta_constraint_prompt
    except ImportError:
        from v2.pipeline.cta_analyzer import build_cta_constraint_prompt
    cta_constraints = build_cta_constraint_prompt()
    print(f"  CTA constraints ready ({len(cta_constraints)} chars)")

    # Build Pacing constraints from transcript analysis (Improvement #7)
    try:
        from tiktok_engine.v2.pipeline.pacing_analyzer import build_pacing_constraint_prompt
    except ImportError:
        from v2.pipeline.pacing_analyzer import build_pacing_constraint_prompt
    pacing_constraints = build_pacing_constraint_prompt()
    print(f"  Pacing constraints ready ({len(pacing_constraints)} chars)")

    # Build Structure summary (Improvement #8 — top-level hero vs remix blueprint)
    try:
        from tiktok_engine.v2.pipeline.structure_rules import build_structure_summary_prompt
    except ImportError:
        from v2.pipeline.structure_rules import build_structure_summary_prompt
    structure_summary = build_structure_summary_prompt()
    print(f"  Structure summary ready ({len(structure_summary)} chars)")

    scripts_prompt = generate_scripts_prompt(
        product_brief=product_brief,
        research_context=research_context,
        structural_rules=structural_rules,
        use_case_rules=use_case_rules,
        locked_hooks=top_5_with_templates,
        persona_context=persona_context,
        web_research=web_research,
        product_research=product_research,
        angle_constraints=angle_constraints,
        ost_constraints=ost_constraints,
        broll_constraints=broll_constraints,
        audio_constraints=audio_constraints,
        cta_constraints=cta_constraints,
        pacing_constraints=pacing_constraints,
        structure_summary=structure_summary,
    )

    print(f"  Scripts prompt ready ({len(scripts_prompt)} chars)")

    stats = get_database_stats()

    return {
        "stage": "scripts_ready",
        "scripts_prompt": scripts_prompt,
        "scored_hooks": ranked,
        "locked_hooks": top_5_with_templates,
        "angle_rankings": angle_rankings,
        "stats": stats,
        "persona": persona,
        "active_holidays": [h["holiday_name"] for h in upcoming_holidays] if upcoming_holidays else [],
    }


def generate_content_plan(product_brief, product_category=None, web_research="", creator_name=None, creator_id=None):
    """
    Main entry point: Generate a full content plan for a product.

    Args:
        product_brief: Text description of the product
        product_category: Optional category to filter research data
        web_research: Optional additional web research about the product
        creator_name: Optional creator name to load persona (e.g., "Michelle")
        creator_id: Optional creator UUID to load persona

    Returns:
        dict with research_context, prompt, persona, and stats
    """
    print("=" * 60)
    print("  TikTok Content Generation Engine")
    print("=" * 60)

    # Load creator persona
    persona = None
    persona_context = ""
    if creator_name or creator_id:
        print(f"\n[0/3] Loading creator persona...")
        persona = get_creator_persona(creator_name=creator_name, creator_id=creator_id)
        if persona:
            persona_context = build_persona_context(persona)
            print(f"  Loaded persona: {persona['name']} ({persona.get('tone', 'unknown')} tone, {persona.get('account_stage', 'unknown')} account)")
        else:
            print(f"  Warning: Creator not found. Generating generic content plan.")

    # Pull research data
    print("\n[1/3] Pulling research data from database...")
    research_data = get_research_data(product_category)
    print(f"  Loaded {len(research_data['videos'])} videos, {len(research_data['hook_patterns'])} hook patterns")

    # Analyze patterns
    print("[2/3] Analyzing patterns...")
    patterns = analyze_patterns(research_data)
    print(f"  Hook categories: {list(patterns['hook_categories'].keys())}")
    print(f"  CTA types: {list(patterns['cta_types'].keys())}")
    print(f"  Formats: {list(patterns['format_counts'].keys())}")

    # Derive structural rules from data
    print("[3/5] Deriving structural rules from data...")
    structural_rules = derive_structural_rules(research_data, patterns)
    print(f"  Generated {len(structural_rules.splitlines())} lines of structural rules")

    # Derive use case rules from product brief
    print("[4/5] Deriving product use case rules...")
    use_case_rules = derive_use_case_rules(product_brief)
    print(f"  Generated {len(use_case_rules.splitlines())} lines of use case rules")

    # Combine all rules
    all_rules = structural_rules + "\n" + use_case_rules

    # Build context and prompt
    print("[5/5] Building content generation prompt...")
    research_context = build_research_context(research_data, patterns)
    prompt = generate_content_plan_prompt(product_brief, research_context, all_rules, web_research, persona_context)

    stats = get_database_stats()
    print(f"\n  Database Stats:")
    print(f"  - {stats['total_videos']} videos analyzed")
    print(f"  - {stats['unique_creators']} unique creators")
    print(f"  - {stats['total_hooks']} hook patterns")
    print(f"  - {stats['total_cta_patterns']} CTA patterns")
    print(f"  - {stats['total_likes_across_db']:,} total likes across database")

    print(f"\n  Content generation prompt ready ({len(prompt)} chars)")
    print(f"  Pass this prompt to Claude to generate the content plan.\n")

    return {
        "prompt": prompt,
        "research_context": research_context,
        "patterns": patterns,
        "stats": stats,
        "persona": persona,
    }


# ═══════════════════════════════════════════════════════════════
#  DATA VALIDATION GATE
# ═══════════════════════════════════════════════════════════════
#
#  Validates ALL 10 TikTok tables in the database.
#
#  TIER 1 — REQUIRED per video (blocks pipeline if missing):
#    tiktok_videos fields: product_name, product_category, audio_type,
#                          content_type, hook_format, voice_delivery,
#                          performance_insight
#    video_transcripts:      (at least 1 row per video)
#    video_shot_breakdown:   (at least 1 row per video)
#    video_visual_notes:     (at least 1 row per video)
#    video_ctas:             (at least 1 row per video)
#    video_visual_scripts:   (at least 1 row per video)
#    video_onscreen_text:    (at least 1 row per video — frame analysis required)
#
#  TIER 3 — REFERENCE tables (global, not per-video):
#    hook_patterns:          aggregate patterns across all videos
#    cta_patterns:           aggregate CTA patterns across all videos
#    creators:               creator profiles
#
# ═══════════════════════════════════════════════════════════════

REQUIRED_VIDEO_FIELDS = [
    "product_name",
    "product_category",
    "audio_type",
    "content_type",
    "hook_format",
    "voice_delivery",
    "performance_insight",
]

# Tier 1: Must have at least 1 row per video — pipeline blocks if missing
REQUIRED_RELATED_TABLES = [
    "video_transcripts",
    "video_shot_breakdown",
    "video_visual_notes",
    "video_ctas",
    "video_visual_scripts",
    "video_onscreen_text",
]

# Tier 2: (currently empty — all per-video tables are now required)
RECOMMENDED_RELATED_TABLES = []

# Tier 3: Global reference tables — checked for minimum rows
REFERENCE_TABLES = {
    "hook_patterns": 1,       # Need at least 1 hook pattern
    "cta_patterns": 1,        # Need at least 1 CTA pattern
    "creators": 1,            # Need at least 1 creator profile
}

# ── QUALITY LAYER ──────────────────────────────────────────
# Not just "does a row exist?" but "does the row contain
# actual, meaningful content above a minimum threshold?"
#
# Each entry: table_name -> { field, min_length, reject_patterns }
#   field:            the column to check
#   min_length:       content must be at least this many characters
#   reject_patterns:  content matching any of these is flagged as low quality
#
QUALITY_THRESHOLDS = {
    "video_transcripts": {
        "field": "full_transcript",
        "min_length": 20,
        "reject_patterns": [
            "",
            "Music",
        ],
        # NOTE: [NO_SPEECH] is now a VALID format — these entries contain
        # meaningful descriptions of the video's audio format and on-screen
        # text content. They pass min_length and should not be rejected.
    },
    "video_visual_scripts": {
        "field": "full_visual_script",
        "min_length": 30,
        "reject_patterns": [],
    },
    "video_visual_notes": {
        "field": "overall_notes",
        "min_length": 30,
        "reject_patterns": [
            "Backfilled from shot_sequence data",
        ],
    },
    "video_shot_breakdown": {
        "field": "notes",
        "min_length": 20,
        "reject_patterns": [],
    },
}


def validate_video_completeness(video_id=None):
    """
    Validate data completeness AND quality for one or all videos across all 10 tables.

    Two-layer validation:
      Layer 1 (Row Presence): Does every required table have at least 1 row per video?
      Layer 2 (Data Quality): Does the row contain actual, meaningful content?

    Args:
        video_id: UUID string for a single video, or None to check all.

    Returns:
        dict with:
          - "passed": bool — True if all REQUIRED checks pass (Tier 1 + Quality)
          - "total_checked": int
          - "total_passed": int
          - "total_failed": int
          - "failures": list of {"video_id", "product_name", "missing_fields": [...]}
          - "quality_failures": list of {"video_id", "product_name", "issues": [...]}
          - "warnings": list of {"video_id", "product_name", "missing_fields": [...]}
          - "reference_table_status": dict of table -> {"rows": int, "ok": bool}
          - "quality_summary": dict with counts by issue type
    """
    # Get videos to check
    if video_id:
        videos = supabase.table("tiktok_videos").select("*").eq("id", video_id).execute().data
    else:
        videos = supabase.table("tiktok_videos").select("*").execute().data

    if not videos:
        return {"passed": True, "total_checked": 0, "total_passed": 0,
                "total_failed": 0, "failures": [], "quality_failures": [],
                "warnings": [], "reference_table_status": {},
                "quality_summary": {}}

    vid_ids = [v["id"] for v in videos]
    vid_set = set(vid_ids)

    # Batch-fetch all related tables (Tier 1 + Tier 2)
    def get_covered_ids(table_name):
        rows = supabase.table(table_name).select("video_id").execute().data
        return {r["video_id"] for r in rows if r.get("video_id") in vid_set}

    related_coverage = {}
    for table in REQUIRED_RELATED_TABLES + RECOMMENDED_RELATED_TABLES:
        related_coverage[table] = get_covered_ids(table)

    # ── QUALITY LAYER: Batch-fetch content for quality checks ──
    quality_data = {}  # table -> {video_id: field_value}
    for table, thresholds in QUALITY_THRESHOLDS.items():
        field = thresholds["field"]
        rows = supabase.table(table).select(f"video_id, {field}").execute().data
        quality_data[table] = {}
        for r in rows:
            vid = r.get("video_id")
            if vid in vid_set:
                quality_data[table][vid] = r.get(field, "")

    # Check Tier 3: Reference tables
    ref_status = {}
    for table, min_rows in REFERENCE_TABLES.items():
        rows = supabase.table(table).select("id", count="exact").limit(1).execute()
        count = rows.count if rows.count is not None else len(rows.data)
        ref_status[table] = {"rows": count, "ok": count >= min_rows}

    # Check each video
    failures = []           # Tier 1 failures (block)
    quality_failures = []   # Quality failures (block)
    warnings = []           # Tier 2 warnings (inform)

    for v in videos:
        missing_required = []
        missing_recommended = []
        quality_issues = []

        # Check required fields on tiktok_videos
        for field in REQUIRED_VIDEO_FIELDS:
            if v.get(field) is None or v.get(field) == "":
                missing_required.append(f"tiktok_videos.{field}")

        # Check Tier 1 required tables (row presence)
        for table in REQUIRED_RELATED_TABLES:
            if v["id"] not in related_coverage[table]:
                missing_required.append(f"{table} (no rows)")

        # Check Tier 2 recommended tables
        for table in RECOMMENDED_RELATED_TABLES:
            if v["id"] not in related_coverage[table]:
                missing_recommended.append(f"{table} (no rows — requires frame analysis)")

        # ── QUALITY CHECKS ──
        for table, thresholds in QUALITY_THRESHOLDS.items():
            content = quality_data.get(table, {}).get(v["id"], None)
            if content is None:
                continue  # Row missing entirely — already caught by Tier 1

            field = thresholds["field"]
            min_len = thresholds["min_length"]
            reject = thresholds["reject_patterns"]

            # Check minimum length
            if len(str(content).strip()) < min_len:
                quality_issues.append(
                    f"{table}.{field} — too short ({len(str(content).strip())} chars, need {min_len}+)"
                )
                continue

            # Check reject patterns
            for pattern in reject:
                if pattern and str(content).strip().startswith(pattern):
                    quality_issues.append(
                        f"{table}.{field} — matches reject pattern: '{pattern}...'"
                    )
                    break

        if missing_required:
            failures.append({
                "video_id": v["id"],
                "product_name": v.get("product_name", "UNKNOWN"),
                "missing_fields": missing_required,
            })

        if quality_issues:
            quality_failures.append({
                "video_id": v["id"],
                "product_name": v.get("product_name", "UNKNOWN"),
                "issues": quality_issues,
            })

        if missing_recommended:
            warnings.append({
                "video_id": v["id"],
                "product_name": v.get("product_name", "UNKNOWN"),
                "missing_fields": missing_recommended,
            })

    # Build quality summary (counts by issue type)
    from collections import Counter
    quality_summary = Counter()
    for qf in quality_failures:
        for issue in qf["issues"]:
            # Extract table.field portion for grouping
            key = issue.split(" — ")[0] if " — " in issue else issue
            quality_summary[key] += 1

    total_passed = len(videos) - len(failures) - len(
        [qf for qf in quality_failures if qf["video_id"] not in
         {f["video_id"] for f in failures}]
    )
    all_passed = len(failures) == 0 and len(quality_failures) == 0

    return {
        "passed": all_passed,
        "total_checked": len(videos),
        "total_passed": total_passed,
        "total_failed": len(videos) - total_passed,
        "failures": failures,
        "quality_failures": quality_failures,
        "warnings": warnings,
        "reference_table_status": ref_status,
        "quality_summary": dict(quality_summary),
    }


def print_validation_report(result=None):
    """Run validation and print a human-readable report."""
    if result is None:
        result = validate_video_completeness()

    print(f"\n{'═' * 60}")
    print(f"  DATA VALIDATION REPORT")
    print(f"  Layer 1: Row Presence  |  Layer 2: Data Quality")
    print(f"  Covers all 10 TikTok tables")
    print(f"{'═' * 60}")
    print(f"  Videos checked:      {result['total_checked']}")
    print(f"  Fully passed:        {result['total_passed']}")
    print(f"  Row failures:        {len(result.get('failures', []))}")
    print(f"  Quality failures:    {len(result.get('quality_failures', []))}")
    print(f"  Warnings:            {len(result.get('warnings', []))}")
    status = '✅ ALL CLEAR' if result['passed'] else '❌ ISSUES DETECTED'
    print(f"  Status:              {status}")

    # Tier 1 row-presence failures
    if result["failures"]:
        print(f"\n  {'─' * 56}")
        print(f"  TIER 1 — MISSING ROWS (blocks pipeline):")
        print(f"  {'─' * 56}")
        for f in result["failures"][:20]:
            print(f"  • {f['product_name']} ({f['video_id'][:8]}...)")
            for m in f["missing_fields"]:
                print(f"      ✗ {m}")
        if len(result["failures"]) > 20:
            print(f"  ... and {len(result['failures']) - 20} more")

    # Quality failures
    quality_failures = result.get("quality_failures", [])
    quality_summary = result.get("quality_summary", {})
    if quality_failures:
        print(f"\n  {'─' * 56}")
        print(f"  DATA QUALITY — LOW-QUALITY CONTENT (blocks pipeline):")
        print(f"  {'─' * 56}")
        print(f"  {len(quality_failures)} video(s) have content below quality thresholds:\n")
        # Show summary by issue type first
        for field, count in sorted(quality_summary.items(), key=lambda x: -x[1]):
            print(f"    ⚠ {field}: {count} video(s)")
        # Then list affected videos
        print(f"\n  Affected videos:")
        for qf in quality_failures[:30]:
            print(f"  • {qf['product_name']} ({qf['video_id'][:8]}...)")
            for issue in qf["issues"]:
                print(f"      ✗ {issue}")
        if len(quality_failures) > 30:
            print(f"  ... and {len(quality_failures) - 30} more")

    # Tier 2 warnings
    warnings = result.get("warnings", [])
    if warnings:
        print(f"\n  {'─' * 56}")
        print(f"  TIER 2 — RECOMMENDED ({len(warnings)} videos missing data):")
        print(f"  {'─' * 56}")
        from collections import Counter
        warn_counts = Counter()
        for w in warnings:
            for m in w["missing_fields"]:
                warn_counts[m] += 1
        for field, count in warn_counts.most_common():
            print(f"  ⚠ {field}: {count} videos")

    # Tier 3 reference tables
    ref_status = result.get("reference_table_status", {})
    if ref_status:
        print(f"\n  {'─' * 56}")
        print(f"  TIER 3 — REFERENCE TABLES:")
        print(f"  {'─' * 56}")
        for table, status in ref_status.items():
            icon = "✓" if status["ok"] else "✗"
            print(f"  {icon} {table}: {status['rows']} rows")

    print(f"\n{'═' * 60}\n")
    return result


if __name__ == "__main__":
    # Run validation gate first
    print_validation_report()

    # Then show stats
    stats = get_database_stats()
    print(f"Database contains: {stats}")
