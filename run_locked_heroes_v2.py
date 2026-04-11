#!/usr/bin/env python3
"""
Custom pipeline v2: Locked Heroes + Data-Driven Remix Mix
=========================================================

Takes 2 pre-selected hero videos and generates 10 data-driven remixes
with the CORRECT format mix based on database analysis:
  - 6 voiceover_broll (angles: lifestyle_aspiration x2, visual_demo x2, problem_solution x1, authentic_review x1)
  - 3 text_only + background_music (angles: lifestyle_aspiration x1, visual_demo x1, authentic_review x1)
  - 1 borrowed_voiceover style (angle: lifestyle_aspiration)

All format/angle/duration decisions backed by 128-video database analysis.
"""

import json
import os
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
os.chdir(SCRIPT_DIR)

# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

PRODUCT_NAME = "You Know Me Better Than Anyone"
PRODUCT_SUBTITLE = "2-Book Friendship Journal Set"
CREATOR_NAME = "Michelle"
OUTPUT_DIR = SCRIPT_DIR / "output" / "ykmb_locked_v2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
#  LOCKED HEROES (exactly as David selected)
# ═══════════════════════════════════════════════════════════════

LOCKED_HEROES = [
    {
        "number": 1,
        "title": "Hero 1 — BFF Questions Discovery",
        "hook": "Can somebody explain to me why we never asked these questions?",
        "hook_template": "HT9",
        "duration": "35-40s",
        "script": [
            {"type": "ON CAMERA", "text": "Can somebody explain to me why we never asked these questions?"},
            {"type": "ON CAMERA", "text": "I've been best friends with my girl for like six years and I'm sitting here reading this and I'm like... I didn't know half of this."},
            {"type": "VOICEOVER", "text": "So, this You Know Me Better Than Anyone book set is basically two matching books - one for you, one for your best friend."},
            {"type": "VOICEOVER", "text": "You both fill out your books, then swap, and read each other's answers."},
            {"type": "ON CAMERA", "text": "It has sections for memories, hypothetical questions, deep personal stuff, and space to attach photos too."},
            {"type": "ON CAMERA", "text": "We spent like three hours going through these and laughing until our sides hurt. It's like friendship therapy, but fun."},
            {"type": "ON CAMERA", "text": "I'm linking these right here because honestly every friendship needs this. They're selling out super fast so grab yours while they're still available."},
        ],
        "broll": [
            {"code": "B1", "timestamp": "[0:08-0:11]"},
            {"code": "B2", "timestamp": "[0:11-0:14]"},
            {"code": "B6", "timestamp": "[0:17-0:19]"},
            {"code": "B5", "timestamp": "[0:19-0:24]"},
        ],
    },
    {
        "number": 2,
        "title": "Hero 2 — Warning About Emotional Impact",
        "hook": "Please be careful when you're reading these answers.",
        "hook_template": "HT3",
        "duration": "35-40s",
        "script": [
            {"type": "ON CAMERA", "text": "Be careful when reading your friends answers."},
            {"type": "ON CAMERA", "text": "I'm not even joking. Me and my girl did these friendship journals and some of the stuff she wrote made me sob."},
            {"type": "VOICEOVER", "text": "The, You Know Me Better Than Anyone book set -- comes with two matching books that you fill out and then exchange with your best friend."},
            {"type": "ON CAMERA", "text": "Like there's fun questions about celebrities and hypotheticals, but then there's this whole other section called 'The Deep Stuff' and, girl..."},
            {"type": "VOICEOVER", "text": "It asks about boundaries, personal mantras, heartfelt sharing - stuff you thought you knew, but maybe never talked about."},
            {"type": "ON CAMERA", "text": "We were sitting on my couch reading and both of us were just crying and laughing and hugging. It was honestly really sweet."},
            {"type": "ON CAMERA", "text": "If you want to feel closer to your person, this is how you do it. But prepare yourself emotionally."},
            {"type": "ON CAMERA", "text": "I'll leave you the link down below in this video. They're printed in the US, but have limited stock, so make sure you grab your set quick."},
        ],
        "broll": [
            {"code": "B1", "timestamp": "[0:08-0:11]"},
            {"code": "B2", "timestamp": "[0:11-0:16]"},
            {"code": "B4", "timestamp": "[0:22-0:24]"},
        ],
    },
]

HERO_BROLL = {
    "B1": "Close-up of the pink book cover showing 'YOU know me better THAN ANYONE' text and BFF heart icon",
    "B2": "Hands holding both books side by side, showing they're identical",
    "B4": "Flipping through pages showing different colored sections - pink, blue, peach",
    "B5": "Close-up of creator writing in the book with a pen",
    "B6": "Pointing at specific question prompts on a page",
}


def banner(text):
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}\n")


def save_json(path, data, label=""):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    if label:
        print(f"  \u2713 Saved {label}: {path.name}")


def save_text(path, text, label=""):
    with open(path, "w") as f:
        f.write(text)
    if label:
        print(f"  \u2713 Saved {label}: {path.name}")


def load_pass1_context():
    """Load all data-driven constraints."""
    banner("PASS 1: Data + Rules + Analyzer Constraints")

    from content_engine import (
        get_research_data, analyze_patterns, derive_structural_rules,
        derive_use_case_rules, build_research_context,
        get_creator_persona, build_persona_context,
        get_upcoming_holidays, build_holiday_context,
    )

    brief_path = SCRIPT_DIR / "v2" / "products" / "ykmb" / "product_brief.txt"
    with open(brief_path) as f:
        product_brief = f.read()

    persona = get_creator_persona(creator_name=CREATOR_NAME)
    persona_context = build_persona_context(persona) if persona else ""

    upcoming = get_upcoming_holidays()
    holiday_context = build_holiday_context(upcoming, product_brief) if upcoming else ""

    research_data = get_research_data()
    patterns = analyze_patterns(research_data)
    structural_rules = derive_structural_rules(research_data, patterns)
    use_case_rules = derive_use_case_rules(product_brief)
    research_context = build_research_context(research_data, patterns)
    if holiday_context:
        structural_rules += "\n" + holiday_context

    from v2.pipeline.angle_scorer import build_angle_constraint_prompt
    from v2.pipeline.ost_patterns import build_ost_constraint_prompt
    from v2.pipeline.ost_copy_analyzer import build_ost_copy_constraint_prompt
    from v2.pipeline.broll_analyzer import build_broll_constraint_prompt
    from v2.pipeline.audio_analyzer import build_audio_constraint_prompt
    from v2.pipeline.cta_analyzer import build_cta_constraint_prompt
    from v2.pipeline.pacing_analyzer import build_pacing_constraint_prompt
    from v2.pipeline.structure_rules import build_structure_summary_prompt

    print(f"  All constraints loaded (including OST copy patterns)")

    return {
        "product_brief": product_brief,
        "persona_context": persona_context,
        "structural_rules": structural_rules,
        "use_case_rules": use_case_rules,
        "research_context": research_context,
        "angle_constraints": build_angle_constraint_prompt(product_brief),
        "ost_constraints": build_ost_constraint_prompt(),
        "ost_copy_constraints": build_ost_copy_constraint_prompt(
            content_angles=["lifestyle_aspiration", "visual_demo", "problem_solution", "authentic_review"]
        ),
        "broll_constraints": build_broll_constraint_prompt(),
        "audio_constraints": build_audio_constraint_prompt(),
        "cta_constraints": build_cta_constraint_prompt(),
        "pacing_constraints": build_pacing_constraint_prompt(),
        "structure_summary": build_structure_summary_prompt(),
    }


def build_scripts_prompt(ctx):
    """Build custom prompt with locked heroes + data-driven format mix."""

    locked_hero_block = ""
    for hero in LOCKED_HEROES:
        lines_text = ""
        for i, line in enumerate(hero["script"]):
            line_id = f"TH{hero['number']}-{'abcdefghij'[i]}"
            lines_text += f'        {{"id":"{line_id}","type":"{line["type"]}","text":"{line["text"]}"}}\n'
        locked_hero_block += f"""
--- LOCKED HERO {hero["number"]} ---
Title: {hero["title"]}
Hook: "{hero["hook"]}" | hook_template: "{hero["hook_template"]}"
Duration: {hero["duration"]}
Lines:
{lines_text}
B-roll: {", ".join(b["code"] + " " + b["timestamp"] for b in hero["broll"])}
"""

    hero_broll_block = "\n".join(f'  {code}: {desc}' for code, desc in sorted(HERO_BROLL.items()))

    prompt = f"""You are the world's most successful TikTok affiliate content strategist.

{ctx["persona_context"]}

═══════════════════════════════════════════════════
PERSONA ENFORCEMENT — NON-NEGOTIABLE
═══════════════════════════════════════════════════
The creator speaks at a normal-person level. No luxury brand references, no scientific
ingredient names, no industry jargon. She found this product because someone sent it to her
or she saw it on TikTok. She describes what she SEES and FEELS, not what ingredients DO.

THE GOLDEN TEST: "Would this person actually say this to her friend while making coffee?"

{ctx["structural_rules"]}

{ctx["use_case_rules"]}

{ctx["angle_constraints"]}

{ctx["ost_constraints"]}

{ctx["ost_copy_constraints"]}

═══════════════════════════════════════════════════
PRE-LOCKED HERO VIDEOS (DO NOT CHANGE)
═══════════════════════════════════════════════════
{locked_hero_block}

═══════════════════════════════════════════════════
B-ROLL POOL FROM LOCKED HEROES
═══════════════════════════════════════════════════
{hero_broll_block}

Remixes MUST reuse these shots. Add 7-10 new b-roll shots (B7+) for variety.
Total master list: 12-15 shots. Every remix uses at least 2 hero b-roll shots.

SOLO CREATOR CONSTRAINT (MANDATORY):
The creator (Michelle) shoots ALL content alone. Every b-roll shot MUST be filmable
by ONE person with ONE product unit. NEVER include shots that require a second person
(e.g., "two friends exchanging books", "handing a book to someone", "friend reacting").
All shots must show either: the product alone, the creator's own hands interacting with
the product, or the product in a setting. NO shots involving another human.

═══════════════════════════════════════════════════
VOICE NATURALNESS RULE
═══════════════════════════════════════════════════
Voiceover scripts must sound SPOKEN, not written. Trail off, self-correct, use filler.
Use contractions always. Read every line out loud — if it sounds like ad copy, rewrite it.

{ctx["structure_summary"]}

=== RESEARCH DATA FROM TOP-PERFORMING AFFILIATE VIDEOS ===
{ctx["research_context"]}

=== PRODUCT INFORMATION ===
{ctx["product_brief"]}

{ctx["broll_constraints"]}

{ctx["audio_constraints"]}

{ctx["cta_constraints"]}

{ctx["pacing_constraints"]}

═══════════════════════════════════════════════════════════════
ON-SCREEN TEXT TIMING (DATABASE-VERIFIED — NON-NEGOTIABLE)
═══════════════════════════════════════════════════════════════
Our database analysis of ALL remix-format videos shows:
  - 100% of remix OST entries appear at timestamp 0:00 (the VERY BEGINNING)
  - 67% are hook_text type (attention-grabbing text from frame 1)
  - 38% are persistent (stay the entire video), 62% non-persistent
  - For text_only videos: the on-screen text IS the hook — it must appear immediately
  - For voiceover videos: the on-screen hook reinforces the spoken hook

RULE: Every remix video's onScreenText MUST start at [0:00].
The on-screen text is a HOOK — it grabs attention in the first frame.
NEVER place on-screen text only at the end of the video.
The CTA should be a SEPARATE field, not crammed into the OST text.

═══════════════════════════════════════════════════════════════
DATA-DRIVEN REMIX FORMAT SPECIFICATION (MANDATORY)
═══════════════════════════════════════════════════════════════
Our database of 128 top-performing TikTok affiliate videos contains 49 remix-format videos.
The following format mix and angle assignments are derived DIRECTLY from that data.
You MUST follow this specification exactly.

REMIX FORMAT MIX (based on database distribution):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FORMAT 1: VOICEOVER + B-ROLL (6 remixes — VO1 through VO6)
  Database: 32 videos, avg 28s, 47K avg likes
  Audio: creator's own voiceover (original audio)
  Duration target: 15-25 seconds (tighter than DB avg — shorter VO remixes outperform per-second)
  Word count: 50-80 words max (at ~3.9 wps speaking rate)
  OST cards: 1 card (data avg = 1.3)
  CTAs: 1-2 per video (data avg = 1.7)

  VO1: angle = "lifestyle_aspiration" (DB: 10 videos, 30K avg likes for VO format)
       — show the product as part of an aspirational friendship moment
       Duration: 18-22s
       OST: 1 hook_text card at [0:00] (persistent or non-persistent)

  VO2: angle = "lifestyle_aspiration" (second take, different hook/concept)
       — different friendship scenario, same aspirational energy
       Duration: 20-25s
       OST: 1 hook_text card at [0:00]

  VO3: angle = "visual_demo" (DB: 7 videos, 41K avg likes for VO format)
       — walk through the product visually while narrating
       Duration: 15-20s
       OST: 1 hook_text card at [0:00]

  VO4: angle = "visual_demo" (second take)
       — focus on a different product feature (e.g., the swap moment, the deep stuff section)
       Duration: 15-18s
       OST: 1 hook_text card at [0:00]

  VO5: angle = "problem_solution" (DB: 4 videos, 88K avg likes — highest performing VO angle)
       — frame a friendship problem, position product as the answer
       Duration: 20-25s
       OST: 1 hook_text card at [0:00]

  VO6: angle = "authentic_review" (DB: 7 videos, 17K avg likes — high volume, good for variety)
       — genuine reaction/review of the experience
       Duration: 18-22s
       OST: 1 hook_text card at [0:00]

FORMAT 2: TEXT-ONLY + BACKGROUND MUSIC (3 remixes — TXT1, TXT2, TXT3)
  Database: 9 videos, avg 12.7s, 53K avg likes, NO VOICE AT ALL
  Audio: background music only (trending or lo-fi — NO voiceover, NO speaking)
  Duration target: 10-15 seconds
  OST cards: 1 card ONLY (data avg = 1.0 — this is critical, text_only videos use MINIMAL text)
  CTAs: 1 per video (on-screen text CTA only)
  These have NO voiceoverScript — they are purely visual: b-roll + music + 1 text overlay

  TXT1: angle = "lifestyle_aspiration" (DB: 3 videos, 89K avg likes — BEST text_only angle)
        — aesthetic b-roll montage of the books in a cozy setting
        Duration: 10-12s
        Single OST at [0:00]: hook_text — short punchy on-screen hook that grabs attention IMMEDIATELY
        (e.g., "the gift she actually wants 💕") — the CTA goes SEPARATELY in the cta field, NOT in the OST text

  TXT2: angle = "visual_demo" (DB: 2 videos, 72K avg likes)
        — quick visual showcase of the product features, no words needed
        Duration: 10-13s
        Single OST at [0:00]: hook_text — product-focused hook text that appears from the first frame

  TXT3: angle = "authentic_review" (DB: 4 videos, 16K avg likes)
        — authentic reaction/emotion captured in b-roll
        Duration: 12-15s
        Single OST at [0:00]: hook_text — emotional hook text that appears immediately

FORMAT 3: BORROWED/TRENDING AUDIO STYLE (1 remix — BA1)
  Database: 7 videos, avg 12.8s, 68K avg likes — HIGHEST avg likes of any remix format
  Audio: short punchy voiceover clip designed to feel like a trending sound bite
  Duration target: 10-15 seconds
  OST cards: 1 card (data avg = 1.3)
  CTAs: 1 per video
  The voiceover should be SHORT (2-3 sentences max, ~20-30 words) — punchy, quotable, trend-worthy

  BA1: angle = "lifestyle_aspiration" (DB: 4 videos, 101K avg likes — dominant angle)
       — short, catchy sound-bite style narration over aesthetic b-roll
       Duration: 12-15s
       OST: 1 hook_text card at [0:00]

═══════════════════════════════════════════════════════════════

=== YOUR TASK ===
Generate the SHOOT GUIDE JSON with 2 locked heroes + 10 data-driven remixes.

Structure:
{{"productName":"{PRODUCT_NAME}","productSubtitle":"{PRODUCT_SUBTITLE}","totalVideos":12,
"masterShotList":{{
  "talkingHeadShots":[
    {{TH1 — copy EXACTLY from locked hero 1}},
    {{TH2 — copy EXACTLY from locked hero 2}}
  ],
  "brollShots":[
    {{"label":"B1","description":"..."}},
    ... (B1-B6 from heroes + B7-B15 new shots)
  ],
  "voiceoverAudio":[
    {{"label":"VO1","format":"voiceover_broll","angle":"lifestyle_aspiration","concept":"...","script":"...","duration":"18-22s","context":"Tone note."}},
    {{"label":"VO2","format":"voiceover_broll","angle":"lifestyle_aspiration","concept":"...","script":"...","duration":"20-25s","context":"..."}},
    {{"label":"VO3","format":"voiceover_broll","angle":"visual_demo","concept":"...","script":"...","duration":"15-20s","context":"..."}},
    {{"label":"VO4","format":"voiceover_broll","angle":"visual_demo","concept":"...","script":"...","duration":"15-18s","context":"..."}},
    {{"label":"VO5","format":"voiceover_broll","angle":"problem_solution","concept":"...","script":"...","duration":"20-25s","context":"..."}},
    {{"label":"VO6","format":"voiceover_broll","angle":"authentic_review","concept":"...","script":"...","duration":"18-22s","context":"..."}},
    {{"label":"TXT1","format":"text_only","angle":"lifestyle_aspiration","concept":"...","script":"","ost_text":"[0:00] short hook text (NO CTA here — CTA is separate)","duration":"10-12s","music":"trending/lo-fi","context":"NO VOICEOVER — b-roll + music + 1 text card at 0:00"}},
    {{"label":"TXT2","format":"text_only","angle":"visual_demo","concept":"...","script":"","ost_text":"[0:00] product hook text","duration":"10-13s","music":"trending/lo-fi","context":"NO VOICEOVER — text at 0:00"}},
    {{"label":"TXT3","format":"text_only","angle":"authentic_review","concept":"...","script":"","ost_text":"[0:00] emotional hook text","duration":"12-15s","music":"emotional/trending","context":"NO VOICEOVER — text at 0:00"}},
    {{"label":"BA1","format":"borrowed_audio","angle":"lifestyle_aspiration","concept":"...","script":"Short 2-3 sentence sound bite (20-30 words max)","duration":"12-15s","music":"punchy trending style","context":"Sound-bite style — catchy, quotable, designed to feel like a trending audio"}}
  ]
}},
"propsAndSetup":{{"products":["..."],"locations":["..."],"wardrobe":["..."]}}
}}

CRITICAL REQUIREMENTS:
- TH1, TH2: LOCKED — copy word for word from pre-locked heroes
- VO1-VO6: voiceover_broll format, 50-80 words each, 15-25 seconds
- TXT1-TXT3: text_only format, NO voiceoverScript at all, just b-roll + music + 1 OST card, 10-15 seconds
- BA1: borrowed/trending audio style, very short punchy VO (20-30 words), 12-15 seconds
- 12-15 total b-roll shots (B1-B6 from heroes + new ones)
- Each voiceover must sound like a real person talking, not ad copy
- Each remix uses at least 2 hero b-roll shots (B1-B6)
- All b-roll filmable at home with one product unit
- The "format" and "angle" fields MUST match the specification above exactly

Output ONLY valid JSON. No commentary."""

    return prompt


def build_edit_guide_prompt(shoot_guide_data, product_brief):
    """Build edit guide prompt respecting format mix."""

    shoot_json_str = json.dumps(shoot_guide_data, indent=2)

    prompt = f"""You are a TikTok video editor creating a detailed Edit Guide from a completed Shoot Guide.

PRODUCT CONTEXT:
{product_brief[:2000]}

SHOOT GUIDE DATA:
{shoot_json_str}

CRITICAL RULES:
- Hero 1 and Hero 2 are LOCKED — transfer scripts exactly as-is
- There are THREE remix formats in this guide (this is DATA-DRIVEN, based on 128-video analysis):

  FORMAT 1: VOICEOVER + B-ROLL (VO1-VO6)
    - Has voiceoverScript text
    - Shot assembly from b-roll pool
    - 1 OST card (data says avg 1.3 for this format)
    - 1-2 CTAs
    - Duration: 15-25s

  FORMAT 2: TEXT-ONLY + BACKGROUND MUSIC (TXT1-TXT3)
    - Has NO voiceoverScript (leave empty string or omit)
    - Shot assembly from b-roll pool
    - 1 OST card ONLY (data says avg 1.0 for this format)
    - Music field should specify "trending" or "lo-fi" or "emotional" background track
    - Duration: 10-15s
    - These are the SHORTEST, punchiest remixes — pure visual + text

  FORMAT 3: BORROWED/TRENDING AUDIO STYLE (BA1)
    - Has a VERY SHORT voiceoverScript (2-3 sentences, ~20-30 words)
    - Designed to feel like a trending sound bite
    - 1 OST card
    - Duration: 12-15s

Create an Edit Guide JSON:
{{
  "productName": "{PRODUCT_NAME}",
  "productSubtitle": "{PRODUCT_SUBTITLE}",
  "heroVideos": [
    {{
      "title": "Hero 1 — [concept]",
      "angle": "...",
      "hook_template": "<HT#>",
      "format": "Talking Head + B-Roll",
      "duration": "35-40s",
      "music": "...",
      "hook": {{"spoken": "...", "onScreen": "..."}},
      "spokenScript": [{{"time": "0:00-0:03", "text": "...", "onCamera": true}}],
      "visualTimeline": [{{"time": "0:00-0:03", "shot": "TH1 CLOSE-UP", "description": "..."}}],
      "ctaStrategy": ["mid-roll CTA", "closing CTA"],
      "onScreenText": ["[0:00] hook text (max 4 cards — LAST = CTA)"]
    }}
  ],
  "remixVideos": [
    {{
      "title": "Remix concept",
      "angle": "<exact angle from shoot guide>",
      "format": "<voiceover_broll OR text_only OR borrowed_audio>",
      "duration": "<from shoot guide>",
      "music": "<background music type>",
      "voiceoverScript": "<full text OR empty string for text_only>",
      "shotAssembly": ["B2 — description", "B5 — description"],
      "onScreenText": ["[0:00] hook text that grabs attention from frame 1"],
      "cta": "CTA description (separate from OST)"
    }}
  ],
  "uploadDetails": {{
    "hashtags": [...],
    "captions": [...],
    "notes": [...],
    "schedule": ["Day 1: Hero 1", "Day 2: Remix 1 (VO)", "Day 3: Remix 2 (text_only)", ...]
  }}
}}

RULES:
- Hero scripts: LOCKED, transfer exactly
- Each remix must include the "format" field matching the shoot guide
- TEXT-ONLY remixes (TXT1-TXT3): voiceoverScript MUST be empty string ""
- TEXT-ONLY remixes: 1 OST card maximum
- VOICEOVER remixes (VO1-VO6): 1 OST card
- BORROWED AUDIO remix (BA1): 1 OST card
- ON-SCREEN TEXT TIMING (CRITICAL): Every remix's onScreenText MUST start at [0:00].
  Database shows 100% of remix OST appears at timestamp 0:00.
  The text is a HOOK — it grabs attention from the very first frame.
  Format: "[0:00] hook text here"
  The CTA goes in the separate "cta" field, NOT jammed into the onScreenText.
- Shot assembly for each remix: 4-6 b-roll shots from shared pool
- ALL b-roll shots must be filmable by ONE person (solo creator). No shots requiring 2 people.
- Posting schedule should alternate formats for variety

Return ONLY the JSON. No markdown."""

    return prompt


def main():
    from run_product import get_llm_client, call_llm, parse_json_from_response, CONFIG

    banner("LOCKED HEROES + DATA-DRIVEN REMIX MIX (v2)")
    print(f"  Product:  {PRODUCT_NAME}")
    print(f"  Creator:  {CREATOR_NAME}")
    print(f"  Heroes:   2 (locked)")
    print(f"  Remixes:  6 VO + 3 text-only + 1 borrowed audio")
    print(f"  Output:   {OUTPUT_DIR}")

    client = get_llm_client()
    if not client:
        print("\n  No API key. Set: export ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)
    print(f"  API key:  Connected\n")

    total_cost = 0.0

    ctx = load_pass1_context()

    # ── PASS 4: Scripts ──
    banner("PASS 4: Locked Heroes + Data-Driven Remixes")

    scripts_prompt = build_scripts_prompt(ctx)
    save_text(OUTPUT_DIR / "scripts_prompt.txt", scripts_prompt, "Scripts prompt")
    print(f"  Prompt: {len(scripts_prompt):,} chars")

    scripts_response, stats = call_llm(
        client, scripts_prompt,
        model=CONFIG["pipeline"]["script_model"],
        max_tokens=CONFIG["pipeline"]["max_tokens_scripts"],
        label="Pass 4 — Data-Driven Remixes",
    )
    total_cost += stats["cost"]
    save_text(OUTPUT_DIR / "scripts_raw_response.txt", scripts_response)

    shoot_guide = parse_json_from_response(scripts_response)
    if not shoot_guide:
        print("  Failed to parse shoot guide JSON.")
        sys.exit(1)
    save_json(OUTPUT_DIR / "shoot_guide.json", shoot_guide, "Shoot Guide")

    ths = shoot_guide.get("masterShotList", {}).get("talkingHeadShots", [])
    brolls = shoot_guide.get("masterShotList", {}).get("brollShots", [])
    vos = shoot_guide.get("masterShotList", {}).get("voiceoverAudio", [])
    print(f"\n  Structure: {len(ths)} heroes | {len(brolls)} b-roll | {len(vos)} remix clips")

    # Count formats
    vo_count = sum(1 for v in vos if v.get("format") == "voiceover_broll")
    txt_count = sum(1 for v in vos if v.get("format") == "text_only")
    ba_count = sum(1 for v in vos if v.get("format") in ("borrowed_audio", "borrowed_voiceover"))
    print(f"  Format mix: {vo_count} VO + {txt_count} text-only + {ba_count} borrowed audio")

    # ── PASS 4b: Edit Guide ──
    banner("PASS 4b: Edit Guide")

    edit_prompt = build_edit_guide_prompt(shoot_guide, ctx["product_brief"])
    save_text(OUTPUT_DIR / "edit_guide_prompt.txt", edit_prompt, "Edit Guide prompt")

    edit_response, stats = call_llm(
        client, edit_prompt,
        model=CONFIG["pipeline"]["script_model"],
        max_tokens=CONFIG["pipeline"]["max_tokens_scripts"],
        label="Pass 4b — Edit Guide",
    )
    total_cost += stats["cost"]
    save_text(OUTPUT_DIR / "edit_guide_raw_response.txt", edit_response)

    edit_guide = parse_json_from_response(edit_response)
    if not edit_guide:
        print("  Failed to parse edit guide JSON.")
        sys.exit(1)
    save_json(OUTPUT_DIR / "edit_guide.json", edit_guide, "Edit Guide")

    heroes = edit_guide.get("heroVideos", [])
    remixes = edit_guide.get("remixVideos", [])
    print(f"  Edit Guide: {len(heroes)} heroes, {len(remixes)} remixes")

    # Show format distribution
    for r in remixes:
        fmt = r.get("format", "unknown")
        angle = r.get("angle", "unknown")
        dur = r.get("duration", "?")
        vo_len = len(r.get("voiceoverScript", "").split()) if r.get("voiceoverScript") else 0
        print(f"    {r['title'][:40]:40s} | {fmt:20s} | {angle:22s} | {dur:8s} | {vo_len}w")

    # ── PASS 4c: Transform ──
    banner("PASS 4c: Transform to v3")

    from v2.pipeline.transform import transform_to_v3

    edit_content, shoot_content = transform_to_v3(
        shoot_guide, edit_guide, PRODUCT_NAME, CREATOR_NAME, analysis_count=128
    )

    if edit_content:
        save_json(OUTPUT_DIR / "edit_guide_v3.json", edit_content, "Edit Guide v3")
    if shoot_content:
        save_json(OUTPUT_DIR / "shoot_guide_v3.json", shoot_content, "Shoot Guide v3")

    # ── PASS 5: Validation ──
    banner("PASS 5: Validation")

    from v2.pipeline.validate_content import validate_content_plan, print_content_validation_report
    from v2.pipeline.quality_scorer import score_content_plan, print_quality_report

    content_plan = {
        "edit_guide_v3": edit_content, "shoot_guide_v3": shoot_content,
        "shoot_guide": shoot_content, "edit_guide": edit_content,
    }
    cv = validate_content_plan(content_plan)
    print_content_validation_report(cv)
    quality = score_content_plan(content_plan)
    print_quality_report(quality)

    # ── PASS 6: DOCX ──
    banner("PASS 6: Generate DOCX")

    from v2.templates.edit_guide_generator import generate_edit_guide as render_edit
    from v2.templates.shoot_guide_generator import generate_shoot_guide as render_shoot

    edit_docx = OUTPUT_DIR / "ykmb_locked_v2_Edit_Guide.docx"
    shoot_docx = OUTPUT_DIR / "ykmb_locked_v2_Shoot_Guide.docx"

    if edit_content:
        try:
            render_edit(edit_content, str(edit_docx))
            print(f"  \u2713 {edit_docx.name}")
        except Exception as e:
            print(f"  \u2717 Edit Guide: {e}")
    if shoot_content:
        try:
            render_shoot(shoot_content, str(shoot_docx))
            print(f"  \u2713 {shoot_docx.name}")
        except Exception as e:
            print(f"  \u2717 Shoot Guide: {e}")

    banner("COMPLETE")
    print(f"  Cost: ${total_cost:.4f}")
    for f in sorted(OUTPUT_DIR.iterdir()):
        marker = "\U0001f4cb" if f.suffix == ".docx" else "\U0001f4c4"
        print(f"  {marker} {f.name} ({f.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
