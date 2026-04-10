#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
  TikTok Affiliate Content Pipeline — Universal Orchestrator
═══════════════════════════════════════════════════════════════════

  One command. One product. Two finished docx guides.

  Usage:
    python run_product.py \
      --product "Dr.Melaxin Calcium Multi Balm Eye Stick" \
      --research raw_research.txt \
      --brief product_brief.txt \
      [--creator Michelle] \
      [--output-dir output/drmelaxin] \
      [--dry-run]

  What it does:
    Pass 0: Synthesize raw web research → structured product brief
    Pass 1: Pull database patterns + derive structural rules
    Pass 2: Generate hooks (3 rounds × 10 = 30 candidates)
    Pass 3: Score hooks algorithmically, select diverse top 5
    Pass 4: Generate full scripts (5 hero + 5 remix)
    Pass 5: Generate Shoot Guide docx
    Pass 6: Generate Edit Guide docx

  --dry-run mode builds all prompts and runs scoring but skips
  LLM API calls. Useful for testing without an API key.

═══════════════════════════════════════════════════════════════════
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime

# ── Load system config ──────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "system_config.json"

with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

# ── Imports from content engine ─────────────────────────────────

sys.path.insert(0, str(SCRIPT_DIR))
from content_engine import (
    generate_product_research,
    generate_content_plan_v2,
    get_creator_persona,
    build_persona_context,
    score_hook,
    score_and_rank_hooks,
    select_diverse_top_n,
    get_research_data,
    validate_video_completeness,
    print_validation_report,
)


# ═══════════════════════════════════════════════════════════════
#  LLM Client
# ═══════════════════════════════════════════════════════════════

def get_llm_client():
    """Initialize the Anthropic client. Returns None if no API key."""
    try:
        import anthropic
        client = anthropic.Anthropic()
        if not client.api_key:
            return None
        return client
    except Exception:
        return None


def call_llm(client, prompt, model=None, max_tokens=None, label=""):
    """Send a prompt to Claude and return the response text."""
    if client is None:
        raise RuntimeError("No LLM client available. Set ANTHROPIC_API_KEY.")

    model = model or CONFIG["pipeline"]["default_model"]
    max_tokens = max_tokens or CONFIG["pipeline"]["max_tokens_scripts"]

    print(f"  → Calling {model} ({label})...", flush=True)
    start = time.time()

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text
    elapsed = time.time() - start
    tokens_in = response.usage.input_tokens
    tokens_out = response.usage.output_tokens
    cost_in = tokens_in * 0.003 / 1000  # Sonnet input pricing approx
    cost_out = tokens_out * 0.015 / 1000  # Sonnet output pricing approx
    total_cost = cost_in + cost_out

    print(f"  ← {len(text)} chars | {tokens_in}→{tokens_out} tokens | ${total_cost:.4f} | {elapsed:.1f}s")
    return text, {"tokens_in": tokens_in, "tokens_out": tokens_out, "cost": total_cost, "elapsed": elapsed}


# ═══════════════════════════════════════════════════════════════
#  File I/O helpers
# ═══════════════════════════════════════════════════════════════

def save_text(filepath, content, label=""):
    """Save text to a file and print confirmation."""
    with open(filepath, "w") as f:
        f.write(content)
    print(f"  📄 Saved: {filepath.name} ({len(content):,} chars)" + (f" — {label}" if label else ""))


def save_json(filepath, data, label=""):
    """Save JSON to a file and print confirmation."""
    content = json.dumps(data, indent=2)
    with open(filepath, "w") as f:
        f.write(content)
    print(f"  📄 Saved: {filepath.name} ({len(content):,} chars)" + (f" — {label}" if label else ""))


def banner(text):
    """Print a section banner."""
    print(f"\n{'═' * 60}")
    print(f"  {text}")
    print(f"{'═' * 60}\n")


# ═══════════════════════════════════════════════════════════════
#  Parse hooks from LLM response
# ═══════════════════════════════════════════════════════════════

def parse_hooks_from_response(response_text):
    """Extract hook texts from an LLM response.
    Handles numbered lists, quoted strings, bullet points."""
    hooks = []

    # Try numbered list: 1. "hook text" or 1) hook text
    numbered = re.findall(r'^\s*\d+[\.\)]\s*["""]?(.+?)["""]?\s*$', response_text, re.MULTILINE)
    if numbered:
        return [h.strip().strip('"').strip('"').strip('"') for h in numbered]

    # Try bullet points
    bullets = re.findall(r'^\s*[-•]\s*["""]?(.+?)["""]?\s*$', response_text, re.MULTILINE)
    if bullets:
        return [h.strip().strip('"').strip('"').strip('"') for h in bullets]

    # Try quoted strings
    quoted = re.findall(r'["""](.+?)["""]', response_text)
    if quoted:
        return [h.strip() for h in quoted]

    # Fallback: non-empty lines
    lines = [l.strip() for l in response_text.strip().split('\n') if l.strip() and len(l.strip()) > 5]
    return lines[:CONFIG["pipeline"]["hooks_per_round"]]


# ═══════════════════════════════════════════════════════════════
#  Parse shoot guide JSON from LLM response
# ═══════════════════════════════════════════════════════════════

def parse_json_from_response(response_text):
    """Extract the first valid JSON object from an LLM response."""
    # Try to find JSON block
    json_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', response_text)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try raw JSON extraction (find largest valid JSON object)
    brace_start = response_text.find('{')
    if brace_start == -1:
        return None

    # Walk from the first { and find the matching }
    depth = 0
    in_string = False
    escape_next = False

    for i in range(brace_start, len(response_text)):
        char = response_text[i]

        if escape_next:
            escape_next = False
            continue

        if char == '\\':
            escape_next = True
            continue

        if char == '"' and not escape_next:
            in_string = not in_string
            continue

        if in_string:
            continue

        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(response_text[brace_start:i+1])
                except json.JSONDecodeError:
                    break

    return None


# ═══════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_pipeline(args):
    """Run the full content generation pipeline."""

    # ── Pre-flight: Data Validation Gate ─────────────────────
    banner("PRE-FLIGHT: Data Validation Gate")
    validation = print_validation_report()
    if not validation["passed"]:
        print(f"  ⚠ {validation['total_failed']} video(s) have incomplete data.")
        print(f"  Pipeline will proceed but results may be degraded.")
        print(f"  Run `python content_engine.py` for full report.\n")
        if not args.dry_run:
            response = input("  Continue anyway? [y/N]: ").strip().lower()
            if response != 'y':
                print("  Pipeline aborted. Fix data gaps first.")
                sys.exit(1)

    # ── Setup ────────────────────────────────────────────────

    product_name = args.product
    dry_run = args.dry_run
    output_dir = Path(args.output_dir) if args.output_dir else SCRIPT_DIR / "output" / product_name.lower().replace(" ", "_").replace(".", "")
    output_dir.mkdir(parents=True, exist_ok=True)

    creator_name = args.creator or CONFIG["persona"]["name"]

    # Slug for filenames
    slug = re.sub(r'[^a-z0-9]+', '_', product_name.lower()).strip('_')

    print(f"\n{'█' * 60}")
    print(f"  TIKTOK AFFILIATE CONTENT PIPELINE")
    print(f"  Product: {product_name}")
    print(f"  Creator: {creator_name}")
    print(f"  Output:  {output_dir}")
    print(f"  Mode:    {'DRY RUN (no API calls)' if dry_run else 'LIVE'}")
    print(f"{'█' * 60}")

    # ── Load inputs ──────────────────────────────────────────

    # Raw web research
    raw_research = ""
    if args.research:
        with open(args.research) as f:
            raw_research = f.read()
        print(f"\n  Loaded research: {args.research} ({len(raw_research):,} chars)")

    # Product brief
    product_brief = ""
    if args.brief:
        with open(args.brief) as f:
            product_brief = f.read()
        print(f"  Loaded brief: {args.brief} ({len(product_brief):,} chars)")
    elif not raw_research:
        print("\n  ⚠ No --research or --brief provided. Pipeline needs at least one.")
        print("    Provide raw research (web scrape) or a structured product brief.")
        sys.exit(1)

    # ── LLM Client ───────────────────────────────────────────

    client = None
    if not dry_run:
        client = get_llm_client()
        if client is None:
            print("\n  ⚠ No ANTHROPIC_API_KEY found in environment.")
            print("    Set it with: export ANTHROPIC_API_KEY=sk-ant-...")
            print("    Or run with --dry-run to test without API calls.\n")
            sys.exit(1)
        print(f"  API key: ✓ Connected")

    total_cost = 0.0

    # ═════════════════════════════════════════════════════════
    #  PASS 0: Product Research Synthesis
    # ═════════════════════════════════════════════════════════

    banner("PASS 0: Product Research Synthesis")

    if product_brief:
        print("  Using provided product brief (skipping Pass 0)")
        product_research = product_brief
    elif raw_research:
        pass0 = generate_product_research(product_name, raw_research)
        save_text(output_dir / "pass0_prompt.txt", pass0["research_prompt"], "Pass 0 prompt")

        if dry_run:
            print("  [DRY RUN] Would call LLM to synthesize research. Skipping.")
            product_research = raw_research  # Use raw as fallback in dry run
        else:
            product_research, stats = call_llm(
                client,
                pass0["research_prompt"],
                model=CONFIG["pipeline"]["research_model"],
                max_tokens=CONFIG["pipeline"]["max_tokens_research"],
                label="Pass 0 — Product Research",
            )
            total_cost += stats["cost"]

        save_text(output_dir / "product_research.txt", product_research)
    else:
        product_research = ""

    # ═════════════════════════════════════════════════════════
    #  PASS 1 + 2: Data, Rules, Hook Generation
    # ═════════════════════════════════════════════════════════

    banner("PASS 1: Database Patterns + Structural Rules")

    # Need a product brief for the engine — use research as brief if no explicit brief
    engine_brief = product_brief or product_research

    result1 = generate_content_plan_v2(
        product_brief=engine_brief,
        product_category=args.category,
        creator_name=creator_name,
        product_research=product_research,
    )

    if result1["stage"] != "hooks_needed":
        print(f"  ⚠ Unexpected stage: {result1['stage']}")
        sys.exit(1)

    save_text(output_dir / "hooks_prompt.txt", result1["hooks_prompt"], "Hooks prompt")

    if CONFIG["output"]["save_intermediates"] and "pass1_context" in result1:
        save_json(output_dir / "pass1_context.json", result1.get("pass1_context", {}))

    # ═════════════════════════════════════════════════════════
    #  PASS 2: Hook Generation (3 rounds)
    # ═════════════════════════════════════════════════════════

    banner("PASS 2: Hook Generation (3 rounds)")

    hook_rounds = CONFIG["pipeline"]["hook_rounds"]
    hook_responses = []

    if dry_run:
        print(f"  [DRY RUN] Would generate {hook_rounds} rounds of hooks. Skipping.")
        print(f"  To test scoring, provide pre-generated hooks with --hooks-file")
    else:
        for i in range(hook_rounds):
            response, stats = call_llm(
                client,
                result1["hooks_prompt"],
                model=CONFIG["pipeline"]["default_model"],
                max_tokens=CONFIG["pipeline"]["max_tokens_hooks"],
                label=f"Pass 2 — Hooks Round {i+1}/{hook_rounds}",
            )
            hook_responses.append(response)
            total_cost += stats["cost"]

            save_text(output_dir / f"hooks_round_{i+1}.txt", response)

    # ── Load pre-existing hooks if provided ──────────────────

    if args.hooks_file:
        print(f"\n  Loading pre-generated hooks from {args.hooks_file}")
        with open(args.hooks_file) as f:
            hook_responses = [f.read()]
        print(f"  Loaded {len(hook_responses)} hook response(s)")

    if not hook_responses:
        if dry_run:
            print("\n  ✓ Dry run complete through Pass 1. Prompts saved.")
            print(f"  Output directory: {output_dir}")
            return
        else:
            print("\n  ⚠ No hooks generated or loaded. Cannot continue.")
            sys.exit(1)

    # ═════════════════════════════════════════════════════════
    #  PASS 3: Hook Scoring + Selection
    # ═════════════════════════════════════════════════════════

    banner("PASS 3: Hook Scoring + Selection")

    result2 = generate_content_plan_v2(
        product_brief=engine_brief,
        product_category=args.category,
        creator_name=creator_name,
        hook_responses=hook_responses,
        product_research=product_research,
    )

    if result2["stage"] != "scripts_ready":
        print(f"  ⚠ Unexpected stage: {result2['stage']}")
        sys.exit(1)

    save_json(output_dir / "hooks_scored.json", result2["scored_hooks"])
    save_json(output_dir / "locked_hooks.json", result2["locked_hooks"])
    save_text(output_dir / "scripts_prompt.txt", result2["scripts_prompt"], "Scripts prompt")

    print(f"\n  Locked hooks (top {CONFIG['pipeline']['top_n_hooks']}):")
    for i, h in enumerate(result2["locked_hooks"]):
        hook_display = h if isinstance(h, str) else h.get("text", str(h))
        print(f"    Hero {i+1}: \"{hook_display}\"")

    # ═════════════════════════════════════════════════════════
    #  PASS 4: Script Generation
    # ═════════════════════════════════════════════════════════

    banner("PASS 4: Script Generation")

    if dry_run:
        print("  [DRY RUN] Would call LLM to generate scripts. Skipping.")
        print(f"  Scripts prompt saved to: {output_dir / 'scripts_prompt.txt'}")
        print(f"\n  ✓ Dry run complete through Pass 3. All prompts and scores saved.")
        print(f"  Output directory: {output_dir}")
        return

    scripts_response, stats = call_llm(
        client,
        result2["scripts_prompt"],
        model=CONFIG["pipeline"]["script_model"],
        max_tokens=CONFIG["pipeline"]["max_tokens_scripts"],
        label="Pass 4 — Scripts",
    )
    total_cost += stats["cost"]

    save_text(output_dir / "scripts_raw_response.txt", scripts_response)

    # Parse the JSON from the response
    shoot_guide = parse_json_from_response(scripts_response)
    if not shoot_guide:
        print("  ⚠ Failed to parse shoot guide JSON from response.")
        print("  Raw response saved. Manual extraction needed.")
        return

    save_json(output_dir / "shoot_guide.json", shoot_guide, "Shoot Guide data")

    # Validate shoot guide structure
    ths = shoot_guide.get("masterShotList", {}).get("talkingHeadShots", [])
    brolls = shoot_guide.get("masterShotList", {}).get("brollShots", [])
    vos = shoot_guide.get("masterShotList", {}).get("voiceoverAudio", [])
    print(f"\n  Structure: {len(ths)} heroes | {len(brolls)} b-roll | {len(vos)} VO clips")

    # ═════════════════════════════════════════════════════════
    #  PASS 4b: Edit Guide Generation
    # ═════════════════════════════════════════════════════════

    banner("PASS 4b: Edit Guide Generation")

    # Build edit guide prompt from the shoot guide data
    edit_guide_prompt = build_edit_guide_prompt(shoot_guide, product_research, engine_brief)
    save_text(output_dir / "edit_guide_prompt.txt", edit_guide_prompt)

    edit_response, stats = call_llm(
        client,
        edit_guide_prompt,
        model=CONFIG["pipeline"]["script_model"],
        max_tokens=CONFIG["pipeline"]["max_tokens_scripts"],
        label="Pass 4b — Edit Guide",
    )
    total_cost += stats["cost"]

    save_text(output_dir / "edit_guide_raw_response.txt", edit_response)

    edit_guide = parse_json_from_response(edit_response)
    if not edit_guide:
        print("  ⚠ Failed to parse edit guide JSON from response.")
        print("  Raw response saved. Manual extraction needed.")
    else:
        save_json(output_dir / "edit_guide.json", edit_guide, "Edit Guide data")

    # ═════════════════════════════════════════════════════════
    #  PASS 4c: Transform LLM Output → Content Contract
    # ═════════════════════════════════════════════════════════

    banner("PASS 4c: Transform to Content Contract")

    from v2.pipeline.transform import transform_to_v3

    if shoot_guide and edit_guide:
        edit_content, shoot_content = transform_to_v3(
            shoot_guide, edit_guide, product_name, creator_name, analysis_count=128
        )
    else:
        edit_content = None
        shoot_content = None

    if edit_content:
        save_json(output_dir / "edit_guide_v3.json", edit_content, "Edit Guide (v3 schema)")
        heroes = [v for v in edit_content.get("videos", []) if v["type"] == "hero"]
        remixes = [v for v in edit_content.get("videos", []) if v["type"] == "remix"]
        print(f"  Transformed: {len(heroes)} heroes, {len(remixes)} remixes → {len(edit_content['videos'])} video concepts")
    if shoot_content:
        save_json(output_dir / "shoot_guide_v3.json", shoot_content, "Shoot Guide (v3 schema)")
        print(f"  Derived: {len(shoot_content.get('on_camera',[]))} on-camera groups, {len(shoot_content.get('broll',[]))} b-roll, {len(shoot_content.get('voiceovers',[]))} VOs")

    # ═════════════════════════════════════════════════════════
    #  PASS 5: Content Validation Gate
    # ═════════════════════════════════════════════════════════

    banner("PASS 5: Content Validation Gate")

    from v2.pipeline.validate_content import validate_content_plan, print_content_validation_report

    # Build content plan — v3 format stores both under their own keys
    content_plan = {
        "edit_guide_v3": edit_content,
        "shoot_guide_v3": shoot_content,
        # Legacy keys for validator compatibility
        "shoot_guide": shoot_content,
        "edit_guide": edit_content,
    }

    cv_result = validate_content_plan(content_plan)
    print_content_validation_report(cv_result)

    if not cv_result["passed"]:
        critical_count = sum(1 for f in cv_result["failures"] if f["severity"] == "critical")
        if critical_count > 0:
            print(f"\n  ⚠ {critical_count} CRITICAL validation failure(s).")
            print(f"  Content may produce inconsistent guides. Review failures above.")
        else:
            print(f"\n  ⚠ {len(cv_result['failures'])} warning(s) — non-critical. Proceeding.")

    # Quality scoring
    from v2.pipeline.quality_scorer import score_content_plan, print_quality_report

    quality = score_content_plan(content_plan)
    print_quality_report(quality)

    if not quality["production_ready"]:
        print(f"\n  ⚠ Quality score {quality['total']}/100 (Grade {quality['grade']}) — below production threshold.")
        print(f"  Documents will still be generated, but review scoring notes above.")

    # Save content.json contract (combines shoot + edit into one file)
    content_json_path = output_dir / "content.json"
    content_plan["_validation"] = {
        "passed": cv_result["passed"],
        "critical_failures": cv_result["stats"].get("critical", 0),
        "warnings": cv_result["stats"].get("warnings", 0),
    }
    content_plan["_quality"] = {
        "total": quality["total"],
        "grade": quality["grade"],
        "production_ready": quality["production_ready"],
        "dimensions": quality["dimensions"],
    }
    save_json(content_json_path, content_plan, "Content contract (shoot + edit)")

    # ═════════════════════════════════════════════════════════
    #  PASS 6: Generate DOCX Files (v2 Python generators)
    # ═════════════════════════════════════════════════════════

    banner("PASS 6: Generate DOCX Files")

    from v2.templates.edit_guide_generator import generate_edit_guide as render_edit_guide
    from v2.templates.shoot_guide_generator import generate_shoot_guide as render_shoot_guide

    edit_guide_docx = output_dir / f"{slug}_Edit_Guide.docx"
    shoot_guide_docx = output_dir / f"{slug}_Shoot_Guide.docx"

    # Generate Edit Guide docx (Document 1 — video concepts)
    if edit_content:
        try:
            render_edit_guide(edit_content, str(edit_guide_docx))
            print(f"  ✓ {edit_guide_docx.name}")
        except (ValueError, Exception) as e:
            print(f"  ✗ Edit Guide docx failed: {e}")

    # Generate Shoot Guide docx (Document 2 — derived shooting checklist)
    if shoot_content:
        try:
            render_shoot_guide(shoot_content, str(shoot_guide_docx))
            print(f"  ✓ {shoot_guide_docx.name}")
        except (ValueError, Exception) as e:
            print(f"  ✗ Shoot Guide docx failed: {e}")

    # ═════════════════════════════════════════════════════════
    #  SUMMARY
    # ═════════════════════════════════════════════════════════

    banner("PIPELINE COMPLETE")

    print(f"  Product:     {product_name}")
    print(f"  Creator:     {creator_name}")
    print(f"  Total cost:  ${total_cost:.4f}")
    print(f"  Output dir:  {output_dir}")
    print(f"\n  Files:")

    for f in sorted(output_dir.iterdir()):
        size_kb = f.stat().st_size / 1024
        marker = "📋" if f.suffix == ".docx" else "📄"
        print(f"    {marker} {f.name} ({size_kb:.1f} KB)")

    # Save run manifest
    manifest = {
        "product": product_name,
        "creator": creator_name,
        "timestamp": datetime.now().isoformat(),
        "total_cost": total_cost,
        "config_version": CONFIG["version"],
        "models_used": {
            "research": CONFIG["pipeline"]["research_model"],
            "hooks": CONFIG["pipeline"]["default_model"],
            "scripts": CONFIG["pipeline"]["script_model"],
        },
        "quality_score": quality["total"] if 'quality' in dir() else None,
        "quality_grade": quality["grade"] if 'quality' in dir() else None,
        "production_ready": quality["production_ready"] if 'quality' in dir() else None,
        "output_files": [f.name for f in sorted(output_dir.iterdir())],
    }
    save_json(output_dir / "run_manifest.json", manifest, "Run manifest")


# ═══════════════════════════════════════════════════════════════
#  Edit Guide Prompt Builder
# ═══════════════════════════════════════════════════════════════

def build_edit_guide_prompt(shoot_guide_data, product_research, product_brief):
    """Build a prompt that takes shoot guide data and produces an edit guide JSON."""

    shoot_json_str = json.dumps(shoot_guide_data, indent=2)

    prompt = f"""You are a TikTok video editor creating a detailed Edit Guide from a completed Shoot Guide.

PRODUCT CONTEXT:
{product_brief[:2000]}

SHOOT GUIDE DATA (the raw material you're working with):
{shoot_json_str}

YOUR TASK: Create an Edit Guide JSON that tells the editor exactly how to assemble 5 hero videos + 5 remix videos from the shot material above.

The Edit Guide must be a JSON object with this exact structure:
{{
  "productName": "...",
  "productSubtitle": "...",
  "heroVideos": [
    {{
      "title": "Hero 1 — [concept]",
      "angle": "...",
      "hook_template": "<HT# from shoot guide — MUST include>",
      "format": "Talking Head + B-Roll",
      "duration": "30-45s",
      "music": "...",
      "hook": {{
        "spoken": "the hook line",
        "onScreen": "the hook line + optional emoji"
      }},
      "spokenScript": [
        {{
          "time": "0:00-0:03",
          "text": "the spoken line",
          "onCamera": true
        }}
      ],
      "visualTimeline": [
        {{
          "time": "0:00-0:03",
          "shot": "TH1 CLOSE-UP",
          "description": "description of what's on screen"
        }}
      ],
      "ctaStrategy": ["mid-roll value prop CTA", "closing urgency CTA — MUST be the final element"],
      "onScreenText": ["[0:00] hook text (max 4 cards total — LAST card MUST be a CTA)"]
    }}
  ],
  "remixVideos": [
    {{
      "title": "The Comparison",
      "angle": "...",
      "format": "B-Roll + Voiceover",
      "duration": "15-25s",
      "music": "...",
      "voiceoverScript": "full voiceover text",
      "shotAssembly": ["B2 — description", "B5 — description"],
      "onScreenText": ["[0:00] text overlay (max 3 cards — LAST card MUST be a CTA)"],
      "cta": "CTA description — REQUIRED, every remix MUST have a closing CTA"
    }}
  ],
  "uploadDetails": {{
    "hashtags": ["#tag1", "#tag2"],
    "captions": ["suggested caption 1"],
    "notes": ["posting tip 1"],
    "schedule": ["Day 1: Hero 1", "Day 2: Remix 1"]
  }}
}}

RULES:
- Each hero video maps to one talking head from the shoot guide (TH1 → Hero 1, etc.)
- Visual timeline must reference actual shot labels from the shoot guide (TH1, B1, B2, etc.)
- Remix videos are assembled from b-roll + voiceover audio — minimal or no talking head
- On-screen text overlays go in the edit guide (NOT the shoot guide)
- Include timestamps for every spoken script line and visual timeline entry
- CTA strategy should specify mid-roll and closing CTA approaches
- Keep the language consistent with the persona in the shoot guide scripts
- Include the "hook_template" field from each hero's shoot guide data (e.g. "HT3", "HT5", "ORIGINAL")

ON-SCREEN TEXT LIMITS (DATA-DRIVEN — MANDATORY):
Our analysis of 128 top-performing TikTok affiliate videos shows:
- 83% of top videos use 1-2 on-screen text cards total
- Maximum 4 OST cards per hero video (HARD LIMIT — do NOT exceed this)
- Maximum 3 OST cards per remix video
- The hook OST should appear at 0:00-0:02 (first 2 seconds)
- Any CTA OST should appear in the final 15% of the video
- Less is more — viewers skip videos with too much text on screen

CTA PLACEMENT (DATA-DRIVEN — MANDATORY):
Every hero video and every remix video MUST end with a clear call-to-action.
- Each hero MUST have a CTA in its final spokenScript entry AND in its ctaStrategy
- Each remix MUST have a "cta" field with explicit closing CTA text
- CTA types proven to work: "link in bio", "orange shopping cart", "link below", "tap the link"
- Mid-roll CTAs (around 40-60% of video) should be value_proposition style
- Closing CTAs (final 10-15% of video) should be urgency/scarcity style
- The LAST item in onScreenText for every video MUST be a CTA overlay

Return ONLY the JSON. No markdown, no explanation."""

    return prompt


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="TikTok Affiliate Content Pipeline — Generate shoot & edit guides for any product",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full run with API key set:
  python run_product.py --product "Dr.Melaxin Eye Balm" --research research.txt --brief brief.txt

  # Dry run (no API calls, tests prompts and scoring):
  python run_product.py --product "Test Product" --research research.txt --dry-run

  # Resume from pre-generated hooks:
  python run_product.py --product "Dr.Melaxin Eye Balm" --brief brief.txt --hooks-file hooks.txt
        """,
    )

    parser.add_argument("--product", required=True, help="Product name")
    parser.add_argument("--research", help="Path to raw web research text file")
    parser.add_argument("--brief", help="Path to structured product brief (skips Pass 0)")
    parser.add_argument("--category", help="Product category for database filtering (optional)")
    parser.add_argument("--creator", help=f"Creator persona name (default: {CONFIG['persona']['name']})")
    parser.add_argument("--output-dir", help="Output directory (default: output/<product_slug>)")
    parser.add_argument("--hooks-file", help="Path to pre-generated hooks file (skips Pass 2)")
    parser.add_argument("--dry-run", action="store_true", help="Build prompts only, skip LLM calls")

    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
