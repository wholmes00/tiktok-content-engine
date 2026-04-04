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
import subprocess
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
    #  PASS 5+6: Generate DOCX Files
    # ═════════════════════════════════════════════════════════

    banner("PASS 5: Generate DOCX Files")

    shoot_guide_json = output_dir / "shoot_guide.json"
    edit_guide_json = output_dir / "edit_guide.json"

    shoot_guide_docx = output_dir / f"{slug}_Shoot_Guide.docx"
    edit_guide_docx = output_dir / f"{slug}_Edit_Guide.docx"

    shoot_template = SCRIPT_DIR / CONFIG["output"]["shoot_guide_template"]
    edit_template = SCRIPT_DIR / CONFIG["output"]["edit_guide_template"]

    # Generate Shoot Guide docx
    if shoot_guide_json.exists():
        result = subprocess.run(
            ["node", str(shoot_template), str(shoot_guide_json), str(shoot_guide_docx)],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"  ✓ {shoot_guide_docx.name}")
        else:
            print(f"  ✗ Shoot Guide docx failed: {result.stderr}")

    # Generate Edit Guide docx
    if edit_guide_json.exists():
        result = subprocess.run(
            ["node", str(edit_template), str(edit_guide_json), str(edit_guide_docx)],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"  ✓ {edit_guide_docx.name}")
        else:
            print(f"  ✗ Edit Guide docx failed: {result.stderr}")

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
      "ctaStrategy": ["bullet points about CTA placement"],
      "onScreenText": ["[0:00] text overlay description"]
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
      "onScreenText": ["[0:00] text overlay"],
      "cta": "CTA description"
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
