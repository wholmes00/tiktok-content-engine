# TikTok Content Engine v2.0.0

Data-driven content generation pipeline for TikTok Shop affiliate videos. Produces shoot guides and edit guides for creators and editors, with every creative decision backed by engagement data from a database of 122+ analyzed videos.

## Architecture

The engine uses a three-system design:

**System 1 — Data Engine** (`analysis_pipeline.py`): Ingests TikTok videos, extracts hooks, on-screen text, CTAs, transcripts, and engagement metrics into a Supabase PostgreSQL database.

**System 2 — Content Engine** (`content_engine.py`): Multi-pass generation pipeline that produces content plans. Pass 1 pulls data and derives rules. Pass 2 generates hooks from proven templates. Pass 3 scores hooks automatically. Pass 4 builds full scripts around the top-scoring hooks.

**System 3 — Document Engine** (`v2/templates/`): Reads `content.json` (the contract between systems) and renders production-ready `.docx` shoot guides and edit guides.

## Full Pipeline (run_product.py)

The orchestrator runs all passes end-to-end:

```
Pass 0:  Product research synthesis (optional)
Pass 1:  Database patterns + structural rules
Pass 2:  Hook generation (3 rounds × 10 = 30 candidates)
Pass 3:  Hook scoring + diverse selection (top 5)
Pass 4:  Script generation (shoot guide JSON)
Pass 4b: Edit guide generation (edit guide JSON)
Pass 5:  Content validation gate (template rules, persona, structure)
Pass 6:  Quality scoring + DOCX rendering (v2 Python generators)
```

Requires `ANTHROPIC_API_KEY` environment variable. Use `--dry-run` to test without API calls.

## Data-Driven Pipeline Modules

All modules live in `v2/pipeline/` and query the database at runtime:

- **`angle_scorer.py`** — Ranks 11 content angles by weighted engagement (shares x3 + comments x2 + likes). Top angles drive hero concept selection.
- **`hook_templates.py`** — 12 structural templates decomposed from 94 top-performing hooks. Warning category averages 97K engagement. Monthly auto-refresh scans for new patterns.
- **`ost_patterns.py`** — 9 on-screen text templates mined from 182 OST entries. Key insight: 83% of top videos use just 1 OST at the hook position.
- **`validate_content.py`** — Three-tier content validation: structural checks, template rule compliance, and quality thresholds. Runs automatically before document generation.
- **`quality_scorer.py`** — Scores content plans 0-100 across four dimensions: hook strength, persona compliance, structure adherence, and CTA/engagement. Grade A (90+) = production ready.

## Running for a product

```bash
# Full pipeline (requires ANTHROPIC_API_KEY)
export ANTHROPIC_API_KEY=sk-ant-...
python run_product.py --product "BoomBoom Nasal Stick" --category nasal_stick --creator Michelle

# Dry run (builds prompts, tests scoring, no API calls)
python run_product.py --product "BoomBoom Nasal Stick" --research research.txt --dry-run

# Render guides from existing content.json
python -c "
import json
from v2.templates.shoot_guide_generator import generate_shoot_guide
from v2.templates.edit_guide_generator import generate_edit_guide

with open('v2/products/boomboom/content.json') as f:
    data = json.load(f)

generate_shoot_guide(data['shoot_guide'], 'output/shoot_guide.docx')
generate_edit_guide(data['edit_guide'], 'output/edit_guide.docx')
"

# Validate + score existing content
python -c "
import json
from v2.pipeline.validate_content import validate_content_plan, print_content_validation_report
from v2.pipeline.quality_scorer import score_content_plan, print_quality_report

with open('v2/products/boomboom/content.json') as f:
    data = json.load(f)

result = validate_content_plan(data)
print_content_validation_report(result)

quality = score_content_plan(data)
print_quality_report(quality)
"
```

## Project structure

```
tiktok_engine/
  content_engine.py          # System 2 — multi-pass content generation
  analysis_pipeline.py       # System 1 — video analysis & data ingestion
  system_config.json         # Persona constraints, filming rules, product assumptions
  run_product.py             # CLI entry point — full pipeline orchestrator
  v2/
    pipeline/
      db.py                  # Centralized Supabase client
      angle_scorer.py        # Content angle rankings from database
      hook_templates.py      # Hook template engine (12 templates + auto-refresh)
      ost_patterns.py        # OST pattern mining (9 templates)
      validate.py            # DOCX structural validation
      validate_content.py    # Content plan validation (3 tiers)
      quality_scorer.py      # Quality scoring (0-100, 4 dimensions)
    templates/
      shoot_guide_generator.py   # Renders shoot guide .docx
      edit_guide_generator.py    # Renders edit guide .docx
      styles.py                  # Shared document styles
      approved/                  # Base .docx templates
    products/
      boomboom/
        content.json         # BoomBoom content plan (the contract)
```

## Configuration

The Supabase connection uses the anon (public read-only) key by default, configured in `v2/pipeline/db.py`. Override with environment variables:

```bash
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-key-here"
```

## Persona constraints

Defined in `system_config.json`:
- `solo_only`: Creator films alone (no other people in frame)
- `home_only`: All filming at home (no gym, store, or external locations)
- `single_product_assumption`: One unit of the product available

## Dependencies

```
python-docx
supabase
anthropic
```
