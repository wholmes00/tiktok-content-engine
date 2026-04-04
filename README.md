# TikTok Content Engine v2.0.0

Data-driven content generation pipeline for TikTok Shop affiliate videos. Produces shoot guides and edit guides for creators and editors, with every creative decision backed by engagement data from a database of 122+ analyzed videos.

## Architecture

The engine uses a three-system design:

**System 1 — Data Engine** (`analysis_pipeline.py`): Ingests TikTok videos, extracts hooks, on-screen text, CTAs, transcripts, and engagement metrics into a Supabase PostgreSQL database.

**System 2 — Content Engine** (`content_engine.py`): Multi-pass generation pipeline that produces content plans. Pass 1 pulls data and derives rules. Pass 2 generates hooks from proven templates. Pass 3 scores hooks automatically. Pass 4 builds full scripts around the top-scoring hooks.

**System 3 — Document Engine** (`v2/templates/`): Reads `content.json` (the contract between systems) and renders production-ready `.docx` shoot guides and edit guides.

## Data-Driven Pipeline Modules

All three modules live in `v2/pipeline/` and query the database at runtime:

- **`angle_scorer.py`** — Ranks 11 content angles by weighted engagement (shares x3 + comments x2 + likes). Top angles drive hero concept selection.
- **`hook_templates.py`** — 12 structural templates decomposed from 94 top-performing hooks. Warning category averages 97K engagement. Monthly auto-refresh scans for new patterns.
- **`ost_patterns.py`** — 9 on-screen text templates mined from 182 OST entries. Key insight: 83% of top videos use just 1 OST at the hook position.

## Running for a product

```bash
# Generate content plan (requires Claude API for multi-pass generation)
python run_product.py --product "BoomBoom Nasal Stick" --category nasal_stick --creator Michelle

# Render guides from content.json
cd v2
python -c "
import json
from templates.shoot_guide_generator import generate_shoot_guide
from templates.edit_guide_generator import generate_edit_guide

with open('products/boomboom/content.json') as f:
    data = json.load(f)

generate_shoot_guide(data['shoot_guide'], '../output/shoot_guide.docx')
generate_edit_guide(data['edit_guide'], '../output/edit_guide.docx')
"
```

## Project structure

```
tiktok_engine/
  content_engine.py          # System 2 — multi-pass content generation
  analysis_pipeline.py       # System 1 — video analysis & data ingestion
  system_config.json         # Persona constraints, filming rules, product assumptions
  run_product.py             # CLI entry point
  v2/
    pipeline/
      db.py                  # Centralized Supabase client
      angle_scorer.py        # Improvement #1 — content angle rankings
      hook_templates.py      # Improvement #2 — hook template engine
      ost_patterns.py        # Improvement #3 — OST pattern mining
      validate.py            # Database validation
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
```
