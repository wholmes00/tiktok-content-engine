"""
Content Validation Layer — TikTok Content Engine v2
====================================================
Validates content.json (the contract between Content Engine and Document Engine)
against template rules, database-driven constraints, and structural requirements.

This is the consistency gate: it ensures every creative decision follows the
data-driven rules before any documents are rendered.

Three validation tiers:
  1. STRUCTURAL — Required fields, correct types, counts match config
  2. TEMPLATE — Hook templates used, OST follows position rules, angles ranked
  3. QUALITY — Hook scores meet minimums, persona compliance, CTA placement

Usage:
    from v2.pipeline.validate_content import validate_content_plan
    result = validate_content_plan(content_plan)
    if not result["passed"]:
        for f in result["failures"]:
            print(f"  [{f['severity']}] {f['message']}")
"""

import re
import json


# ─── Template IDs ───────────────────────────────────────────────────
VALID_HOOK_TEMPLATES = {f"HT{i}" for i in range(1, 13)}
VALID_OST_TEMPLATES = {f"OST{i}" for i in range(1, 10)}
OST_POSITION_MAP = {
    "OST1": "hook", "OST2": "hook", "OST3": "hook", "OST4": "hook",
    "OST5": "hook", "OST6": "hook",
    "OST7": "narrative", "OST8": "narrative",
    "OST9": "cta",
}

# ─── Thresholds ─────────────────────────────────────────────────────
MIN_HOOK_TEMPLATE_COVERAGE = 0.6   # At least 60% of heroes should use a template
MAX_HERO_OST_CARDS = 4             # Data: 83% of top videos use 1-2 OST cards
MIN_HERO_OST_CARDS = 1             # Every hero needs at least a hook-position OST
MAX_BROLL_OST_CARDS = 7            # B-roll/no-voice can have more OST
MIN_BROLL_OST_CARDS = 3            # B-roll needs OST to carry narrative


def validate_content_plan(content_plan):
    """
    Validate a full content plan (shoot_guide + edit_guide).

    Args:
        content_plan: dict with 'shoot_guide' and 'edit_guide' keys

    Returns:
        dict with:
          'passed': bool (True if no critical failures)
          'failures': list of {'severity': 'critical'|'warning', 'section': str, 'message': str}
          'stats': dict with validation statistics
    """
    failures = []
    stats = {}

    # v3 compatibility: if the content plan has v3-format data, convert it
    # to the v2 format the validator expects
    shoot = content_plan.get("shoot_guide")
    edit = content_plan.get("edit_guide")

    if shoot and "on_camera" in shoot and "heroes" not in shoot:
        shoot = _adapt_v3_shoot_to_v2(shoot)
    if edit and "videos" in edit and "heroes" not in edit:
        edit = _adapt_v3_edit_to_v2(edit)

    # ═══════════════════════════════════════════════════════
    #  TIER 1: Structural Validation
    # ═══════════════════════════════════════════════════════

    # Shoot guide structure
    if not shoot:
        failures.append({
            "severity": "critical",
            "section": "shoot_guide",
            "message": "shoot_guide is missing or empty",
        })
    else:
        failures.extend(_validate_shoot_structure(shoot))
        stats["heroes"] = len(shoot.get("heroes", []))
        stats["broll"] = len(shoot.get("broll", []))
        stats["voiceovers"] = len(shoot.get("voiceovers", []))

    # Edit guide structure
    if not edit:
        failures.append({
            "severity": "critical",
            "section": "edit_guide",
            "message": "edit_guide is missing or empty",
        })
    else:
        failures.extend(_validate_edit_structure(edit))
        stats["edit_heroes"] = len(edit.get("heroes", []))
        stats["edit_remixes"] = len(edit.get("remixes", []))

    # ═══════════════════════════════════════════════════════
    #  TIER 2: Template Validation
    # ═══════════════════════════════════════════════════════

    if shoot:
        failures.extend(_validate_hook_templates(shoot))
    if edit:
        failures.extend(_validate_ost_rules(edit))
        failures.extend(_validate_angle_consistency(shoot, edit))

    # ═══════════════════════════════════════════════════════
    #  TIER 3: Quality Validation
    # ═══════════════════════════════════════════════════════

    if shoot:
        failures.extend(_validate_persona_compliance(shoot))
        failures.extend(_validate_cta_placement(edit))
        failures.extend(_validate_script_quality(shoot))

    # ═══════════════════════════════════════════════════════
    #  Results
    # ═══════════════════════════════════════════════════════

    critical_failures = [f for f in failures if f["severity"] == "critical"]
    stats["total_checks"] = len(failures) + _count_passes(shoot, edit)
    stats["failures"] = len(failures)
    stats["critical"] = len(critical_failures)
    stats["warnings"] = len(failures) - len(critical_failures)

    return {
        "passed": len(critical_failures) == 0,
        "failures": failures,
        "stats": stats,
    }


# ═══════════════════════════════════════════════════════════════════
#  TIER 1: Structural Validation
# ═══════════════════════════════════════════════════════════════════

def _validate_shoot_structure(shoot):
    """Check shoot guide has all required fields and correct types."""
    failures = []

    # Top-level required fields
    for field in ["title", "subtitle", "product_summary", "heroes", "broll", "voiceovers"]:
        if field not in shoot:
            failures.append({
                "severity": "critical",
                "section": "shoot_guide",
                "message": f"Missing required field: '{field}'",
            })

    heroes = shoot.get("heroes", [])
    if len(heroes) == 0:
        failures.append({
            "severity": "critical",
            "section": "shoot_guide.heroes",
            "message": "No hero videos defined",
        })

    for i, hero in enumerate(heroes):
        for field in ["code", "product", "title", "notes", "lines"]:
            if field not in hero:
                failures.append({
                    "severity": "critical",
                    "section": f"shoot_guide.heroes[{i}]",
                    "message": f"Hero {i+1} missing required field: '{field}'",
                })

        # Check lines have required fields
        for j, line in enumerate(hero.get("lines", [])):
            for field in ["code", "tag", "text"]:
                if field not in line:
                    failures.append({
                        "severity": "critical",
                        "section": f"shoot_guide.heroes[{i}].lines[{j}]",
                        "message": f"Hero {i+1}, line {j+1} missing: '{field}'",
                    })

    # B-roll checks
    for i, shot in enumerate(shoot.get("broll", [])):
        for field in ["code", "product", "description"]:
            if field not in shot:
                failures.append({
                    "severity": "critical",
                    "section": f"shoot_guide.broll[{i}]",
                    "message": f"B-roll {i+1} missing: '{field}'",
                })

    # VO checks
    for i, vo in enumerate(shoot.get("voiceovers", [])):
        for field in ["code", "product", "script"]:
            if field not in vo:
                failures.append({
                    "severity": "critical",
                    "section": f"shoot_guide.voiceovers[{i}]",
                    "message": f"Voiceover {i+1} missing: '{field}'",
                })

    return failures


def _validate_edit_structure(edit):
    """Check edit guide has all required fields."""
    failures = []

    for field in ["creator_name", "product_summary", "video_counts",
                   "analysis_count", "date", "heroes", "remixes", "upload_details"]:
        if field not in edit:
            failures.append({
                "severity": "critical",
                "section": "edit_guide",
                "message": f"Missing required field: '{field}'",
            })

    for i, hero in enumerate(edit.get("heroes", [])):
        for field in ["label", "title", "hook", "audio", "timeline", "onscreen_text"]:
            if field not in hero:
                failures.append({
                    "severity": "critical",
                    "section": f"edit_guide.heroes[{i}]",
                    "message": f"Hero {i+1} missing: '{field}'",
                })

    for i, remix in enumerate(edit.get("remixes", [])):
        for field in ["title", "info_line", "broll_assembly", "onscreen_text_script"]:
            if field not in remix:
                failures.append({
                    "severity": "critical",
                    "section": f"edit_guide.remixes[{i}]",
                    "message": f"Remix {i+1} missing: '{field}'",
                })

    ud = edit.get("upload_details", {})
    for field in ["hashtags", "captions", "schedule"]:
        if field not in ud:
            failures.append({
                "severity": "critical",
                "section": "edit_guide.upload_details",
                "message": f"Missing: '{field}'",
            })

    return failures


# ═══════════════════════════════════════════════════════════════════
#  TIER 2: Template Validation
# ═══════════════════════════════════════════════════════════════════

def _validate_hook_templates(shoot):
    """Check that hooks use data-driven templates at sufficient coverage."""
    failures = []
    heroes = shoot.get("heroes", [])
    if not heroes:
        return failures

    templated = 0
    for i, hero in enumerate(heroes):
        ht = hero.get("hook_template")
        if ht:
            if ht in VALID_HOOK_TEMPLATES:
                templated += 1
            else:
                failures.append({
                    "severity": "warning",
                    "section": f"shoot_guide.heroes[{i}]",
                    "message": f"Hero {i+1} uses unknown hook template: '{ht}'",
                })
        else:
            failures.append({
                "severity": "warning",
                "section": f"shoot_guide.heroes[{i}]",
                "message": f"Hero {i+1} has no hook_template tag — not template-driven",
            })

    coverage = templated / len(heroes) if heroes else 0
    if coverage < MIN_HOOK_TEMPLATE_COVERAGE:
        failures.append({
            "severity": "critical",
            "section": "shoot_guide.heroes",
            "message": f"Hook template coverage {coverage:.0%} below minimum {MIN_HOOK_TEMPLATE_COVERAGE:.0%} "
                       f"({templated}/{len(heroes)} heroes use templates)",
        })

    # Check VOs for templates too
    vos = shoot.get("voiceovers", [])
    vo_templated = sum(1 for vo in vos if vo.get("hook_template"))
    if vos and vo_templated == 0:
        failures.append({
            "severity": "warning",
            "section": "shoot_guide.voiceovers",
            "message": "No voiceovers use hook templates for opening lines",
        })

    return failures


def _validate_ost_rules(edit):
    """Check OST follows data-driven rules: heroes 1-4 cards, b-roll 3-7 cards."""
    failures = []

    for i, hero in enumerate(edit.get("heroes", [])):
        ost_entries = hero.get("onscreen_text", [])
        count = len(ost_entries)

        if count < MIN_HERO_OST_CARDS:
            failures.append({
                "severity": "critical",
                "section": f"edit_guide.heroes[{i}]",
                "message": f"Hero {i+1} has {count} OST cards (minimum {MIN_HERO_OST_CARDS})",
            })
        elif count > MAX_HERO_OST_CARDS:
            failures.append({
                "severity": "warning",
                "section": f"edit_guide.heroes[{i}]",
                "message": f"Hero {i+1} has {count} OST cards (data shows 83% of top videos use 1-2, max {MAX_HERO_OST_CARDS})",
            })

        # Check OST template validity
        for j, ost in enumerate(ost_entries):
            template = ost.get("ost_template")
            if template and template not in VALID_OST_TEMPLATES and template != "ORIGINAL":
                failures.append({
                    "severity": "warning",
                    "section": f"edit_guide.heroes[{i}].onscreen_text[{j}]",
                    "message": f"Unknown OST template: '{template}'",
                })

    for i, remix in enumerate(edit.get("remixes", [])):
        ost_entries = remix.get("onscreen_text_script", [])
        count = len(ost_entries)

        if count < MIN_BROLL_OST_CARDS:
            failures.append({
                "severity": "warning",
                "section": f"edit_guide.remixes[{i}]",
                "message": f"Remix {i+1} has {count} OST cards (minimum {MIN_BROLL_OST_CARDS} for b-roll)",
            })
        elif count > MAX_BROLL_OST_CARDS:
            failures.append({
                "severity": "warning",
                "section": f"edit_guide.remixes[{i}]",
                "message": f"Remix {i+1} has {count} OST cards (maximum {MAX_BROLL_OST_CARDS})",
            })

    return failures


def _validate_angle_consistency(shoot, edit):
    """Check that content angles in shoot guide match edit guide."""
    failures = []
    if not shoot or not edit:
        return failures

    shoot_angles = []
    for hero in shoot.get("heroes", []):
        angle = hero.get("content_angle")
        if angle:
            shoot_angles.append(angle)

    edit_angles = []
    for hero in edit.get("heroes", []):
        angle = hero.get("content_angle")
        if angle:
            edit_angles.append(angle)

    # If both have angles, check they match
    if shoot_angles and edit_angles:
        for i, (sa, ea) in enumerate(zip(shoot_angles, edit_angles)):
            if sa != ea:
                failures.append({
                    "severity": "warning",
                    "section": f"heroes[{i}]",
                    "message": f"Hero {i+1} angle mismatch: shoot='{sa}' vs edit='{ea}'",
                })

    # Check angles have evidence
    for i, hero in enumerate(shoot.get("heroes", [])):
        if hero.get("content_angle") and not hero.get("angle_evidence"):
            failures.append({
                "severity": "warning",
                "section": f"shoot_guide.heroes[{i}]",
                "message": f"Hero {i+1} has content_angle but no angle_evidence (not data-backed)",
            })

    return failures


# ═══════════════════════════════════════════════════════════════════
#  TIER 3: Quality Validation
# ═══════════════════════════════════════════════════════════════════

def _validate_persona_compliance(shoot):
    """Check scripts don't violate persona constraints."""
    failures = []

    # Load forbidden vocabulary
    forbidden_terms = [
        "la mer", "sk-ii", "tatcha", "drunk elephant", "sunday riley",
        "adenosine", "glutathione", "niacinamide", "retinol", "hyaluronic acid",
        "k-beauty", "actives", "barrier repair", "peptides",
        "clinically proven", "dermatologist-tested", "clinical trials",
    ]

    # Location violations (persona is home_only)
    location_violations = [
        "gym", "grocery store", "target", "walmart", "office",
        "starbucks", "restaurant", "salon",
    ]

    # Solo violations (persona is solo_only)
    solo_violations = [
        "my friend handed", "hand it to", "my husband", "my boyfriend",
        "my coworker", "my sister", "reaction from",
    ]

    all_text = []
    for hero in shoot.get("heroes", []):
        for line in hero.get("lines", []):
            all_text.append(line.get("text", ""))
    for vo in shoot.get("voiceovers", []):
        all_text.append(vo.get("script", ""))

    combined = " ".join(all_text).lower()

    for term in forbidden_terms:
        if term in combined:
            failures.append({
                "severity": "critical",
                "section": "persona_compliance",
                "message": f"Forbidden vocabulary detected: '{term}' — violates vocabulary ceiling",
            })

    for term in location_violations:
        if term in combined:
            failures.append({
                "severity": "warning",
                "section": "persona_compliance",
                "message": f"Possible location violation: '{term}' — persona is home_only",
            })

    for term in solo_violations:
        if term in combined:
            failures.append({
                "severity": "warning",
                "section": "persona_compliance",
                "message": f"Possible solo violation: '{term}' — persona is solo_only",
            })

    return failures


def _validate_cta_placement(edit):
    """Check every video has a CTA in the final third."""
    failures = []
    if not edit:
        return failures

    for i, hero in enumerate(edit.get("heroes", [])):
        timeline = hero.get("timeline", [])
        if not timeline:
            continue

        # Check last 2 timeline entries for CTA-like language
        last_entries = timeline[-2:] if len(timeline) >= 2 else timeline
        has_cta = False
        for entry in last_entries:
            content = entry.get("content", "").lower()
            if any(cta in content for cta in ["link", "shop", "go", "get it", "i'll link", "linked"]):
                has_cta = True
                break

        # Also check OST for CTA
        ost = hero.get("onscreen_text", [])
        for entry in ost:
            text = entry.get("text", "").lower()
            if any(cta in text for cta in ["link below", "shop", "get it"]):
                has_cta = True
                break

        if not has_cta:
            failures.append({
                "severity": "warning",
                "section": f"edit_guide.heroes[{i}]",
                "message": f"Hero {i+1} may be missing a CTA in the final section",
            })

    for i, remix in enumerate(edit.get("remixes", [])):
        ost = remix.get("onscreen_text_script", [])
        has_cta = False
        if ost:
            last_ost = ost[-1].lower() if isinstance(ost[-1], str) else ""
            if any(cta in last_ost for cta in ["link", "shop", "get it"]):
                has_cta = True

        if not has_cta:
            failures.append({
                "severity": "warning",
                "section": f"edit_guide.remixes[{i}]",
                "message": f"Remix {i+1} may be missing a closing CTA",
            })

    return failures


def _validate_script_quality(shoot):
    """Check scripts meet minimum quality thresholds."""
    failures = []

    for i, hero in enumerate(shoot.get("heroes", [])):
        lines = hero.get("lines", [])
        spoken_lines = [l for l in lines if not l.get("is_direction", False)]

        # Minimum 3 spoken lines per hero
        if len(spoken_lines) < 3:
            failures.append({
                "severity": "warning",
                "section": f"shoot_guide.heroes[{i}]",
                "message": f"Hero {i+1} has only {len(spoken_lines)} spoken lines (minimum 3)",
            })

        # Check hook line length (first spoken line should be punchy)
        if spoken_lines:
            hook_text = spoken_lines[0].get("text", "").strip('"').strip('"').strip('"')
            hook_words = len(hook_text.split())
            if hook_words > 15:
                failures.append({
                    "severity": "warning",
                    "section": f"shoot_guide.heroes[{i}]",
                    "message": f"Hero {i+1} hook is {hook_words} words (data: top hooks are ≤12 words)",
                })

    # Check VO scripts have reasonable length
    for i, vo in enumerate(shoot.get("voiceovers", [])):
        script = vo.get("script", "")
        word_count = len(script.split())
        if word_count < 20:
            failures.append({
                "severity": "warning",
                "section": f"shoot_guide.voiceovers[{i}]",
                "message": f"VO{i+1} script is only {word_count} words (seems too short)",
            })
        elif word_count > 100:
            failures.append({
                "severity": "warning",
                "section": f"shoot_guide.voiceovers[{i}]",
                "message": f"VO{i+1} script is {word_count} words (may be too long for TikTok)",
            })

    return failures


def _count_passes(shoot, edit):
    """Count implicit passes (checks that didn't fail) for stats."""
    count = 0
    if shoot:
        count += len(shoot.get("heroes", [])) * 5  # ~5 checks per hero
        count += len(shoot.get("broll", [])) * 2
        count += len(shoot.get("voiceovers", [])) * 3
    if edit:
        count += len(edit.get("heroes", [])) * 6
        count += len(edit.get("remixes", [])) * 4
        count += 3  # upload_details
    return count


def print_content_validation_report(result):
    """Print a formatted validation report."""
    stats = result["stats"]
    failures = result["failures"]

    print(f"  Content Validation: {'✓ PASSED' if result['passed'] else '✗ ISSUES FOUND'}")
    print(f"  Heroes: {stats.get('heroes', '?')} shoot / {stats.get('edit_heroes', '?')} edit")
    print(f"  B-roll: {stats.get('broll', '?')} | VOs: {stats.get('voiceovers', '?')} | Remixes: {stats.get('edit_remixes', '?')}")
    print(f"  Failures: {stats.get('critical', 0)} critical, {stats.get('warnings', 0)} warnings")

    if failures:
        print()
        for f in failures:
            icon = "🔴" if f["severity"] == "critical" else "🟡"
            print(f"  {icon} [{f['section']}] {f['message']}")


# ═══════════════════════════════════════════════════════════════════
#  v3 → v2 ADAPTER (for backward-compatible validation)
# ═══════════════════════════════════════════════════════════════════

def _adapt_v3_shoot_to_v2(v3_shoot):
    """Convert v3 shoot guide to v2 format for the validator."""
    product_tag = v3_shoot.get("product_name", v3_shoot.get("subtitle", "")).split()[0].upper()

    heroes = []
    for group in v3_shoot.get("on_camera", []):
        label = group.get("video_label", "")
        lines = []
        for item in group.get("lines", []):
            if isinstance(item, dict):
                lines.append({
                    "code": "",
                    "tag": item.get("tag", "ON CAMERA"),
                    "text": item.get("text", ""),
                    "is_direction": False,
                })
            else:
                lines.append({
                    "code": "",
                    "tag": "ON CAMERA",
                    "text": str(item),
                    "is_direction": False,
                })
        heroes.append({
            "code": f"TH{len(heroes)+1}",
            "product": product_tag,
            "title": label,
            "notes": [],
            "lines": lines,
            "hook_template": group.get("hook_template", ""),
        })

    broll = []
    for shot in v3_shoot.get("broll", []):
        broll.append({
            "code": shot.get("code", ""),
            "product": product_tag,
            "description": shot.get("description", ""),
        })

    voiceovers = []
    for vo in v3_shoot.get("voiceovers", []):
        voiceovers.append({
            "code": f"VO{len(voiceovers)+1}",
            "product": product_tag,
            "script": vo.get("script", ""),
        })

    return {
        "title": "SHOOT GUIDE",
        "subtitle": v3_shoot.get("subtitle", ""),
        "product_summary": v3_shoot.get("product_name", ""),
        "heroes": heroes,
        "broll": broll,
        "voiceovers": voiceovers,
    }


def _adapt_v3_edit_to_v2(v3_edit):
    """Convert v3 edit guide to v2 format for the validator."""
    videos = v3_edit.get("videos", [])
    hero_videos = [v for v in videos if v.get("type") == "hero"]
    remix_videos = [v for v in videos if v.get("type") == "remix"]

    heroes = []
    for i, hv in enumerate(hero_videos):
        # Build timeline from script lines
        timeline = []
        for j, line in enumerate(hv.get("script", [])):
            tag = "ON CAMERA" if "camera" in line.get("type", "").lower() else "VOICEOVER"
            timeline.append({
                "timestamp": "",
                "shot_ref": f"TH{i+1}",
                "content": f'{tag}: "{line.get("text", "")}"',
            })

        # Build OST
        onscreen_text = []
        for entry in hv.get("on_screen_text", []):
            if isinstance(entry, str):
                onscreen_text.append({"timestamp": "", "text": entry})
            elif isinstance(entry, dict):
                onscreen_text.append(entry)

        heroes.append({
            "label": f"HERO VIDEO {i+1}",
            "title": hv.get("title", ""),
            "hook": hv.get("hook", ""),
            "hook_template": hv.get("hook_template", ""),
            "content_angle": hv.get("content_angle", ""),
            "audio": hv.get("audio", ""),
            "timeline": timeline,
            "onscreen_text": onscreen_text,
        })

    remixes = []
    for i, rv in enumerate(remix_videos):
        vo_lines = [l.get("text", "") for l in rv.get("script", []) if "voice" in l.get("type", "").lower()]
        vo_script = " ".join(vo_lines) if vo_lines else None

        ost = rv.get("on_screen_text", [])

        remixes.append({
            "title": rv.get("title", ""),
            "info_line": "",
            "broll_assembly": [f"{b['code']} — {b['description']}" for b in rv.get("broll_used", [])],
            "onscreen_text_script": ost,
            "voiceover_script": vo_script,
        })

    return {
        "creator_name": v3_edit.get("creator_name", ""),
        "product_summary": v3_edit.get("product_name", ""),
        "video_counts": v3_edit.get("video_count", ""),
        "analysis_count": v3_edit.get("analysis_count", 0),
        "date": v3_edit.get("date", ""),
        "heroes": heroes,
        "remixes": remixes,
        "upload_details": {"hashtags": [], "captions": [], "schedule": []},
    }
