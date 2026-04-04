"""
Quality Scoring System — TikTok Content Engine v2
==================================================
Scores generated content on consistency dimensions that determine
whether the output meets production-ready standards.

Scoring dimensions:
  1. Hook Strength (0-25): Word count, template match, incompleteness
  2. Persona Compliance (0-25): Vocabulary, location, solo constraints
  3. Structure Adherence (0-25): Template coverage, OST rules, angle evidence
  4. CTA + Engagement (0-25): CTA placement, emotional hooks, specificity

Total score: 0-100
  90-100 = Production ready
  75-89  = Good, minor tweaks recommended
  60-74  = Needs revision
  <60    = Reject — regenerate

Usage:
    from v2.pipeline.quality_scorer import score_content_plan
    result = score_content_plan(content_plan)
    print(f"Score: {result['total']}/100 — {result['grade']}")
"""


def score_content_plan(content_plan):
    """
    Score a complete content plan on quality dimensions.

    Args:
        content_plan: dict with 'shoot_guide' and 'edit_guide' keys

    Returns:
        dict with:
          'total': int (0-100)
          'grade': str ('A', 'B', 'C', 'F')
          'production_ready': bool
          'dimensions': dict of individual dimension scores
          'details': list of scoring notes
    """
    shoot = content_plan.get("shoot_guide", {})
    edit = content_plan.get("edit_guide", {})

    details = []

    hook_score = _score_hook_strength(shoot, details)
    persona_score = _score_persona_compliance(shoot, details)
    structure_score = _score_structure_adherence(shoot, edit, details)
    cta_score = _score_cta_engagement(shoot, edit, details)

    total = hook_score + persona_score + structure_score + cta_score

    if total >= 90:
        grade = "A"
    elif total >= 75:
        grade = "B"
    elif total >= 60:
        grade = "C"
    else:
        grade = "F"

    return {
        "total": total,
        "grade": grade,
        "production_ready": total >= 75,
        "dimensions": {
            "hook_strength": {"score": hook_score, "max": 25},
            "persona_compliance": {"score": persona_score, "max": 25},
            "structure_adherence": {"score": structure_score, "max": 25},
            "cta_engagement": {"score": cta_score, "max": 25},
        },
        "details": details,
    }


def _score_hook_strength(shoot, details):
    """Score hooks on word count, template usage, and punchy-ness. Max 25."""
    score = 25
    heroes = shoot.get("heroes", [])
    if not heroes:
        details.append("Hook: No heroes found (-25)")
        return 0

    for i, hero in enumerate(heroes):
        lines = hero.get("lines", [])
        spoken = [l for l in lines if not l.get("is_direction", False)]
        if not spoken:
            score -= 5
            details.append(f"Hook: Hero {i+1} has no spoken lines (-5)")
            continue

        hook_text = spoken[0].get("text", "").strip('"').strip('"').strip('"')
        words = len(hook_text.split())

        # Optimal: 4-12 words
        if words <= 3:
            score -= 2
            details.append(f"Hook: Hero {i+1} hook too short ({words} words) (-2)")
        elif words > 15:
            score -= 3
            details.append(f"Hook: Hero {i+1} hook too long ({words} words) (-3)")
        elif words > 12:
            score -= 1
            details.append(f"Hook: Hero {i+1} hook slightly long ({words} words) (-1)")

        # Template match bonus (already starts at 25, so we penalize missing)
        if not hero.get("hook_template"):
            score -= 1
            details.append(f"Hook: Hero {i+1} not template-tagged (-1)")

    return max(0, min(25, score))


def _score_persona_compliance(shoot, details):
    """Score persona constraint adherence. Max 25."""
    score = 25

    forbidden = [
        "la mer", "sk-ii", "tatcha", "drunk elephant", "sunday riley",
        "adenosine", "glutathione", "niacinamide", "retinol", "hyaluronic acid",
        "k-beauty", "actives", "barrier repair", "peptides",
        "clinically proven", "dermatologist-tested", "clinical trials",
    ]
    location_bad = ["gym", "grocery store", "target", "walmart", "office", "salon"]
    solo_bad = ["my friend handed", "hand it to someone", "my husband tried",
                "my boyfriend", "reaction from"]

    all_text = []
    for hero in shoot.get("heroes", []):
        for line in hero.get("lines", []):
            all_text.append(line.get("text", ""))
    for vo in shoot.get("voiceovers", []):
        all_text.append(vo.get("script", ""))

    combined = " ".join(all_text).lower()

    for term in forbidden:
        if term in combined:
            score -= 5
            details.append(f"Persona: Forbidden term '{term}' (-5)")

    for term in location_bad:
        if term in combined:
            score -= 3
            details.append(f"Persona: Location violation '{term}' (-3)")

    for term in solo_bad:
        if term in combined:
            score -= 3
            details.append(f"Persona: Solo violation '{term}' (-3)")

    return max(0, min(25, score))


def _score_structure_adherence(shoot, edit, details):
    """Score structural rules: templates, OST, angles. Max 25."""
    score = 25
    heroes = shoot.get("heroes", [])

    # Hook template coverage
    if heroes:
        templated = sum(1 for h in heroes if h.get("hook_template"))
        coverage = templated / len(heroes)
        if coverage < 0.6:
            score -= 8
            details.append(f"Structure: Hook template coverage {coverage:.0%} < 60% (-8)")
        elif coverage < 0.8:
            score -= 3
            details.append(f"Structure: Hook template coverage {coverage:.0%} < 80% (-3)")

    # Angle evidence
    if heroes:
        has_evidence = sum(1 for h in heroes if h.get("angle_evidence"))
        if has_evidence == 0:
            score -= 5
            details.append("Structure: No heroes have angle_evidence (-5)")
        elif has_evidence < len(heroes):
            score -= 2
            details.append(f"Structure: Only {has_evidence}/{len(heroes)} heroes have angle_evidence (-2)")

    # Edit guide OST rules
    for i, hero in enumerate(edit.get("heroes", [])):
        ost_count = len(hero.get("onscreen_text", []))
        if ost_count > 4:
            score -= 2
            details.append(f"Structure: Hero {i+1} has {ost_count} OST cards > 4 (-2)")
        elif ost_count == 0:
            score -= 3
            details.append(f"Structure: Hero {i+1} has no OST (-3)")

    # Remix OST
    for i, remix in enumerate(edit.get("remixes", [])):
        ost_count = len(remix.get("onscreen_text_script", []))
        if ost_count < 3:
            score -= 1
            details.append(f"Structure: Remix {i+1} has only {ost_count} OST cards (-1)")

    return max(0, min(25, score))


def _score_cta_engagement(shoot, edit, details):
    """Score CTA placement and engagement hooks. Max 25."""
    score = 25

    # Check heroes for CTAs
    for i, hero in enumerate(edit.get("heroes", [])):
        timeline = hero.get("timeline", [])
        ost = hero.get("onscreen_text", [])

        has_cta = False
        # Check last 2 timeline entries
        for entry in timeline[-2:]:
            content = entry.get("content", "").lower()
            if any(w in content for w in ["link", "shop", "go", "get it", "i'll link"]):
                has_cta = True
                break
        # Check OST
        for entry in ost:
            if any(w in entry.get("text", "").lower() for w in ["link below", "shop"]):
                has_cta = True
                break

        if not has_cta:
            score -= 3
            details.append(f"CTA: Hero {i+1} missing clear CTA (-3)")

    # Check remixes for CTAs
    for i, remix in enumerate(edit.get("remixes", [])):
        ost = remix.get("onscreen_text_script", [])
        if ost:
            last = ost[-1].lower() if isinstance(ost[-1], str) else ""
            if not any(w in last for w in ["link", "shop", "get it"]):
                score -= 2
                details.append(f"CTA: Remix {i+1} last OST card isn't a CTA (-2)")

    # Check upload_details completeness
    ud = edit.get("upload_details", {})
    if not ud.get("hashtags"):
        score -= 2
        details.append("CTA: No hashtags in upload_details (-2)")
    if not ud.get("captions"):
        score -= 2
        details.append("CTA: No captions in upload_details (-2)")
    if not ud.get("schedule"):
        score -= 2
        details.append("CTA: No posting schedule (-2)")

    return max(0, min(25, score))


def print_quality_report(result):
    """Print a formatted quality scoring report."""
    print(f"\n{'=' * 50}")
    print(f"  QUALITY SCORE: {result['total']}/100 — Grade {result['grade']}")
    print(f"  {'✓ PRODUCTION READY' if result['production_ready'] else '✗ NEEDS REVISION'}")
    print(f"{'=' * 50}")

    for name, dim in result["dimensions"].items():
        bar_len = int(dim["score"] / dim["max"] * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        print(f"  {name:<22} [{bar}] {dim['score']}/{dim['max']}")

    if result["details"]:
        print(f"\n  Scoring notes:")
        for note in result["details"]:
            print(f"    • {note}")

    print(f"{'=' * 50}")
