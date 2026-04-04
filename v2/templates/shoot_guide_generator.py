"""
TikTok Content Engine — Shoot Guide Generator
==============================================
Takes a structured content dict and produces a .docx that matches
the approved shoot guide template exactly.

Usage:
    from templates.shoot_guide_generator import generate_shoot_guide
    generate_shoot_guide(content, output_path)

The content dict schema is defined in CONTENT_SCHEMA at the bottom
of this file. The generator validates the input before rendering.
"""

import os
from docx import Document

from templates.styles import (
    # Colors
    DARK, BLUE, GREEN, PURPLE, GRAY_TEXT, BODY_TEXT,
    # Sizes
    SIZE_TITLE, SIZE_SUBTITLE, SIZE_BODY_SMALL, SIZE_BODY_MEDIUM,
    SIZE_SECTION_HEADER, SIZE_SUB_HEADER, SIZE_BODY,
    # Font
    FONT_PRIMARY,
    # Builders
    add_centered_text, add_left_text, add_section_divider,
    add_shoot_th_header, add_shoot_script_line, add_shoot_broll_entry,
    add_shoot_vo_entry, add_shoot_bullet_note, add_blank,
    add_bottom_border, add_top_border, make_run, add_angle_callout,
)


def validate_content(content):
    """Validate content dict has all required fields.

    Raises ValueError with specific message if anything is missing.
    """
    required_top = ["title", "subtitle", "product_summary", "heroes", "broll", "voiceovers"]
    for key in required_top:
        if key not in content:
            raise ValueError(f"Missing required top-level key: '{key}'")

    if len(content["heroes"]) == 0:
        raise ValueError("'heroes' list cannot be empty")

    for i, hero in enumerate(content["heroes"]):
        for field in ["code", "product", "title", "notes", "lines"]:
            if field not in hero:
                raise ValueError(f"Hero {i+1} missing required field: '{field}'")
        for j, line in enumerate(hero["lines"]):
            for field in ["code", "tag", "text"]:
                if field not in line:
                    raise ValueError(f"Hero {i+1}, line {j+1} missing field: '{field}'")

    for i, shot in enumerate(content["broll"]):
        for field in ["code", "product", "description"]:
            if field not in shot:
                raise ValueError(f"B-roll {i+1} missing required field: '{field}'")

    for i, vo in enumerate(content["voiceovers"]):
        for field in ["code", "product", "script"]:
            if field not in vo:
                raise ValueError(f"Voiceover {i+1} missing required field: '{field}'")


def generate_shoot_guide(content, output_path):
    """Generate a shoot guide .docx from structured content.

    Args:
        content: dict matching CONTENT_SCHEMA
        output_path: where to save the .docx file

    Returns:
        output_path on success

    Raises:
        ValueError if content validation fails
    """
    validate_content(content)

    doc = Document()

    # Set default font
    style = doc.styles["Normal"]
    style.font.name = FONT_PRIMARY

    # ── TITLE BLOCK ──
    add_centered_text(doc, "SHOOT GUIDE", SIZE_TITLE, DARK, bold=True)
    add_centered_text(doc, content["subtitle"], SIZE_SUBTITLE, BLUE, bold=True)

    p = add_centered_text(doc, content["product_summary"], SIZE_BODY_MEDIUM, GRAY_TEXT)
    # Adjust: the approved template uses default (non-bold) for product summary

    p = add_centered_text(doc, content.get("tagline",
        f"{_count_videos(content)} videos from one shoot — here's everything you need to capture"),
        SIZE_BODY_SMALL, GRAY_TEXT)
    add_bottom_border(p)

    # ── ON-CAMERA LINES ──
    hero_count = len(content["heroes"])
    add_section_divider(doc, f"ON-CAMERA LINES ({hero_count} videos)", color=BLUE)

    p = doc.add_paragraph()
    make_run(p, "Each video is broken into short clips. Film each line as its own take — ",
             size=SIZE_BODY_SMALL, color=GRAY_TEXT)
    make_run(p, "don't try to do it all in one shot.", size=SIZE_BODY_SMALL,
             color=GRAY_TEXT, bold=True)
    make_run(p, " The editor will stitch them together.",
             size=SIZE_BODY_SMALL, color=GRAY_TEXT)

    for hero in content["heroes"]:
        add_shoot_th_header(doc, hero["code"], hero["product"], hero["title"])

        for note in hero["notes"]:
            add_shoot_bullet_note(doc, note)

        add_blank(doc)

        for line in hero["lines"]:
            is_direction = line.get("is_direction", False)
            add_shoot_script_line(doc, line["code"], line["tag"],
                                  line["text"], is_direction=is_direction)

        add_blank(doc)

    # ── B-ROLL SHOTS ──
    broll_count = len(content["broll"])
    add_section_divider(doc, f"B-ROLL SHOTS ({broll_count} clips)", color=GREEN)

    p = doc.add_paragraph()
    make_run(p, "Product shots, action shots, lifestyle moments. Shoot everything in natural light. "
             "Keep it simple — clean surface, steady hand.",
             size=SIZE_BODY_SMALL, color=GRAY_TEXT)

    add_blank(doc)

    for shot in content["broll"]:
        add_shoot_broll_entry(doc, shot["code"], shot["product"], shot["description"])

    # ── VOICEOVER AUDIO ──
    vo_count = len(content["voiceovers"])
    add_section_divider(doc, f"VOICEOVER AUDIO ({vo_count} clips)", color=PURPLE)

    p = doc.add_paragraph()
    make_run(p, "Record these audio-only. You can record in your car, bedroom, anywhere quiet. "
             "Natural voice — don't perform it.",
             size=SIZE_BODY_SMALL, color=GRAY_TEXT)

    add_blank(doc)

    for vo in content["voiceovers"]:
        add_shoot_vo_entry(doc, vo["code"], vo["product"], vo["script"])
        add_blank(doc)

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    doc.save(output_path)
    return output_path


def _count_videos(content):
    """Count total videos (heroes + implied remixes from VOs + no-voice)."""
    return len(content["heroes"]) + len(content.get("voiceovers", []))


# ═══════════════════════════════════════════════════════════════
# CONTENT SCHEMA
# ═══════════════════════════════════════════════════════════════
# This defines the expected structure of the content dict.
# The content engine (System 2) must produce this format.

CONTENT_SCHEMA = {
    "title": "SHOOT GUIDE",                    # Always "SHOOT GUIDE"
    "subtitle": "str — product or creator name",
    "product_summary": "str — products + counts (e.g. 'BoomBoom Nasal Stick — 5 Heroes, ...')",
    "tagline": "str — optional override for the 'X videos from one shoot' line",

    "heroes": [
        {
            "code": "str — TH1, TH2, etc.",
            "product": "str — product name (e.g. BOOMBOOM)",
            "title": "str — creative title (e.g. The First Reaction)",
            "notes": [
                "str — bullet point note for creator"
            ],
            "lines": [
                {
                    "code": "str — TH1-a, TH1-b, etc.",
                    "tag": "str — ON CAMERA or VOICEOVER",
                    "text": "str — the script line or direction",
                    "is_direction": "bool — True if this is a stage direction, not spoken"
                }
            ]
        }
    ],

    "broll": [
        {
            "code": "str — B1, B2, etc.",
            "product": "str — product name",
            "description": "str — what to shoot"
        }
    ],

    "voiceovers": [
        {
            "code": "str — VO1, VO2, etc.",
            "product": "str — product name",
            "script": "str — the full VO script text"
        }
    ]
}
