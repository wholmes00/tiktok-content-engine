"""
TikTok Content Engine — Edit Guide Generator
=============================================
Takes a structured content dict and produces a .docx that matches
the approved edit guide template exactly.

Usage:
    from templates.edit_guide_generator import generate_edit_guide
    generate_edit_guide(content, output_path)

The content dict schema is defined in CONTENT_SCHEMA at the bottom
of this file. The generator validates the input before rendering.
"""

import os
from docx import Document

try:
    from templates.styles import (
        DARK, BLUE, GREEN, PURPLE, GRAY_TEXT, BODY_TEXT, RED,
        SIZE_EDIT_TITLE, SIZE_SUBTITLE, SIZE_SECTION_HEADER, SIZE_SUB_HEADER,
        SIZE_BODY_SMALL, SIZE_BODY_MEDIUM, SIZE_BODY, SIZE_EDIT_SECTION,
        SIZE_EDIT_HERO_TITLE,
        FONT_PRIMARY,
        add_centered_text, add_sub_header, add_blank, add_bullet_item,
        add_top_border, make_run, add_angle_callout,
        add_edit_hero_label, add_edit_hero_title, add_edit_hook_line,
        add_edit_audio_line, add_edit_timeline_entry, add_edit_timeline_content,
        add_edit_ost_entry, add_edit_remix_title, add_edit_remix_info,
        add_edit_body_text,
    )
except ImportError:
    from v2.templates.styles import (
        DARK, BLUE, GREEN, PURPLE, GRAY_TEXT, BODY_TEXT, RED,
        SIZE_EDIT_TITLE, SIZE_SUBTITLE, SIZE_SECTION_HEADER, SIZE_SUB_HEADER,
        SIZE_BODY_SMALL, SIZE_BODY_MEDIUM, SIZE_BODY, SIZE_EDIT_SECTION,
        SIZE_EDIT_HERO_TITLE,
        FONT_PRIMARY,
        add_centered_text, add_sub_header, add_blank, add_bullet_item,
        add_top_border, make_run, add_angle_callout,
        add_edit_hero_label, add_edit_hero_title, add_edit_hook_line,
        add_edit_audio_line, add_edit_timeline_entry, add_edit_timeline_content,
        add_edit_ost_entry, add_edit_remix_title, add_edit_remix_info,
        add_edit_body_text,
    )


def validate_content(content):
    """Validate content dict has all required fields."""
    required_top = ["creator_name", "product_summary", "video_counts",
                    "analysis_count", "date", "heroes", "remixes", "upload_details"]
    for key in required_top:
        if key not in content:
            raise ValueError(f"Missing required top-level key: '{key}'")

    for i, hero in enumerate(content["heroes"]):
        for field in ["label", "title", "hook", "audio", "timeline", "onscreen_text"]:
            if field not in hero:
                raise ValueError(f"Hero {i+1} missing field: '{field}'")
        for j, entry in enumerate(hero["timeline"]):
            for field in ["timestamp", "shot_ref", "content"]:
                if field not in entry:
                    raise ValueError(f"Hero {i+1}, timeline {j+1} missing: '{field}'")
        for j, entry in enumerate(hero["onscreen_text"]):
            for field in ["timestamp", "text"]:
                if field not in entry:
                    raise ValueError(f"Hero {i+1}, OST {j+1} missing: '{field}'")

    for i, remix in enumerate(content["remixes"]):
        for field in ["title", "info_line", "broll_assembly", "onscreen_text_script"]:
            if field not in remix:
                raise ValueError(f"Remix {i+1} missing field: '{field}'")

    ud = content["upload_details"]
    for field in ["hashtags", "captions", "schedule"]:
        if field not in ud:
            raise ValueError(f"upload_details missing field: '{field}'")


def generate_edit_guide(content, output_path):
    """Generate an edit guide .docx from structured content.

    Args:
        content: dict matching CONTENT_SCHEMA
        output_path: where to save the .docx file

    Returns:
        output_path on success
    """
    validate_content(content)

    doc = Document()
    style = doc.styles["Normal"]
    style.font.name = FONT_PRIMARY

    # ── TITLE BLOCK ──
    add_centered_text(doc, content["creator_name"], SIZE_EDIT_TITLE, DARK, bold=True)
    add_centered_text(doc, content["product_summary"], SIZE_SUBTITLE, GRAY_TEXT)

    p = add_centered_text(doc, "TikTok Affiliate Edit Guide",
                           SIZE_SECTION_HEADER, BLUE, bold=True)
    add_top_border(p)

    add_centered_text(doc, content["video_counts"], SIZE_BODY_MEDIUM, GRAY_TEXT)
    add_centered_text(doc,
        f"Based on analysis of {content['analysis_count']} top-performing TikTok affiliate videos",
        SIZE_BODY_SMALL, GRAY_TEXT)
    add_centered_text(doc, content["date"], SIZE_BODY_SMALL, GRAY_TEXT)

    add_blank(doc)

    # ── SECTION A: HERO VIDEOS ──
    add_centered_text(doc, "SECTION A", SIZE_BODY_SMALL, BLUE, bold=True)
    add_centered_text(doc, "Hero Videos", SIZE_EDIT_SECTION, DARK, bold=True)

    for hero in content["heroes"]:
        _render_hero(doc, hero)

    # ── SECTION B: REMIX VIDEOS ──
    add_centered_text(doc, "SECTION B", SIZE_BODY_SMALL, BLUE, bold=True)
    add_centered_text(doc, "B-Roll Remix Videos", SIZE_EDIT_SECTION, DARK, bold=True)
    add_centered_text(doc, "Short, snappy edits \u2014 music-driven, minimal talking head",
                       SIZE_BODY_MEDIUM, GRAY_TEXT)

    for remix in content["remixes"]:
        _render_remix(doc, remix)

    # ── UPLOAD DETAILS ──
    add_centered_text(doc, "UPLOAD DETAILS", SIZE_BODY_SMALL, BLUE, bold=True)

    ud = content["upload_details"]

    add_sub_header(doc, "Suggested Hashtags")
    for tag in ud["hashtags"]:
        add_bullet_item(doc, tag)

    add_sub_header(doc, "Suggested Captions/Titles")
    for cap in ud["captions"]:
        add_bullet_item(doc, cap)

    add_sub_header(doc, "Posting Schedule")
    for item in ud["schedule"]:
        add_bullet_item(doc, item)

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    doc.save(output_path)
    return output_path


def _render_hero(doc, hero):
    """Render a single hero video section."""
    add_edit_hero_label(doc, hero["label"])
    add_edit_hero_title(doc, hero["title"])
    add_edit_hook_line(doc, hero["hook"])
    add_edit_audio_line(doc, hero["audio"])

    # Timeline
    add_sub_header(doc, "TIMELINE")
    for entry in hero["timeline"]:
        add_edit_timeline_entry(doc, entry["timestamp"], entry["shot_ref"])
        add_edit_timeline_content(doc, entry["content"])

    # On-screen text
    add_sub_header(doc, "ON-SCREEN TEXT")
    for entry in hero["onscreen_text"]:
        add_edit_ost_entry(doc, entry["timestamp"], entry["text"])

    add_blank(doc)


def _render_remix(doc, remix):
    """Render a single remix video section."""
    add_edit_remix_title(doc, remix["title"])
    add_edit_remix_info(doc, remix["info_line"])

    # B-roll assembly
    add_sub_header(doc, "B-ROLL ASSEMBLY")
    for shot in remix["broll_assembly"]:
        add_bullet_item(doc, shot)

    # On-screen text script
    add_sub_header(doc, "ON-SCREEN TEXT SCRIPT")
    for item in remix["onscreen_text_script"]:
        add_bullet_item(doc, item)

    # Voiceover script (only for VO remixes)
    if remix.get("voiceover_script"):
        add_sub_header(doc, "VOICEOVER SCRIPT")
        add_edit_body_text(doc, remix["voiceover_script"])

    add_blank(doc)


# ═══════════════════════════════════════════════════════════════
# CONTENT SCHEMA
# ═══════════════════════════════════════════════════════════════

CONTENT_SCHEMA = {
    "creator_name": "str — e.g. MICHELLE'S BOOMBOOM SHOOT",
    "product_summary": "str — e.g. BoomBoom Nasal Stick — 5 Heroes, 3 VO Remixes, 2 No-Voice Remixes",
    "video_counts": "str — e.g. 5 Hero Videos + 5 Remix Videos — Production Ready",
    "analysis_count": "int — number of videos in database (e.g. 122)",
    "date": "str — e.g. April 2026",

    "heroes": [
        {
            "label": "str — e.g. HERO VIDEO 1",
            "title": "str — e.g. Hero 1 — The First Reaction",
            "hook": "str — the hook line text (without quotes)",
            "audio": "str — e.g. Original sound (no music)",
            "timeline": [
                {
                    "timestamp": "str — e.g. [0:00-0:03]",
                    "shot_ref": "str — e.g. TH1 CLOSE-UP",
                    "content": "str — e.g. ON CAMERA: \"script text\""
                }
            ],
            "onscreen_text": [
                {
                    "timestamp": "str — e.g. [0:00-0:03]",
                    "text": "str — the overlay text"
                }
            ]
        }
    ],

    "remixes": [
        {
            "title": "str — e.g. Remix 1 — Essential Oil Wake-Up",
            "info_line": "str — e.g. BOOMBOOM \u2022 VOICEOVER",
            "broll_assembly": ["str — e.g. B2 — Uncapping the stick (slow, satisfying)"],
            "onscreen_text_script": ["str — e.g. [0:00-0:03] one breath and everything clears"],
            "voiceover_script": "str or None — full VO script text (None for no-voice remixes)"
        }
    ],

    "upload_details": {
        "hashtags": ["str"],
        "captions": ["str"],
        "schedule": ["str"]
    }
}
