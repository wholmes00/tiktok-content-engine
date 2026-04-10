"""
Build SQL INSERT statements from visual_analysis.json for Supabase child tables.
Outputs separate SQL files for each table.
"""
import json
from pathlib import Path

data = json.loads((Path(__file__).parent / "visual_analysis.json").read_text())

def esc(s):
    """Escape single quotes for SQL."""
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"

def json_esc(obj):
    """Escape a JSON object for SQL."""
    return esc(json.dumps(obj))

# Duration mapping for timestamp estimation
DURATIONS = {
    "7602822534198775053": 119.3,
    "7603942722579320077": 15.8,
    "7615445522677026079": 55.6,
    "7617704190168567053": 72.2,
    "7619817856556993806": 54.3,
    "7625051711895424270": 180.8,
}

def position_to_seconds(pos, duration):
    """Convert beginning/middle/end to approximate seconds."""
    pos = str(pos).lower()
    if pos == "beginning":
        return 0
    elif pos == "middle":
        return round(duration / 2)
    elif pos == "end":
        return round(duration * 0.85)
    else:
        try:
            return float(pos)
        except:
            return 0

# Build all SQL
shot_rows = []
ost_rows = []
cta_rows = []
vn_rows = []
vs_rows = []

for vid, analysis in data.items():
    uuid = analysis["_uuid"]
    dur = DURATIONS.get(vid, 60)
    sb = analysis.get("shot_breakdown", {})

    # shot_breakdown
    shot_counts_json = json.dumps(sb.get("shot_counts", {}))
    shot_seq = sb.get("shot_sequence", [])
    shot_seq_sql = "ARRAY[" + ",".join(esc(s) for s in shot_seq) + "]" if shot_seq else "NULL"

    shot_rows.append(
        f"('{uuid}', {sb.get('total_frames', 0)}, {sb.get('face_frames', 0)}, "
        f"{sb.get('broll_frames', 0)}, '{shot_counts_json}'::jsonb, "
        f"{esc(sb.get('dominant_broll_type'))}, {esc(sb.get('secondary_broll_type'))}, "
        f"{shot_seq_sql}, {esc(sb.get('analysis_confidence'))}, {esc(sb.get('notes'))})"
    )

    # onscreen_text
    for ost in analysis.get("onscreen_text", []):
        ts = position_to_seconds(ost.get("timestamp_position", "beginning"), dur)
        ost_rows.append(
            f"('{uuid}', {ts}, {esc(ost.get('text_content', ''))}, "
            f"{esc(ost.get('text_type', 'subtitle'))}, {str(ost.get('is_persistent', False)).lower()})"
        )

    # ctas
    for cta in analysis.get("ctas", []):
        ts = position_to_seconds(cta.get("timestamp_position", cta.get("cta_position", "end")), dur)
        cta_rows.append(
            f"('{uuid}', {ts}, {esc(cta.get('cta_text', ''))}, "
            f"{esc(cta.get('cta_type', 'verbal'))}, {esc(cta.get('cta_position', 'end'))})"
        )

    # visual_notes
    vn = analysis.get("visual_notes", {})
    scenes = json.dumps(vn.get("scene_descriptions", []))
    vn_rows.append(
        f"('{uuid}', {esc(vn.get('format_type'))}, {esc(vn.get('hook_style'))}, "
        f"'{scenes.replace(chr(39), chr(39)+chr(39))}'::jsonb, "
        f"{vn.get('product_reveal_timestamp') if vn.get('product_reveal_timestamp') is not None else 'NULL'}, "
        f"{esc(vn.get('overall_notes'))}, {esc(vn.get('cross_reference_notes'))})"
    )

    # visual_scripts
    vs_text = analysis.get("visual_script", "")
    vs_rows.append(f"('{uuid}', {esc(vs_text)}, true)")


# Output SQL
output = []

if shot_rows:
    output.append("-- video_shot_breakdown")
    output.append("INSERT INTO video_shot_breakdown (video_id, total_frames_analyzed, face_frames, broll_frames, shot_counts, dominant_broll_type, secondary_broll_type, shot_sequence, analysis_confidence, notes) VALUES")
    output.append(",\n".join(shot_rows) + ";")
    output.append("")

if ost_rows:
    output.append("-- video_onscreen_text")
    output.append("INSERT INTO video_onscreen_text (video_id, timestamp_seconds, text_content, text_type, is_persistent) VALUES")
    output.append(",\n".join(ost_rows) + ";")
    output.append("")

if cta_rows:
    output.append("-- video_ctas")
    output.append("INSERT INTO video_ctas (video_id, timestamp_seconds, cta_text, cta_type, cta_position) VALUES")
    output.append(",\n".join(cta_rows) + ";")
    output.append("")

if vn_rows:
    output.append("-- video_visual_notes")
    output.append("INSERT INTO video_visual_notes (video_id, format_type, hook_style, scene_descriptions, product_reveal_timestamp, overall_notes, cross_reference_notes) VALUES")
    output.append(",\n".join(vn_rows) + ";")
    output.append("")

if vs_rows:
    output.append("-- video_visual_scripts")
    output.append("INSERT INTO video_visual_scripts (video_id, full_visual_script, is_primary_script) VALUES")
    output.append(",\n".join(vs_rows) + ";")

sql_text = "\n".join(output)
out_path = Path(__file__).parent / "insert_visual.sql"
out_path.write_text(sql_text)
print(f"Generated SQL: {len(shot_rows)} shot_breakdown, {len(ost_rows)} onscreen_text, {len(cta_rows)} ctas, {len(vn_rows)} visual_notes, {len(vs_rows)} visual_scripts")
print(f"Saved to {out_path}")
