"""
Visual frame analysis for 6 TikTok videos using Anthropic Claude API.
Analyzes frames to extract: shot breakdown, on-screen text, CTAs, visual notes, visual scripts.
Outputs JSON results for Supabase insertion.
"""
import os, json, base64, sys
from pathlib import Path
import anthropic

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

FRAMES_DIR = Path(__file__).parent / "frames"

# UUID mapping from tiktok_videos inserts
UUID_MAP = {
    "7602822534198775053": "4fe54f02-783c-454a-a0fe-e763159c2f5e",
    "7603942722579320077": "90e2a52c-ce75-4937-a10f-2b8bd609211a",
    "7615445522677026079": "97587968-314e-4517-800c-c41052087ea2",
    "7617704190168567053": "7986265d-330a-4359-a9c2-daeb4aeecdd4",
    "7619817856556993806": "83df0093-2d87-4387-bd21-9c4f4ac0fdc9",
    "7625051711895424270": "70e0baf2-b95d-4e2c-8c1d-31130430308d",
}

TRANSCRIPTS = json.loads((Path(__file__).parent / "transcripts.json").read_text())

ANALYSIS_PROMPT = """Analyze these video frames from a TikTok product video. The frames are in chronological order.

Transcript of this video:
{transcript}

For each frame, classify the shot type as one of:
- FACE (creator's face visible, talking head)
- PRODUCT_CLOSEUP (close-up of product)
- HANDS_ON_DEMO (hands using/demonstrating product)
- LIFESTYLE (lifestyle/context shot)
- TEXT_SCREEN (mostly text overlay)
- TRANSITION (transition/effect)
- BEFORE_AFTER (comparison shot)
- PACKAGING (product packaging/unboxing)
- SCREEN_RECORDING (phone/screen recording)

Also extract:
1. Any on-screen text visible in frames (with approximate timestamp position: beginning/middle/end)
2. Any call-to-action elements (link mentions, shopping cart, "link below" etc.)
3. Overall visual style and format notes

Return a JSON object with this exact structure:
{{
  "shot_breakdown": {{
    "total_frames": <number>,
    "face_frames": <number>,
    "broll_frames": <number>,
    "shot_counts": {{"FACE": N, "PRODUCT_CLOSEUP": N, ...}},
    "dominant_broll_type": "<most common non-face type or null>",
    "secondary_broll_type": "<second most common or null>",
    "shot_sequence": ["FACE", "PRODUCT_CLOSEUP", ...],
    "analysis_confidence": "high|medium|low",
    "notes": "<brief analysis note>"
  }},
  "onscreen_text": [
    {{
      "timestamp_position": "beginning|middle|end",
      "text_content": "<exact text>",
      "text_type": "hook_banner|product_name|price_tag|cta_overlay|educational_overlay|comparison_graphic|subtitle",
      "is_persistent": true/false
    }}
  ],
  "ctas": [
    {{
      "timestamp_position": "beginning|middle|end",
      "cta_text": "<CTA text>",
      "cta_type": "verbal|visual|link_in_bio|shopping_cart|swipe_up",
      "cta_position": "beginning|middle|end"
    }}
  ],
  "visual_notes": {{
    "format_type": "talking_head|product_demo|voiceover_broll|comparison|tutorial|montage",
    "hook_style": "question|warning|bold_claim|before_after|curiosity|demonstration|shock_value",
    "scene_descriptions": [
      {{"timestamp": 0, "description": "<what happens>"}},
      {{"timestamp": <mid>, "description": "<what happens>"}},
      {{"timestamp": <end>, "description": "<what happens>"}}
    ],
    "product_reveal_timestamp": <seconds or null>,
    "overall_notes": "<2-3 sentence summary of visual strategy>",
    "cross_reference_notes": "<how visuals relate to transcript>"
  }},
  "visual_script": "<Full visual script describing what happens visually from start to finish, frame by frame, in prose format. 3-5 sentences.>"
}}

Return ONLY valid JSON, no markdown fences."""


def load_frames(video_id, max_frames=12):
    """Load up to max_frames evenly sampled from the video's frame directory."""
    frame_dir = FRAMES_DIR / video_id
    frames = sorted(frame_dir.glob("*.jpg"))
    if len(frames) > max_frames:
        step = len(frames) / max_frames
        frames = [frames[int(i * step)] for i in range(max_frames)]

    result = []
    for f in frames:
        with open(f, "rb") as fh:
            data = base64.standard_b64encode(fh.read()).decode("utf-8")
            result.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": data}
            })
    return result


def analyze_video(video_id):
    """Run visual analysis on a single video's frames."""
    print(f"  Analyzing {video_id}...")
    transcript = TRANSCRIPTS.get(video_id, "No transcript available")

    frames = load_frames(video_id)
    if not frames:
        print(f"  WARNING: No frames for {video_id}")
        return None

    content = [{"type": "text", "text": ANALYSIS_PROMPT.format(transcript=transcript[:1500])}]
    content.extend(frames)

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        messages=[{"role": "user", "content": content}],
    )

    raw = response.content[0].text.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
        if raw.endswith("```"):
            raw = raw[:-3]

    try:
        result = json.loads(raw)
        result["_uuid"] = UUID_MAP[video_id]
        result["_video_id"] = video_id
        print(f"  OK — {len(frames)} frames analyzed")
        return result
    except json.JSONDecodeError as e:
        print(f"  ERROR parsing JSON for {video_id}: {e}")
        print(f"  Raw: {raw[:200]}")
        return None


def main():
    video_ids = list(UUID_MAP.keys())
    if len(sys.argv) > 1:
        video_ids = sys.argv[1:]

    results = {}
    for vid in video_ids:
        r = analyze_video(vid)
        if r:
            results[vid] = r

    out_path = Path(__file__).parent / "visual_analysis.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved {len(results)} analyses to {out_path}")


if __name__ == "__main__":
    main()
