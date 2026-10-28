#!/usr/bin/env python3
"""
Transcribe every grouped video ad in every race with Whisper, for newsletter research.

Ads that are cuts of the same creative are grouped the same way the race pages do it (an ad's
`variantOf` points at the ad it was merged into), and only the longest cut in each group is
transcribed -- the shorter ones are trims of the same script.

Nothing is written to Firestore or the bucket. Each transcript is saved as
`<out>/<race>/<ad_id>.json`, and `<out>/transcripts.md` is rebuilt from all of them on every
run so there's one file to read through. An ad that already has a transcript is skipped, so
re-running after a pipeline run only picks up new ads (or a longer cut of an existing one).

Meant to run on a machine with a GPU. Whisper picks it up through torch, so on an AMD card this
needs the ROCm build of PyTorch; it falls back to CPU (slowly) if torch can't see a GPU.

Usage:
    python -m commands.transcribe_ads
    python -m commands.transcribe_ads --race PA-H-07,IL-S
    python -m commands.transcribe_ads --model large-v3 --out ~/ad-transcripts
    python -m commands.transcribe_ads --force          # redo ads that already have a transcript
    python -m commands.transcribe_ads --dry-run        # list what would be transcribed
"""

import argparse
import json
import os
import tempfile
from collections import defaultdict

import requests

from Database import Database

# Same origin the frontend serves rehosted media from (MEDIA_BASE_URL in utils/ads.ts). The
# bucket is public, so no storage credentials are needed to read from it.
MEDIA_BASE_URL = "https://storage.googleapis.com/follow-the-crypto-ads"

DEFAULT_OUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "ad_transcripts"
)


def resolve_primary_id(ad_id, ads):
    """Walk variantOf up to the ad nobody points at.

    Port of resolvePrimaryId in the frontend's utils/ads.ts, so the groups here match the ones
    shown on the race pages. A cycle resolves to the alphabetically first ad in it.
    """
    seen = {ad_id}
    current = ad_id
    while True:
        parent = ads.get(current, {}).get("variantOf")
        if not parent or parent not in ads:
            return current
        if parent in seen:
            cycle = []
            node = parent
            while True:
                cycle.append(node)
                node = ads[node]["variantOf"]
                if node == parent:
                    break
            return sorted(cycle)[0]
        seen.add(parent)
        current = parent


def get_ad_groups(client):
    """Return {race: [(primary_id, [ads in group])]} for every video ad with a rehosted copy."""
    ads_by_race = defaultdict(dict)
    for doc in client.collection("ads").stream():
        for ad_id, ad in (doc.to_dict().get("ads") or {}).items():
            if ad.get("type") != "google" or ad.get("excluded"):
                continue
            if not ad.get("race"):
                continue
            ads_by_race[ad["race"]][ad_id] = ad

    groups_by_race = {}
    for race, ads in ads_by_race.items():
        groups = defaultdict(list)
        for ad_id in ads:
            groups[resolve_primary_id(ad_id, ads)].append(ads[ad_id])
        # Groups are resolved over every ad in the race, not just the video ones, so a
        # variantOf chain that passes through an ad with no video still lands on the
        # same primary the frontend picks.
        video_groups = []
        for primary_id, group in groups.items():
            videos = [ad for ad in group if ad.get("video")]
            if videos:
                video_groups.append((primary_id, group))
        if video_groups:
            groups_by_race[race] = video_groups
    return groups_by_race


def longest_cut(group):
    videos = [ad for ad in group if ad.get("video")]
    return max(videos, key=lambda ad: ad["video"].get("duration") or 0)


def download(key, destination):
    with requests.get(f"{MEDIA_BASE_URL}/{key}", stream=True, timeout=180) as response:
        response.raise_for_status()
        with open(destination, "wb") as handle:
            for block in response.iter_content(chunk_size=1 << 16):
                handle.write(block)


def transcribe(model, path, fp16):
    # condition_on_previous_text=False stops one misheard line from being carried into every
    # segment after it, which matters more than cross-segment context on 30-second clips.
    result = model.transcribe(
        path, language="en", fp16=fp16, condition_on_previous_text=False
    )
    return {
        "text": result["text"].strip(),
        "segments": [
            {
                "start": round(segment["start"], 2),
                "end": round(segment["end"], 2),
                "text": segment["text"].strip(),
                # Kept so a suspect line (Whisper filling a music bed with words) can be
                # spotted: high no_speech_prob or very low avg_logprob means it's likely
                # invented.
                "no_speech_prob": round(segment["no_speech_prob"], 3),
                "avg_logprob": round(segment["avg_logprob"], 3),
            }
            for segment in result["segments"]
        ],
    }


def format_timestamp(seconds):
    return f"{int(seconds // 60)}:{int(seconds % 60):02d}"


def write_digest(out_dir):
    """Rebuild transcripts.md from every saved transcript, grouped by race."""
    records = []
    for race in sorted(os.listdir(out_dir)):
        race_dir = os.path.join(out_dir, race)
        if not os.path.isdir(race_dir):
            continue
        for filename in sorted(os.listdir(race_dir)):
            if filename.endswith(".json"):
                with open(os.path.join(race_dir, filename)) as handle:
                    records.append(json.load(handle))

    by_race = defaultdict(list)
    for record in records:
        by_race[record["race"]].append(record)

    lines = ["# Ad transcripts", ""]
    for race in sorted(by_race):
        lines += [f"## {race}", ""]
        for record in sorted(by_race[race], key=lambda r: r.get("date_range_start") or ""):
            lines.append(f"### {record['committee_name']} ({record['date_range_start']})")
            lines.append("")
            details = [
                f"{format_timestamp(record['duration'] or 0)} long",
                f"{record['date_range_start']} to {record['date_range_end']}",
            ]
            if len(record["variant_ids"]) > 1:
                details.append(f"longest of {len(record['variant_ids'])} cuts")
            lines.append(" · ".join(details))
            lines.append("")
            lines.append(record["ad_url"])
            lines.append("")
            lines.append(record["text"] or "_No speech detected._")
            lines.append("")

    with open(os.path.join(out_dir, "transcripts.md"), "w") as handle:
        handle.write("\n".join(lines))
    print(f"Wrote {len(records)} transcripts to {os.path.join(out_dir, 'transcripts.md')}")


def main():
    parser = argparse.ArgumentParser(description="Transcribe grouped video ads with Whisper")
    parser.add_argument("--race", help="Comma-separated race ids to limit to")
    parser.add_argument("--model", default="large-v3", help="Whisper model name")
    parser.add_argument("--out", default=DEFAULT_OUT_DIR, help="Output directory")
    parser.add_argument("--force", action="store_true", help="Redo existing transcripts")
    parser.add_argument("--dry-run", action="store_true", help="List ads without transcribing")
    args = parser.parse_args()

    out_dir = os.path.expanduser(args.out)
    races = set(args.race.split(",")) if args.race else None

    client = Database().client
    committees = client.collection("constants").document("committees").get().to_dict() or {}
    groups_by_race = get_ad_groups(client)

    pending = []
    for race in sorted(groups_by_race):
        if races and race not in races:
            continue
        for primary_id, group in groups_by_race[race]:
            ad = longest_cut(group)
            path = os.path.join(out_dir, race, f"{ad['ad_id']}.json")
            if os.path.exists(path) and not args.force:
                continue
            pending.append((race, ad, [variant["ad_id"] for variant in group], path))

    print(f"{len(pending)} ads to transcribe.")
    if args.dry_run:
        for race, ad, variant_ids, _ in pending:
            print(f"  {race}  {ad['ad_id']}  {ad['video'].get('duration')}s  {ad['ad_url']}")
        return
    if not pending:
        write_digest(out_dir)
        return

    # Imported here so --dry-run works on a machine without torch installed.
    import torch
    import whisper

    # ROCm builds of torch expose AMD GPUs through the cuda device name.
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Loading Whisper {args.model} on {device}.")
    model = whisper.load_model(args.model, device=device)

    for index, (race, ad, variant_ids, path) in enumerate(pending, start=1):
        print(f"[{index}/{len(pending)}] {race} {ad['ad_id']}")
        try:
            with tempfile.TemporaryDirectory() as workdir:
                video = os.path.join(workdir, "original.mp4")
                download(ad["video"]["original"], video)
                transcript = transcribe(model, video, fp16=device == "cuda")
        except Exception as error:
            print(f"  Failed: {error}")
            continue

        record = {
            "race": race,
            "ad_id": ad["ad_id"],
            "variant_ids": variant_ids,
            "committee_id": ad["fec_id"],
            "committee_name": committees.get(ad["fec_id"], {}).get("name", ad["fec_id"]),
            "ad_url": ad["ad_url"],
            "date_range_start": ad.get("date_range_start"),
            "date_range_end": ad.get("date_range_end"),
            "duration": ad["video"].get("duration"),
            "model": args.model,
            **transcript,
        }
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as handle:
            json.dump(record, handle, indent=2)

    write_digest(out_dir)


if __name__ == "__main__":
    main()
