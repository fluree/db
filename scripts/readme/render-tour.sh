#!/usr/bin/env bash
# Render the README hero GIF (docs/assets/readme/tour.gif) from tour.tape.
#
# 1. vhs records the tape to tour.raw.gif.
# 2. ffmpeg puts the last frame first (as a 1-frame poster), so anything that shows
#    only the first frame (reduced motion, link previews) shows the finished result.
#    The loop still reads naturally: it opens on the result, then replays the tour.
#
# Needs: vhs, ffmpeg, and `fluree` on PATH. Run from anywhere.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
dir="$root/docs/assets/readme"
cd "$root"

vhs "$dir/tour.tape"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

ffmpeg -v error -y -sseof -0.1 -i "$dir/tour.raw.gif" -frames:v 1 -update 1 "$tmp/poster.png"
ffmpeg -v error -y \
  -loop 1 -t 0.07 -framerate 15 -i "$tmp/poster.png" \
  -i "$dir/tour.raw.gif" \
  -filter_complex "[0:v]fps=15,format=rgb24[p];[1:v]fps=15,format=rgb24[g];[p][g]concat=n=2:v=1[v];[v]split[a][b];[a]palettegen=max_colors=64:stats_mode=full[pal];[b][pal]paletteuse=dither=none" \
  -loop 0 "$dir/tour.gif"
rm -f "$dir/tour.raw.gif"

size=$(wc -c <"$dir/tour.gif" | tr -d ' ')
dur=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$dir/tour.gif")
echo "wrote docs/assets/readme/tour.gif (${size} bytes, ${dur}s)"
