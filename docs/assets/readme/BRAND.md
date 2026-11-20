# README brand assets

Everything the repository README uses for its look lives in this folder. Colors and
logo come from the Fluree website's brand system (`www-flur.ee`: the `--fluree-*` CSS
tokens and `public/fluree-logo.svg`). Don't add colors that aren't on that list.

## Colors

| Token | Hex | Used for |
|---|---|---|
| Deep | `#091133` | Badge label background, light-mode wordmark, GIF background, social preview background |
| Vibrant | `#13C6FF` | Status badges (release, Docker pulls, CI), GIF prompt and cursor |
| Purple | `#4B57A5` | Link badges (license, docs) |
| Twilight | `#202152` | Social preview gradient |
| Snowflake | `#FDFDFD` | GIF foreground text |

Badges are [shields.io](https://shields.io) `flat-square` with `labelColor=091133`.

## Files

| File | What it is | How to update |
|---|---|---|
| `fluree-wordmark-light.svg` | Official logo (`www-flur.ee/public/fluree-logo.svg`), Deep fill, for light backgrounds | Replace with the newest official file |
| `fluree-wordmark-dark.svg` | Same path recolored to white for GitHub dark mode | Re-derive from the light file, or replace with an official reversed logo |
| `tour.tape` / `tour.gif` | Hero recording of the 60-second tour | `vhs docs/assets/readme/tour.tape` from the repo root (needs [VHS](https://github.com/charmbracelet/vhs) and `fluree` on `PATH`) |
| `dblp-core-geomean.svg` | DBLP-core chart, copied from `fluree/benchmark-db/assets/` | `python3 scripts/readme/sync-bench.py --bench-repo ../benchmark-db` (also run weekly by `.github/workflows/readme-bench-sync.yml`) |
| `social-preview.html` / `social-preview.png` | 1280×640 repository social preview | Edit the HTML, then screenshot it at 1280×640 (see the comment at the top of the HTML). Upload the PNG in Settings → General → Social preview |

## Benchmark numbers

The README's benchmark block is generated. Edit `scripts/readme/sync-bench.py`, not the
README, and never type a number into that block by hand.
