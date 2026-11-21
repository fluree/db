# Hero GIF for the README: the "Zero to graph in 60 seconds" tour.
#
# Regenerate from the repo root (needs vhs, ffmpeg and a `fluree` binary on PATH):
#
#   scripts/readme/render-tour.sh
#
# That runs this tape, then puts the final frame first so static previews
# (reduced motion, link unfurls) show the result instead of an empty prompt.
#
# The tape works in /tmp/fluree-tour (wiped first), so it never touches your own
# .fluree project. Off-camera setup lives in tour-setup.sh: it writes movies.rq
# (shown with `cat` on camera), sets the prompt, and drops the "novelty is NKB ...
# run `fluree index`" maintenance notice from stderr. Every other line is real output.
# Colors use the Fluree brand palette (see BRAND.md).

Output docs/assets/readme/tour.raw.gif

Require fluree

Set Shell "bash"
Set FontSize 22
Set LineHeight 1.15
Set Width 1200
Set Height 800
Set Padding 26
Set Margin 18
Set MarginFill "#202152"
Set WindowBar Colorful
Set WindowBarSize 36
Set BorderRadius 12
Set Framerate 15
Set TypingSpeed 16ms
Set PlaybackSpeed 1.0
Set CursorBlink false
Set Theme { "name": "Fluree Deep", "background": "#091133", "foreground": "#FDFDFD", "cursor": "#13C6FF", "selection": "#282966", "black": "#091133", "red": "#FF6813", "green": "#26C575", "yellow": "#EEC638", "blue": "#13C6FF", "magenta": "#B775D6", "cyan": "#18CFDB", "white": "#F6F6F6", "brightBlack": "#6A7893", "brightRed": "#FF6813", "brightGreen": "#26C575", "brightYellow": "#EEC638", "brightBlue": "#13C6FF", "brightMagenta": "#B775D6", "brightCyan": "#18CFDB", "brightWhite": "#FFFFFF" }

Hide
Type `source docs/assets/readme/tour-setup.sh`
Enter
Sleep 400ms
Show

Type `fluree init && fluree create movies`
Enter
Sleep 900ms

Type `fluree insert '@prefix s: <http://schema.org/> .`
Enter
Type `@prefix ex: <http://example.org/> .`
Enter
Type `ex:blade-runner  s:name "Blade Runner" ; s:director ex:ridley .`
Enter
Type `ex:alien         s:name "Alien"        ; s:director ex:ridley .'`
Enter
Sleep 900ms

Type `fluree update 'PREFIX s: <http://schema.org/>`
Enter
Type `PREFIX ex: <http://example.org/>`
Enter
Type `INSERT { ?m s:genre "sci-fi" } WHERE { ?m s:director ex:ridley }'`
Enter
Sleep 1.2s

Type `clear`
Enter
Type `cat movies.rq`
Enter
Sleep 900ms

Type `fluree query -f movies.rq`
Enter
Sleep 1.6s

Type `fluree query -f movies.rq --at 1     # before the update`
Enter
Sleep 3.2s
