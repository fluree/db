# Hero GIF for the README: the "Zero to graph in 60 seconds" tour.
#
# Regenerate from the repo root (needs `vhs` and a `fluree` binary on PATH):
#
#   vhs docs/assets/readme/tour.tape
#
# Output: docs/assets/readme/tour.gif (target: <= 20 s, <= 2 MB, 1200 px wide).
# The tape runs in /tmp/fluree-tour (wiped at the start), so it never touches your own
# .fluree project. Colors use the Fluree brand palette (see BRAND.md).

Output docs/assets/readme/tour.gif

Require fluree

Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 600
Set Padding 28
Set Framerate 12
Set TypingSpeed 15ms
Set PlaybackSpeed 1.0
Set CursorBlink false
Set Theme { "name": "Fluree Deep", "background": "#091133", "foreground": "#FDFDFD", "cursor": "#13C6FF", "selection": "#282966", "black": "#091133", "red": "#FF6813", "green": "#26C575", "yellow": "#EEC638", "blue": "#13C6FF", "magenta": "#B775D6", "cyan": "#18CFDB", "white": "#F6F6F6", "brightBlack": "#6A7893", "brightRed": "#FF6813", "brightGreen": "#26C575", "brightYellow": "#EEC638", "brightBlue": "#13C6FF", "brightMagenta": "#B775D6", "brightCyan": "#18CFDB", "brightWhite": "#FFFFFF" }

# Set up a clean scratch project, off camera.
Hide
Type `mkdir -p /tmp/fluree-tour && cd /tmp/fluree-tour && rm -rf .fluree && export PS1='\[\e[38;2;19;198;255m\]$\[\e[0m\] ' && export Q='SELECT ?title ?genre WHERE { ?m a <http://schema.org/Movie> ; <http://schema.org/name> ?title . OPTIONAL { ?m <http://schema.org/genre> ?genre } }' && clear`
Enter
Sleep 300ms
Show

Type `fluree init && fluree create movies`
Enter
Sleep 1s

Type `fluree insert '@prefix s: <http://schema.org/> . @prefix ex: <http://example.org/> .`
Enter
Type `ex:blade-runner a s:Movie ; s:name "Blade Runner" ; s:director ex:ridley-scott .`
Enter
Type `ex:alien a s:Movie ; s:name "Alien" ; s:director ex:ridley-scott .'`
Enter
Sleep 1s

Type `fluree update 'PREFIX s: <http://schema.org/> PREFIX ex: <http://example.org/>`
Enter
Type `INSERT { ?m s:genre "sci-fi" } WHERE { ?m s:director ex:ridley-scott }'`
Enter
Sleep 1s

Type `clear`
Enter
Type `# Now: every Ridley Scott movie has a genre`
Enter
Type `fluree query --format table "$Q"`
Enter
Sleep 2s

Type `# Before the update (t=1): time travel is built in`
Enter
Type `fluree query --at 1 --format table "$Q"`
Enter
Sleep 3s
