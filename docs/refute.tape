# The front page's GIF, recorded rather than drawn.
#
# `vhs` runs these commands in a real terminal and films the result, so the GIF is an
# execution and not an illustration - which is the same distinction the rest of this
# repository is about. `make gif` regenerates it, so it is a reproducible artifact like every
# other number here: if `make refute` starts printing something else, re-running this tape
# says so, and the GIF in `docs/img/` stops matching the one in the tree.
#
# It films `make refute SEED=424242` and NOT `make demo`. The demo is the pleasant command;
# `refute` is the one that matters, because it runs every claim about the data again on a seed
# the author did not choose, and the chain refuses the result as evidence. A GIF of the happy
# path would be the pasted transcript this repository already removed once, in colour.
#
# The frame is sized to the OUTPUT rather than to a pleasant default: 13 point over 1200
# pixels is the width at which the longest line `refute` prints - the one about
# `evidence/refutations.jsonl` - does not wrap, and 430 is the height of what it prints with
# nothing left over. A GIF with half a screen of black is a GIF that is mostly padding.
#
# Recorded on WSL2 Ubuntu 24.04. vhs, ttyd and ffmpeg are not in the fast lane's dependencies
# and nothing in CI runs this: the artifact is committed, and `make gif` is how it is redone.

Output docs/img/refute.gif

Require make

Set Shell "bash"
Set FontSize 13
Set Width 1200
Set Height 430
Set Padding 20
Set Framerate 10
Set Theme "Catppuccin Mocha"
Set TypingSpeed 60ms

# Off camera: the prompt, the PATH and the working directory are setup, not content.
Hide
Type "export PATH=$HOME/bin:$PATH; export PS1='$ '; cd ~/samegold; clear"
Enter
Sleep 1s
Show

Type "make refute SEED=424242"
Sleep 800ms
Enter

# Long enough for the seven claims about the data, which took 1m16s when this was recorded,
# and a real ceiling rather than a guess: a machine that takes four minutes is a machine whose
# GIF is wrong, and the tape should fail rather than film half a run.
Wait+Screen@4m /SG-09/
Sleep 4s
