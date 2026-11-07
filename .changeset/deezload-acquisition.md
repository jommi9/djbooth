---
"@gakonst/tracks": minor
"@gakonst/djbooth": patch
"@gakonst/djbooth-app": patch
---

Add a `deezload` acquisition backend that drives the external deezload CLI with `DEEZLOAD_ARL` to
download exact Deezer recordings as FLAC or MP3 ahead of Soulseek and web sources. Candidates come
from the public Deezer catalog, ISRC matches are preferred, delivered files keep their container, and
Deezer sources appear as clickable links in the workbench player.
