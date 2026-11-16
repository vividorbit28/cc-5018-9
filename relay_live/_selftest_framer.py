import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import subprocess
from common import AccessUnitFramer, iter_nals

p = subprocess.Popen(
    ["./bin/ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
     "-i", "testsrc2=size=320x180:rate=15", "-c:v", "libx264",
     "-preset", "veryfast", "-g", "15", "-t", "2", "-f", "h264", "pipe:1"],
    stdout=subprocess.PIPE)
data = p.stdout.read()
p.wait()
framer = AccessUnitFramer()
aus = framer.push(data)
key = [a for a in aus if a[0]]
types = sorted({n.nal_type for _, _, au in aus for n in iter_nals(au)})
print("total AU:", len(aus), "keyframes:", len(key),
      "nal types present:", types)
print("first AU keyframe:", aus[0][0], "has init:", aus[0][1])
assert aus[0][0] and aus[0][1]
assert len(key) >= 2
# Every keyframe AU should be independently decodable after init replay.
for is_key, has_init, au in aus:
    if is_key:
        nals = [n.nal_type for n in iter_nals(au)]
        assert 5 in nals
print("FRAMER_OK")
