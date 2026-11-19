# async-profiler 4 regression recording

`async-profiler-4.5.jfr.gz` records two seconds of the synthetic `Busy.java`
workload with async-profiler 4.5.0.0 (the macOS native library bundled in
`io.pyroscope:agent:2.9.2`) and OpenJDK 11.0.27.

It contains the `jdk.types.ChunkHeader` constant-pool entry introduced in
async-profiler 4.3. `jfr-parser v0.9.0` rejects the recording with
`error reading CP: unknown type 0`; v0.19.0 parses it successfully.
The fixture tests the collector's own JFR-to-pprof conversion, including Java
stacks, CPU sample values, aggregation, and serialized pprof output.
The existing receiver tests cover legacy CPU and allocation/live-object JFRs.

The fixture has no application or production data. `jfropts=7` disables system
information, system properties, and native library paths when recording.

To regenerate on macOS with a JDK and the agent JAR downloaded from Maven Central:

```sh
unzip -p agent-2.9.2.jar libasyncProfiler-macos.so > libasyncProfiler.so
javac -cp agent-2.9.2.jar Busy.java
java -cp agent-2.9.2.jar:. Busy "$PWD/libasyncProfiler.so" "$PWD/recording.jfr"
python3 - <<'PY'
import gzip
from pathlib import Path
Path('async-profiler-4.5.jfr.gz').write_bytes(
    gzip.compress(Path('recording.jfr').read_bytes(), mtime=0)
)
PY
```

The checked-in recording contains 258 `jdk.ExecutionSample` events, independently
verified with `jfr summary` and `jfr print --json --events jdk.ExecutionSample`.
All have `STATE_DEFAULT`; at 1000 Hz the expected CPU total is 258,000,000 ns.
Sample counts vary when regenerating; update that expected total from the JDK
tool output if the fixture is replaced. Go tests read the checked-in fixture and
require neither Java nor async-profiler at test time.
