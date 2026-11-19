import io.pyroscope.vendor.one.profiler.AsyncProfiler;

public class Busy {
    private static volatile long result;

    public static void main(String[] args) throws Exception {
        AsyncProfiler profiler = AsyncProfiler.getInstance(args[0]);
        System.out.println("async-profiler " + profiler.getVersion());
        profiler.execute("start,event=itimer,interval=1ms,jfr,jfropts=7,file=" + args[1]);
        long end = System.nanoTime() + 2_000_000_000L;
        while (System.nanoTime() < end) {
            for (int i = 0; i < 10000; i++) {
                result += i;
            }
        }
        profiler.execute("stop");
    }
}
