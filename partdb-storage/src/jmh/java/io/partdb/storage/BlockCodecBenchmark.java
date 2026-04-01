package io.partdb.storage;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
public class BlockCodecBenchmark {

    @Benchmark
    public byte[] compress(CodecState state) {
        return state.codec.compress(state.payload);
    }

    @Benchmark
    public byte[] decompress(CodecState state) {
        return state.codec.decompress(state.compressedPayload, state.payload.length);
    }

    @State(Scope.Benchmark)
    public static class CodecState {
        @Param({"NONE", "DEFLATE"})
        String codecName;

        @Param({"512", "4096", "32768"})
        int payloadSize;

        @Param({"COMPRESSIBLE", "SEMI_COMPRESSIBLE", "RANDOM"})
        String payloadKind;

        BlockCodec codec;
        byte[] payload;
        byte[] compressedPayload;

        @Setup(Level.Trial)
        public void setup() {
            codec = BlockCodec.valueOf(codecName);
            payload = StorageBenchmarkSupport.payload(payloadKind, payloadSize, 0xabcL + payloadSize);
            compressedPayload = codec.compress(payload);
        }
    }
}
