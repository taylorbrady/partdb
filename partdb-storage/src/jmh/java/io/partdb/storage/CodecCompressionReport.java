package io.partdb.storage;

import java.util.Locale;

public final class CodecCompressionReport {

    private static final String[] PAYLOAD_KINDS = {"COMPRESSIBLE", "SEMI_COMPRESSIBLE", "RANDOM"};
    private static final int[] PAYLOAD_SIZES = {512, 4096, 32768};

    private CodecCompressionReport() {
    }

    public static void main(String[] args) {
        System.out.println("codec\tpayloadKind\tpayloadSize\tcompressedBytes\tcompressionRatio");
        for (StoreOptions.Compression compression : StoreOptions.Compression.values()) {
            for (String payloadKind : PAYLOAD_KINDS) {
                for (int payloadSize : PAYLOAD_SIZES) {
                    byte[] payload = BenchmarkData.payload(payloadKind, payloadSize, 0xabcL + payloadSize);
                    byte[] compressed = BlockCodec.compress(compression, payload);
                    System.out.printf(
                        Locale.ROOT,
                        "%s\t%s\t%d\t%d\t%.4f%n",
                        compression,
                        payloadKind,
                        payloadSize,
                        compressed.length,
                        (double) compressed.length / payload.length
                    );
                }
            }
        }
    }
}
