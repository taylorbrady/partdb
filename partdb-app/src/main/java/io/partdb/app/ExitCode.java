package io.partdb.app;

enum ExitCode {
    OK(0),
    FAILED(1),
    USAGE(2),
    NOT_FOUND(3);

    private final int code;

    ExitCode(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }
}
