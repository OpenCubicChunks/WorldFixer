package io.github.opencubicchunks.worldfixer;

import java.util.function.Supplier;

public interface StatusHandler {

    void status(String txt);

    void info(String txt);

    void chunkInfo(Supplier<String> info);

    void progress(Supplier<Double> progressString, Supplier<String> progress, boolean isDone);

    void error(String msg, Throwable exception);

    void warning(String msg);
}
