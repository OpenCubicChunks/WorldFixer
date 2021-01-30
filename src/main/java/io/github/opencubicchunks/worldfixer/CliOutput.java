package io.github.opencubicchunks.worldfixer;

import static org.fusesource.jansi.Ansi.ansi;

import org.fusesource.jansi.Ansi;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Supplier;

/**
 * Wraps jansi and System.out.println() to produce and keep output in the form:
 *
 * [warnins and errors]
 * Main action message
 * Secondary action message or currently processed chunk position
 * Progress
 *
 * In case jansi support is disabled (not supported or on windows, which may not support changing cursor position)
 * progress output stays on one line using \r, and everything else is printed in it's own lines.
 * Chunk positions are skipped to avoid too much output.
 *
 * In all cases, the dynamically updated lines update no more than once every 200ms (outputTimer).
 */
public class CliOutput implements StatusHandler {

    private final int outputTimer = 800;
    static boolean jansi = false;
    private volatile boolean lastNewline = true;
    private String action1 = "";
    private String action2 = "";
    private String lastProgress = "";

    private volatile long lastProgressPrintTime = System.currentTimeMillis();
    private volatile long lastChunkInfoPrintTime = System.currentTimeMillis();
    static boolean printChunkInfo = false;

    @Override public void status(String txt) {
        synchronized (CliOutput.class) {
            if (!jansi) {
                noColorPrintln(txt);
                return;
            }
            txt = txt.substring(0, Math.min(txt.length(), 80));
            System.out.print(ansi()
                .eraseLine(Ansi.Erase.ALL)
                .fg(Ansi.Color.CYAN)
                .a(action1 = String.format("%1$-78s", txt))
                .reset()
                .cursorToColumn(0));
        }
    }

    private void noColorPrintln(String txt) {
        if (txt.isEmpty()) {
            return;
        }
        if (!lastNewline) {
            System.out.println();
        }
        System.out.println(txt);
        lastNewline = true;
    }

    private void noColorPrintln(String txt, Throwable t) {
        if (!lastNewline) {
            System.out.println();
        }
        System.out.println(txt);
        t.printStackTrace(System.out);
        lastNewline = true;
    }

    private void noColorPrint(String txt) {
        System.out.print(txt);
        lastNewline = false;
    }

    @Override public void info(String txt) {
        synchronized (CliOutput.class) {
            if (!jansi) {
                noColorPrintln(txt);
                return;
            }
            txt = txt.substring(0, Math.min(txt.length(), 80));
            System.out.print(ansi()
                .cursorDownLine()
                .eraseLine(Ansi.Erase.ALL)
                .fg(Ansi.Color.CYAN)
                .a(action2 = String.format("%1$-78s", txt))
                .reset()
                .cursorUpLine()
                .cursorToColumn(0));
        }
    }


    @Override public void chunkInfo(Supplier<String> info) {
        if (!printChunkInfo) {
            return;
        }
        if (!jansi) {
            return;
        }
        long time = System.currentTimeMillis();
        if (Math.abs(time - lastChunkInfoPrintTime) > outputTimer) {
            synchronized (CliOutput.class) {
                String txt = info.get();
                txt = txt.substring(0, Math.min(txt.length(), 80));

                System.out.print(ansi()
                    .cursorDownLine()
                    .eraseLine(Ansi.Erase.ALL)
                    .fg(Ansi.Color.CYAN)
                    .a(action2 = String.format("%1$-78s", txt))
                    .reset()
                    .cursorUpLine()
                    .cursorToColumn(0));
                lastChunkInfoPrintTime = System.currentTimeMillis();
            }
        }
    }

    @Override public void progress(Supplier<Double> progressString, Supplier<String> progress, boolean isDone) {
        long time = System.currentTimeMillis();
        if (isDone || Math.abs(time - lastProgressPrintTime) > outputTimer) {
            synchronized (CliOutput.class) {
                if (!jansi) {
                    noColorPrint(progress.get());
                    lastProgressPrintTime = System.currentTimeMillis();
                    return;
                }
                String txt = '\r' + progress.get();
                txt = txt.substring(0, Math.min(txt.length(), 80));

                if (printChunkInfo) {
                    System.out.print(ansi()
                            .cursorDownLine(2)
                            .eraseLine(Ansi.Erase.ALL)
                            .fg(Ansi.Color.GREEN)
                            .a(lastProgress = String.format("%1$-78s", txt))
                            .reset()
                            .cursorUpLine(2)
                            .cursorToColumn(0));
                } else {
                    System.out.print(ansi()
                            .eraseLine(Ansi.Erase.ALL)
                            .fg(Ansi.Color.GREEN)
                            .a(lastProgress = String.format("%1$-78s", txt))
                            .reset()
                            .cursorToColumn(0));
                }
                lastProgressPrintTime = System.currentTimeMillis();;
            }
        }
    }

    @Override public void error(String msg, Throwable exception) {
        synchronized (CliOutput.class) {
            if (!jansi) {
                noColorPrintln(msg, exception);
                return;
            }
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);
            String[] stackTrace = sw.toString().split("\n");
            printErr(msg);
            for (String s : stackTrace) {
                printErr(s);
            }

            System.out.print(ansi()
                .a("\n\n")
                .cursorUpLine(2));

            status(action1);
            info(action2);
            progress(() -> 0.0, () -> lastProgress, true);
        }
    }

    @Override public void warning(String msg) {
        synchronized (CliOutput.class) {
            if (!jansi) {
                noColorPrintln(msg);
                return;
            }
            System.out.print(ansi()
                .fg(Ansi.Color.YELLOW)
                .a(String.format("%1$-80s\n\n\n", msg))
                .reset()
                .cursorUpLine(2)
                .cursorToColumn(0));

            status(action1);
            info(action2);
            progress(() -> 0.0, () -> lastProgress, true);
        }
    }

    private void printErr(String msg) {
        msg = msg.replaceAll("\t", "    ");
        System.out.println(ansi()
            .fg(Ansi.Color.RED)
            .a(String.format("%1$-80s", msg))
            .reset()
            .cursorToColumn(0));
    }
}
