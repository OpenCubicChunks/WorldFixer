package io.github.opencubicchunks.worldfixer;

import static org.fusesource.jansi.Ansi.ansi;

import cubicchunks.regionlib.api.storage.SaveSection;
import cubicchunks.regionlib.impl.EntryLocation2D;
import cubicchunks.regionlib.impl.EntryLocation3D;
import cubicchunks.regionlib.impl.save.SaveSection2D;
import cubicchunks.regionlib.impl.save.SaveSection3D;
import net.kyori.nbt.ByteArrayTag;
import net.kyori.nbt.CompoundTag;
import net.kyori.nbt.TagIO;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Color;
import org.fusesource.jansi.Ansi.Erase;
import org.fusesource.jansi.AnsiConsole;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Main {

    private static final CompoundTag NULL_TAG = new CompoundTag();
    private static final byte[] NULL_OPACITY_INDEX_DATA;
    static {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
        for (int i = 0; i < 256; i++) {
            try {
                out.writeInt(Integer.MIN_VALUE + 32);
                out.writeInt(Integer.MIN_VALUE + 32);
                out.writeShort(0);
            } catch (IOException e) {
                throw new Error();
            }
        }
        NULL_OPACITY_INDEX_DATA = bout.toByteArray();
    }

    private static final ByteArrayTag NULL_OPACITY_INDEX = new ByteArrayTag(NULL_OPACITY_INDEX_DATA);

    private final ExecutorService fixingExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    private final ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
    private final AtomicInteger submittedFix = new AtomicInteger();
    private final AtomicInteger submittedIo = new AtomicInteger();
    private final AtomicInteger saved = new AtomicInteger();
    private final Map<Path, SaveSection<?, ?>> savesToClose = new HashMap<>();

    private final Output output = new Output();

    public static void main(String... args) throws IOException, InterruptedException {
        new Main().start(args);
    }

    private void start(String... args) throws IOException, InterruptedException {
        if (args.length == 0 || args[0].equals("-h") || args[0].equals("--help") || args[0].equals("-?")) {
            printHelp();
            return;
        }
        AnsiConsole.systemInstall();
        String ansi = System.getProperty("worldfixer.jansi", null);
        output.printChunkInfo = Boolean.parseBoolean(System.getProperty("worldfixer.printchunk", "false"));
        if (ansi == null) {
            output.jansi = !System.getProperty("os.name").startsWith("Windows") || !Ansi.isEnabled();
        } else {
            output.jansi = ansi.equalsIgnoreCase("true");
        }
        if (args[0].equals("-w") || args[0].equals("--world")) {
            if (args.length != 2) {
                System.out.println(ansi().fg(Color.RED).a("Expected 2 options but got " + args.length).reset());
                printHelp();
                return;
            }

            System.out.print(ansi().eraseScreen(Erase.ALL).cursor(0, 0));

            output.printStatus("Scanning worlds...");
            Path path = Paths.get(args[1]).toAbsolutePath();
            fixWorld(path.toString(), path.resolveSibling(path.getFileName() + "_fixed").toString());
            output.printStatus("Waiting for chunk fixes...");
            output.printInfo("");

            fixingExecutor.shutdown();
            fixingExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            if (!fixingExecutor.isTerminated()) {
                output.printWarning("FIXING EXECUTOR NOT TERMINATED AFTER TERMINATION!");
            }
            output.printStatus("Waiting to write save changes...");
            ioExecutor.shutdown();
            ioExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            if (!ioExecutor.isTerminated()) {
                output.printWarning("IO EXECUTOR NOT TERMINATED AFTER TERMINATION!");
            }
            logProgress(true);
            output.printStatus("Closing saves...");
            savesToClose.forEach((p, s) -> {
                try {
                    output.printInfo("Closing dimension " + p.getFileName().toString());
                    s.close();
                } catch (IOException e) {
                    output.printError("Error while closing save", e);
                }
            });
            output.printStatus("DONE!");
            output.printInfo("All dimensions closed");
            System.out.println("\n\n");
        } else {
            System.out.println("Unrecognized options:\n\t" + String.join("\n\t", args) + "\n");
            printHelp();
        }

    }

    private void fixWorld(String worldLocation, String outputWorld) throws IOException {
        Path inPath = Paths.get(worldLocation);
        Path outPath = Paths.get(outputWorld);

        String dimName = inPath.getFileName().toString();
        output.printInfo("Scheduling fixes in dimension " + dimName);

        Path part2dIn = inPath.resolve("region2d");
        Files.createDirectories(part2dIn);

        Path part3dIn = inPath.resolve("region3d");
        Files.createDirectories(part3dIn);

        Path part2dOut = outPath.resolve("region2d");
        Files.createDirectories(part2dOut);

        Path part3dOut = outPath.resolve("region3d");
        Files.createDirectories(part3dOut);

        SaveSection3D save3dIn = SaveSection3D.createAt(part3dIn);
        SaveSection2D save2dIn = SaveSection2D.createAt(part2dIn);

        SaveSection3D save3dOut = SaveSection3D.createAt(part3dOut);
        SaveSection2D save2dOut = SaveSection2D.createAt(part2dOut);

        fixSave(save3dIn, save3dOut, save2dIn, save2dOut);

        savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_3d_in"), save3dIn);
        savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_2d_in"), save2dIn);
        savesToClose.put(outPath.resolveSibling(inPath.getFileName() + "_3d_out"), save3dOut);
        savesToClose.put(outPath.resolveSibling(inPath.getFileName() + "_2d_out"), save2dOut);
        try (Stream<Path> stream = Files.list(inPath)) {
            Set<Path> dimensions =
                    stream.filter(p -> p.getFileName().toString().startsWith("DIM") && Files.isDirectory(p)).collect(Collectors.toSet());
            for (Path dim : dimensions) {
                try {
                    fixWorld(dim.toString(), outPath.resolve(inPath.relativize(dim)).toString());
                } catch (Throwable t) {
                    output.printError("Could not fix dimension: " + dim.getFileName().toString(), t);
                }
            }
        }
    }

    private void fixSave(SaveSection3D save3dIn, SaveSection3D save3dOut, SaveSection2D save2dIn, SaveSection2D save2dOut) throws IOException {
        save2dIn.forAllKeys(location -> {
            submittedFix.incrementAndGet();
            fixingExecutor.submit(() -> {
                try {
                    ByteBuffer buf;
                    try {
                        buf = save2dIn.load(location, true).orElse(null);
                    } catch (Throwable t) {
                        output.printError("Error loading column data " + location + ", skipping...", t);
                        return;
                    }
                    if (buf == null) {
                        output.printWarning("Column at " + location + " doesn't have any data! This should not be possible. Skipping...");
                        return;
                    }
                    output.printChunkInfo(() -> "Fixing chunk " + location.getEntryX() + ", " + location.getEntryZ());
                    ByteBuffer newBuf = fix2dBuffer(location, buf);

                    submittedIo.incrementAndGet();
                    ioExecutor.submit(() -> {
                        try {
                            save2dOut.save(location, newBuf);
                        } catch (IOException e) {
                            output.printError("An error occurred while saving column at " + location, e);
                        }
                        saved.incrementAndGet();
                        logProgress(false);
                    });
                } catch (Throwable t) {
                    output.printError("An error occurred while fixing column at " + location, t);
                }
            });
        });
        save3dIn.forAllKeys(location -> {
            submittedFix.incrementAndGet();
            fixingExecutor.submit(() -> {
                try {
                    ByteBuffer buf;
                    try {
                        buf = save3dIn.load(location, true).orElse(null);
                    } catch (Throwable t) {
                        output.printError("Error loading chunk data " + location + ", skipping...", t);
                        return;
                    }
                    if (buf == null) {
                        output.printWarning("Cube at " + location + " doesn't have any data! This should not be possible. Skipping...");
                        return;
                    }

                    output.printChunkInfo(() -> "Fixing chunk " + location.getEntryX() + ", " + location.getEntryY() + ", " + location.getEntryZ());
                    ByteBuffer newBuf = fixBuffer(location, buf);

                    submittedIo.incrementAndGet();
                    ioExecutor.submit(() -> {
                        try {
                            save3dOut.save(location, newBuf);
                        } catch (IOException e) {
                            output.printError("An error occurred while saving chunk at " + location, e);
                        }
                        saved.incrementAndGet();
                        logProgress(false);
                    });
                } catch (Throwable t) {
                    output.printError("An error occurred while fixing chunk at " + location, t);
                }
            });
        });
    }

    private void logProgress(boolean finalPrint) {
        output.printProgress(() -> String.format("\r%d/%d/%d (%.2f%%)",
            saved.get(), submittedIo.get(), submittedFix.get(), 100 * saved.get() / (double) submittedFix.get()), finalPrint);
    }

    private ByteBuffer fixBuffer(EntryLocation3D loc, ByteBuffer buf) throws IOException {
        //lgtm [java/input-resource-leak]
        CompoundTag tag = readCompressed(buf);
        CompoundTag level = tag.getCompound("Level", NULL_TAG);
        if (level == NULL_TAG) {
            output.printWarning("Cube at " + loc + " has no Level tag! Skipping...");
            buf.flip();
            return buf;
        }
        level.putBoolean("isSurfaceTracked", false);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferedOutputStream stream = new BufferedOutputStream(out)) {
            writeCompressed(tag, stream);
        }
        return ByteBuffer.wrap(out.toByteArray());
    }


    private ByteBuffer fix2dBuffer(EntryLocation2D loc, ByteBuffer buf) throws IOException {
        //lgtm [java/input-resource-leak]
        CompoundTag tag = readCompressed(buf);
        CompoundTag level = tag.getCompound("Level", NULL_TAG);
        if (level == NULL_TAG) {
            output.printWarning("Column at " + loc + " has no Level tag! Skipping...");
            buf.flip();
            return buf;
        }
        level.put("OpacityIndex", NULL_OPACITY_INDEX);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferedOutputStream stream = new BufferedOutputStream(out)) {
            writeCompressed(tag, stream);
        }
        return ByteBuffer.wrap(out.toByteArray());
    }

    public static @NonNull CompoundTag readCompressed(final @NonNull ByteBuffer input) throws IOException {
        try (final DataInputStream dis = new DataInputStream(new BufferedInputStream(new GZIPInputStream(new ByteBufferBackedInputStream(input))))) {
            return TagIO.readDataInput(dis);
        }
    }

    public static void writeCompressed(final @NonNull CompoundTag tag, final @NonNull OutputStream output) throws IOException {
        try (final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(output)))) {
            TagIO.writeDataOutput(tag, dos);
        }
    }

    private void printHelp() {
        System.out.println("Usage: java -jar filename.jar options");
        System.out.println("available options are:\n" +
                "   -h --help -?        print this help text\n" +
                "   -w --world file     specify world location (required)");
    }

    public static class ByteBufferBackedInputStream extends InputStream {

        ByteBuffer buf;

        public ByteBufferBackedInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public int read() {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int off, int len) {
            if (!buf.hasRemaining()) {
                return -1;
            }

            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }
    }
}
