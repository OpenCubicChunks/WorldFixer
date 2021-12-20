package io.github.opencubicchunks.worldfixer;

import cubicchunks.regionlib.api.region.key.IKey;
import cubicchunks.regionlib.api.storage.SaveSection;
import cubicchunks.regionlib.impl.EntryLocation2D;
import cubicchunks.regionlib.impl.EntryLocation3D;
import cubicchunks.regionlib.impl.save.SaveSection2D;
import cubicchunks.regionlib.impl.save.SaveSection3D;
import cubicchunks.regionlib.lib.ExtRegion;
import cubicchunks.regionlib.lib.provider.SimpleRegionProvider;
import net.kyori.nbt.ByteArrayTag;
import net.kyori.nbt.CompoundTag;
import net.kyori.nbt.TagIO;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.fusesource.jansi.Ansi;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.fusesource.jansi.Ansi.ansi;

public class WorldFixer {
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

    public static final int PROCESSOR_COUNT = Integer.parseInt(System.getProperty("worldfixer.processorCount",
            Runtime.getRuntime().availableProcessors() + ""));
    private static final int READ_THREADS = Integer.parseInt(System.getProperty("worldfixer.readThreads", "1"));
    private static final int FIX_THREADS = Integer.parseInt(System.getProperty("worldfixer.fixThreads",
            (PROCESSOR_COUNT + 1) + ""));
    private static final int WRITE_THREADS = Integer.parseInt(System.getProperty("worldfixer.writeThreads", "1"));
    private static final int CLOSE_THREADS = Integer.parseInt(System.getProperty("worldfixer.closeThreads", "2"));
    private static final int READ_QUEUE = Integer.parseInt(System.getProperty("worldfixer.readQueue",
            (1024 * 128 * PROCESSOR_COUNT) + ""));
    private static final int FIX_QUEUE = Integer.parseInt(System.getProperty("worldfixer.fixQueue",
            (1024 * 128 * PROCESSOR_COUNT) + ""));
    private static final int WRITE_QUEUE = Integer.parseInt(System.getProperty("worldfixer.writeQueue",
            (1024 * 1024 * PROCESSOR_COUNT) + ""));

    private final ArrayBlockingQueue<Runnable> readerQueue = new ArrayBlockingQueue<>(READ_QUEUE);
    private final ArrayBlockingQueue<Runnable> fixingQueue = new ArrayBlockingQueue<>(FIX_QUEUE);
    private final ArrayBlockingQueue<Runnable> writeQueue = new ArrayBlockingQueue<>(WRITE_QUEUE);
    private final AtomicInteger readThreads = new AtomicInteger(), fixThreads = new AtomicInteger(), writeThreads = new AtomicInteger();
    private final ExecutorService readExecutor = new ThreadPoolExecutor(
            READ_THREADS, READ_THREADS,
            1000L, TimeUnit.MILLISECONDS, readerQueue,
            runnable -> {
                Thread t = new Thread(runnable);
                t.setName("Reader thread " + readThreads.incrementAndGet());
                return t;
            });
    private final ExecutorService fixingExecutor = new ThreadPoolExecutor(
            FIX_THREADS, FIX_THREADS,
            1000L, TimeUnit.MILLISECONDS, fixingQueue,
            runnable -> {
                Thread t = new Thread(runnable);
                t.setName("Fix thread " + fixThreads.incrementAndGet());
                return t;
            });
    private final ExecutorService writeExecutor = new ThreadPoolExecutor(
            WRITE_THREADS, WRITE_THREADS,
            1000L, TimeUnit.MILLISECONDS, writeQueue,
            runnable -> {
                Thread t = new Thread(runnable);
                t.setName("Write thread " + writeThreads.incrementAndGet());
                return t;
            });
    private final ExecutorService closingExecutor = new ThreadPoolExecutor(
            CLOSE_THREADS, CLOSE_THREADS,
            1000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            runnable -> {
                Thread t = new Thread(runnable);
                t.setName("Closing thread " + writeThreads.incrementAndGet());
                return t;
            });
    private final AtomicInteger totalCount = new AtomicInteger();
    private final AtomicInteger readingCount = new AtomicInteger();
    private final AtomicInteger fixingCount = new AtomicInteger();
    private final AtomicInteger writingCount = new AtomicInteger();
    private final AtomicInteger doneCount = new AtomicInteger();
    private final Map<Path, SaveSection<?, ?>> savesToClose = new HashMap<>();

    {
        RejectedExecutionHandler handler = (r, executor) -> {
            try {
                if (!executor.isShutdown()) {
                    executor.getQueue().put(r);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("Executor was interrupted while the task was waiting to put on work queue", e);
            }
        };
        ((ThreadPoolExecutor) readExecutor).setRejectedExecutionHandler(handler);
        ((ThreadPoolExecutor) fixingExecutor).setRejectedExecutionHandler(handler);
        ((ThreadPoolExecutor) writeExecutor).setRejectedExecutionHandler(handler);
    }

    public void fixWorld(String world, String outputWorld, StatusHandler statusHandler) throws IOException,
        InterruptedException {

        long start = System.nanoTime();
        try {
            Path inWorld, outWorld;
            FileSystem inFs = null, outFs = null;
            if (world.endsWith(".zip")) {
                inFs = FileSystems.newFileSystem(Paths.get(world), getClass().getClassLoader());
                inWorld = findWorldIn(inFs.getPath("/"));
            } else {
                inWorld = Paths.get(world);
            }
            if (outputWorld.endsWith(".zip")) {
                outFs = createZipFs(Paths.get(outputWorld));
                outWorld = outFs.getPath("/");
            } else {
                outWorld = Paths.get(outputWorld);
            }

            try (FileSystem ignored = inFs; FileSystem ignored1 = outFs) {
                fixWorld(inWorld, outWorld, statusHandler);
            }
        } finally {
            statusHandler.info("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) * 0.001 + " seconds!");
        }
    }

    @Nullable private Path findWorldIn(Path path) throws IOException {
        if (Files.exists(path.resolve("level.dat"))) {
            return path;
        }
        try (Stream<Path> stream = Files.list(path)) {
            for (Path o : stream.toArray(Path[]::new)) {
                Path worldPath = findWorldIn(o);
                if (worldPath != null) {
                    return worldPath;
                }
            }
            return null;
        }
    }

    private FileSystem createZipFs(Path path) throws IOException {
        Map<String, String> env = new HashMap<>();
        env.put("create", "true");
        URI uri = URI.create("jar:" + path.toUri());
        return FileSystems.newFileSystem(uri, env);
    }

    private void fixWorld(Path world, Path outputWorld, StatusHandler statusHandler) throws IOException,
            InterruptedException {
        System.out.print(ansi().eraseScreen(Ansi.Erase.ALL).cursor(0, 0));

        statusHandler.status("Scanning worlds (counting chunks)...");

        Thread counting = new Thread(() -> {
            try {
                countChunks(world, statusHandler);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }, "Chunk Counting");
        counting.start();

        savesToClose.forEach((p, s) -> {
            try {
                statusHandler.info("Closing dimension " + p.getFileName().toString());
                s.close();
            } catch (IOException e) {
                statusHandler.error("Error while closing save", e);
            }
        });
        statusHandler.status("Scanning worlds (copying chunks)...");
        beginFixing(world, outputWorld, statusHandler);

        statusHandler.status("Waiting for chunk fixes...");
        statusHandler.info("");

        readExecutor.shutdown();
        readExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        if (!readExecutor.isTerminated()) {
            statusHandler.warning("READING EXECUTOR NOT TERMINATED AFTER TERMINATION!");
        }
        fixingExecutor.shutdown();
        fixingExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        if (!fixingExecutor.isTerminated()) {
            statusHandler.warning("FIXING EXECUTOR NOT TERMINATED AFTER TERMINATION!");
        }
        statusHandler.status("Waiting to write save changes...");
        writeExecutor.shutdown();
        writeExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        if (!writeExecutor.isTerminated()) {
            statusHandler.warning("IO EXECUTOR NOT TERMINATED AFTER TERMINATION!");
        }
        showProgress(statusHandler, true);
        counting.join();
        statusHandler.status("Closing saves...");
        savesToClose.forEach((p, s) -> {
            try {
                statusHandler.info("Closing dimension " + p.getFileName().toString());
                s.close();
            } catch (IOException e) {
                statusHandler.error("Error while closing save", e);
            }
        });
        statusHandler.status("DONE!");
        statusHandler.info("All dimensions closed");
        System.out.println("\n\n");
    }

    private void countChunks(Path inPath, StatusHandler statusHandler) throws IOException {
        try {
            String dimName = inPath.getFileName().toString();
            statusHandler.info("Scheduling fixes in dimension " + dimName);

            Path part2dIn = inPath.resolve("region2d");
            Files.createDirectories(part2dIn);

            Path part3dIn = inPath.resolve("region3d");
            Files.createDirectories(part3dIn);

            SaveSection3D save3dIn =
                    new SaveSection3D(
                            new SimpleRegionProvider<>(
                                    new EntryLocation3D.Provider(),
                                    part3dIn,
                                    (keyProv, r) -> MemoryReadRegion.<EntryLocation3D>builder()
                                            .setDirectory(part3dIn)
                                            .setRegionKey(r)
                                            .setKeyProvider(keyProv)
                                            .setSectorSize(512)
                                            .build(),
                                    (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName()))
                            ),
                            new SimpleRegionProvider<>(new EntryLocation3D.Provider(), part3dIn,
                                    (keyProvider, regionKey) -> new ExtRegion<>(part3dIn, Collections.emptyList(), keyProvider, regionKey),
                                    (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                            )
                    );
            SaveSection2D save2dIn = new SaveSection2D(
                    new SimpleRegionProvider<>(
                            new EntryLocation2D.Provider(),
                            part2dIn,
                            (keyProv, r) -> MemoryReadRegion.<EntryLocation2D>builder()
                                    .setDirectory(part2dIn)
                                    .setRegionKey(r)
                                    .setKeyProvider(keyProv)
                                    .setSectorSize(512)
                                    .build(),
                            (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName()))
                    ),
                    new SimpleRegionProvider<>(new EntryLocation2D.Provider(), part2dIn,
                            (keyProvider, regionKey) -> new ExtRegion<>(part2dIn, Collections.emptyList(), keyProvider, regionKey),
                            (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                    )
            );


            doCountChunks(save3dIn, save2dIn, statusHandler);

            savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_3d_in"), save3dIn);
            savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_2d_in"), save2dIn);
            try (Stream<Path> stream = Files.list(inPath)) {
                Set<Path> dimensions =
                        stream.filter(p -> p.getFileName().toString().startsWith("DIM") && Files.isDirectory(p)).collect(Collectors.toSet());
                for (Path dim : dimensions) {
                    countChunks(dim, statusHandler);
                }
            }
        } catch (Throwable t) {
            statusHandler.error("Could not count chunks in dimension: " + inPath, t);
        }
    }

    private void doCountChunks(SaveSection3D save3dIn, SaveSection2D save2dIn, StatusHandler statusHandler) throws IOException {
        save2dIn.forAllKeys(location -> {
            if ((totalCount.incrementAndGet() & 4096) == 0) {
                statusHandler.chunkInfo(() -> "Counting: " + totalCount.get());
            }

        });
        save3dIn.forAllKeys(location -> {
            if ((totalCount.incrementAndGet() & 4096) == 0) {
                statusHandler.chunkInfo(() -> "Counting: " + totalCount.get());
            }
        });
    }

    private void beginFixing(Path inPath, Path outPath, StatusHandler statusHandler) throws IOException {
        String dimName = inPath.getFileName().toString();
        statusHandler.info("Scheduling fixes in dimension " + dimName);

        Path part2dIn = inPath.resolve("region2d");
        Files.createDirectories(part2dIn);

        Path part3dIn = inPath.resolve("region3d");
        Files.createDirectories(part3dIn);

        Path part2dOut = outPath.resolve("region2d");
        Files.createDirectories(part2dOut);

        Path part3dOut = outPath.resolve("region3d");
        Files.createDirectories(part3dOut);

        SaveSection3D save3dIn =
                new SaveSection3D(
                        new RWLockingCachedRegionProvider<>(
                                new SimpleRegionProvider<>(
                                        new EntryLocation3D.Provider(),
                                        part3dIn,
                                        (keyProv, r) -> MemoryReadRegion.<EntryLocation3D>builder()
                                                .setDirectory(part3dIn)
                                                .setRegionKey(r)
                                                .setKeyProvider(keyProv)
                                                .setSectorSize(512)
                                                .build(),
                                        (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName()))
                                ), closingExecutor
                        ),
                        new RWLockingCachedRegionProvider<>(
                                new SimpleRegionProvider<>(new EntryLocation3D.Provider(), part3dIn,
                                        (keyProvider, regionKey) -> new ExtRegion<>(part3dIn, Collections.emptyList(), keyProvider, regionKey),
                                        (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                                ), closingExecutor
                        ));
        SaveSection2D save2dIn = new SaveSection2D(
                new RWLockingCachedRegionProvider<>(
                        new SimpleRegionProvider<>(
                                new EntryLocation2D.Provider(),
                                part2dIn,
                                (keyProv, r) -> MemoryReadRegion.<EntryLocation2D>builder()
                                        .setDirectory(part2dIn)
                                        .setRegionKey(r)
                                        .setKeyProvider(keyProv)
                                        .setSectorSize(512)
                                        .build(),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName()))
                        ), closingExecutor
                ),
                new RWLockingCachedRegionProvider<>(
                        new SimpleRegionProvider<>(new EntryLocation2D.Provider(), part2dIn,
                                (keyProvider, regionKey) -> new ExtRegion<>(part2dIn, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        ), closingExecutor
                ));

        SaveSection3D save3dOut = new SaveSection3D(
                new RWLockingCachedRegionProvider<>(
                        new SimpleRegionProvider<>(
                                new EntryLocation3D.Provider(),
                                part3dOut,
                                (keyProv, r) -> MemoryWriteRegion.<EntryLocation3D>builder()
                                        .setDirectory(part3dOut)
                                        .setRegionKey(r)
                                        .setKeyProvider(keyProv)
                                        .setSectorSize(512)
                                        .build(),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName()))
                        ), closingExecutor
                ),
                new RWLockingCachedRegionProvider<>(
                        new SimpleRegionProvider<>(new EntryLocation3D.Provider(), part3dOut,
                                (keyProvider, regionKey) -> new ExtRegion<>(part3dOut, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        ), closingExecutor
                ));
        SaveSection2D save2dOut = new SaveSection2D(
                new RWLockingCachedRegionProvider<>(
                        new SimpleRegionProvider<>(
                                new EntryLocation2D.Provider(),
                                part2dOut,
                                (keyProv, r) -> MemoryWriteRegion.<EntryLocation2D>builder()
                                        .setDirectory(part2dOut)
                                        .setRegionKey(r)
                                        .setKeyProvider(keyProv)
                                        .setSectorSize(512)
                                        .build(),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName()))
                        ), closingExecutor
                ),
                new RWLockingCachedRegionProvider<>(
                        new SimpleRegionProvider<>(new EntryLocation2D.Provider(), part2dOut,
                                (keyProvider, regionKey) -> new ExtRegion<>(part2dOut, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        ), closingExecutor
                ));

        fixSave(save3dIn, save3dOut, save2dIn, save2dOut, statusHandler);

        savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_3d_in"), save3dIn);
        savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_2d_in"), save2dIn);
        savesToClose.put(outPath.resolveSibling(inPath.getFileName() + "_3d_out"), save3dOut);
        savesToClose.put(outPath.resolveSibling(inPath.getFileName() + "_2d_out"), save2dOut);
        try (Stream<Path> stream = Files.list(inPath)) {
            Set<Path> dimensions =
                stream.filter(p -> p.getFileName().toString().startsWith("DIM") && Files.isDirectory(p)).collect(Collectors.toSet());
            for (Path dim : dimensions) {
                try {
                    // workaround for https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8146754
                    Path dimNoSlash = Utils.removeTrailingSlash(dim);
                    Path inPathNoSlash = Utils.removeTrailingSlash(inPath);
                    beginFixing(dim, outPath.resolve(inPathNoSlash.relativize(dimNoSlash).toString()), statusHandler);
                } catch (Throwable t) {
                    statusHandler.error("Could not fix dimension: " + dim.getFileName().toString(), t);
                }
            }
        }
    }

    private void fixSave(SaveSection3D save3dIn, SaveSection3D save3dOut, SaveSection2D save2dIn, SaveSection2D save2dOut, StatusHandler statusHandler) throws IOException {
        save2dIn.forAllKeys(location -> CompletableFuture
                .supplyAsync(() -> readValue(save2dIn, statusHandler, location), readExecutor)
                .thenApplyAsync(buf -> fixBuffer2D(statusHandler, location, buf), fixingExecutor)
                .thenAcceptAsync(buf -> writeBuffer(save2dOut, statusHandler, location, buf), writeExecutor)
        );
        save3dIn.forAllKeys(location -> CompletableFuture
                .supplyAsync(() -> readValue(save3dIn, statusHandler, location), readExecutor)
                .thenApplyAsync(buf -> fixBuffer3D(statusHandler, location, buf), fixingExecutor)
                .thenAcceptAsync(buf -> writeBuffer(save3dOut, statusHandler, location, buf), writeExecutor)
        );
    }

    private <S extends SaveSection<S, T>, T extends IKey<T>> ByteBuffer readValue(S save, StatusHandler statusHandler, T location) {
        readingCount.incrementAndGet();
        ByteBuffer buf;
        try {
            buf = save.load(location, true).orElse(null);
        } catch (Throwable t) {
            statusHandler.error("Error loading entry data " + location + ", skipping...", t);
            return null;
        }
        if (buf == null) {
            statusHandler.warning("Entry at " + location + " doesn't have any data! This should not be possible. Skipping...");
            return null;
        }
        return buf;
    }

    private ByteBuffer fixBuffer2D(StatusHandler statusHandler, EntryLocation2D location, ByteBuffer buf) {
        fixingCount.incrementAndGet();
        statusHandler.chunkInfo(() -> "Fixing column " + location.getEntryX() + ", " + location.getEntryZ());
        try {
            if (buf == null) {
                return null;
            }
            return fix2dBuffer(statusHandler, location, buf);
        } catch (Throwable t) {
            statusHandler.error("An error occurred while fixing column at " + location, t);
            return null;
        }
    }

    private ByteBuffer fixBuffer3D(StatusHandler statusHandler, EntryLocation3D location, ByteBuffer buf) {
        fixingCount.incrementAndGet();
        statusHandler.chunkInfo(() -> "Fixing cube " + location.getEntryX() + ", " + location.getEntryY() + ", " + location.getEntryZ());
        try {
            if (buf == null) {
                return null;
            }
            return fixBuffer(statusHandler, location, buf);
        } catch (Throwable t) {
            statusHandler.error("An error occurred while fixing cube at " + location, t);
            return null;
        }
    }

    private <S extends SaveSection<S, T>, T extends IKey<T>> void writeBuffer(S save2dOut, StatusHandler statusHandler, T location, ByteBuffer buf) {
        writingCount.incrementAndGet();
        try {
            if (buf == null) {
                return;
            }
            save2dOut.save(location, buf);
        } catch (IOException e) {
            statusHandler.error("An error occurred while saving entry at " + location, e);
        } finally {
            doneCount.incrementAndGet();
            showProgress(statusHandler, false);
        }
    }

    private void showProgress(StatusHandler statusHandler, boolean isDone) {
        statusHandler.progress(
                () -> isDone ? 1.0 : (doneCount.get() / (double) totalCount.get()),
                () -> isDone ? "DONE" : String.format("%d/%d (%.2f%%) [r=%d|f=%d|w=%d|rq=%d|fq=%d|wq=%d]",
                        doneCount.get(),
                        totalCount.get(),
                        100 * doneCount.get() / (double) totalCount.get(),

                        readingCount.get(),
                        fixingCount.get(),
                        writingCount.get(),
                        readerQueue.size(),
                        fixingQueue.size(),
                        writeQueue.size()
                ),
                isDone);
    }

    private ByteBuffer fixBuffer(StatusHandler statusHandler, EntryLocation3D loc, ByteBuffer buf) throws IOException {
        //lgtm [java/input-resource-leak]
        CompoundTag tag = readCompressed(buf);

        CompoundTag level = tag.getCompound("Level", NULL_TAG);
        if (level == NULL_TAG) {
            statusHandler.warning("Cube at " + loc + " has no Level tag! Skipping...");
            return null;
        }
        if (level.getByte("v", (byte) 0) != 1) {
            statusHandler.warning("Cube at " + loc + " has version " + level.getInt("v", 0) + " but expected 1, skipping...");
            return null;
        }
        level.putBoolean("isSurfaceTracked", false);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferedOutputStream stream = new BufferedOutputStream(out)) {
            writeCompressed(tag, stream);
        }
        return ByteBuffer.wrap(out.toByteArray());
    }


    private ByteBuffer fix2dBuffer(StatusHandler statusHandler, EntryLocation2D loc, ByteBuffer buf) throws IOException {
        //lgtm [java/input-resource-leak]
        CompoundTag tag = readCompressed(buf);
        CompoundTag level = tag.getCompound("Level", NULL_TAG);
        if (level == NULL_TAG) {
            statusHandler.warning("Column at " + loc + " has no Level tag! Skipping...");
            return null;
        }
        if (level.getByte("v", (byte) 0) != 1) {
            statusHandler.warning("Column at " + loc + " has version " + level.getInt("v", 0) + " but expected 1, skipping...");
            return null;
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

    public static void writeCompressed(final @NonNull CompoundTag tag, final @NonNull OutputStream statusHandler) throws IOException {
        try (final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(statusHandler)))) {
            TagIO.writeDataOutput(tag, dos);
        }
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
