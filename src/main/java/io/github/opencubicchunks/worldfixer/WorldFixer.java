package io.github.opencubicchunks.worldfixer;

import static org.fusesource.jansi.Ansi.ansi;

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
import org.fusesource.jansi.Ansi;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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


    private static final int THREADS = Runtime.getRuntime().availableProcessors() + 1;
    private final ExecutorService fixingExecutor =
        new ThreadPoolExecutor(THREADS, THREADS, 1000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1024 * 1024));
    private final ExecutorService ioExecutor = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(256 * 1024));
    private final AtomicInteger totalCount = new AtomicInteger();
    private final AtomicInteger submittedFix = new AtomicInteger();
    private final AtomicInteger submittedIo = new AtomicInteger();
    private final AtomicInteger saved = new AtomicInteger();
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
        ((ThreadPoolExecutor) fixingExecutor).setRejectedExecutionHandler(handler);
        ((ThreadPoolExecutor) ioExecutor).setRejectedExecutionHandler(handler);
    }

    public void fixWorld(String world, String outputWorld, StatusHandler statusHandler) throws IOException,
        InterruptedException {
        System.out.print(ansi().eraseScreen(Ansi.Erase.ALL).cursor(0, 0));

        statusHandler.status("Scanning worlds (counting chunks)...");
        Path path = Paths.get(world).toAbsolutePath();
        Thread counting = new Thread(() -> {
            try {
                countChunks(path.toString(), statusHandler);
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
        beginFixing(path.toString(), outputWorld, statusHandler);

        statusHandler.status("Waiting for chunk fixes...");
        statusHandler.info("");

        fixingExecutor.shutdown();
        fixingExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        if (!fixingExecutor.isTerminated()) {
            statusHandler.warning("FIXING EXECUTOR NOT TERMINATED AFTER TERMINATION!");
        }
        statusHandler.status("Waiting to write save changes...");
        ioExecutor.shutdown();
        ioExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        if (!ioExecutor.isTerminated()) {
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

    private void countChunks(String worldLocation, StatusHandler statusHandler) throws IOException {
        Path inPath = Paths.get(worldLocation);

        String dimName = inPath.getFileName().toString();
        statusHandler.info("Scheduling fixes in dimension " + dimName);

        Path part2dIn = inPath.resolve("region2d");
        Files.createDirectories(part2dIn);

        Path part3dIn = inPath.resolve("region3d");
        Files.createDirectories(part3dIn);

        SaveSection3D save3dIn = new SaveSection3D(
                new RegionCache<>(
                        SimpleRegionProvider.createDefault(new EntryLocation3D.Provider(), part3dIn, 512)
                ),
                new RegionCache<>(
                        new SimpleRegionProvider<>(new EntryLocation3D.Provider(), part3dIn,
                                (keyProvider, regionKey) -> new ExtRegion<>(part3dIn, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        )
                ));
        SaveSection2D save2dIn = new SaveSection2D(
                new RegionCache<>(
                        SimpleRegionProvider.createDefault(new EntryLocation2D.Provider(), part2dIn, 512)
                ),
                new RegionCache<>(
                        new SimpleRegionProvider<>(new EntryLocation2D.Provider(), part2dIn,
                                (keyProvider, regionKey) -> new ExtRegion<>(part2dIn, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        )
                ));

        doCountChunks(save3dIn, save2dIn, statusHandler);

        savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_3d_in"), save3dIn);
        savesToClose.put(inPath.resolveSibling(inPath.getFileName() + "_2d_in"), save2dIn);
        try (Stream<Path> stream = Files.list(inPath)) {
            Set<Path> dimensions =
                stream.filter(p -> p.getFileName().toString().startsWith("DIM") && Files.isDirectory(p)).collect(Collectors.toSet());
            for (Path dim : dimensions) {
                try {
                    countChunks(dim.toString(), statusHandler);
                } catch (Throwable t) {
                    statusHandler.error("Could not count chunks in dimension: " + dim.getFileName().toString(), t);
                }
            }
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

    private void beginFixing(String worldLocation, String statusHandlerWorld, StatusHandler statusHandler) throws IOException {
        Path inPath = Paths.get(worldLocation);
        Path outPath = Paths.get(statusHandlerWorld);

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

        SaveSection3D save3dIn = new SaveSection3D(
                new RegionCache<>(
                        SimpleRegionProvider.createDefault(new EntryLocation3D.Provider(), part3dIn, 512)
                ),
                new RegionCache<>(
                        new SimpleRegionProvider<>(new EntryLocation3D.Provider(), part3dIn,
                                (keyProvider, regionKey) -> new ExtRegion<>(part3dIn, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        )
                ));
        SaveSection2D save2dIn = new SaveSection2D(
                new RegionCache<>(
                        SimpleRegionProvider.createDefault(new EntryLocation2D.Provider(), part2dIn, 512)
                ),
                new RegionCache<>(
                        new SimpleRegionProvider<>(new EntryLocation2D.Provider(), part2dIn,
                                (keyProvider, regionKey) -> new ExtRegion<>(part2dIn, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        )
                ));

        SaveSection3D save3dOut = new SaveSection3D(
                new RegionCache<>(
                        SimpleRegionProvider.createDefault(new EntryLocation3D.Provider(), part3dOut, 512)
                ),
                new RegionCache<>(
                        new SimpleRegionProvider<>(new EntryLocation3D.Provider(), part3dOut,
                                (keyProvider, regionKey) -> new ExtRegion<>(part3dOut, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        )
                ));
        SaveSection2D save2dOut = new SaveSection2D(
                new RegionCache<>(
                        SimpleRegionProvider.createDefault(new EntryLocation2D.Provider(), part2dOut, 512)
                ),
                new RegionCache<>(
                        new SimpleRegionProvider<>(new EntryLocation2D.Provider(), part2dOut,
                                (keyProvider, regionKey) -> new ExtRegion<>(part2dOut, Collections.emptyList(), keyProvider, regionKey),
                                (dir, key) -> Files.exists(dir.resolve(key.getRegionKey().getName() + ".ext"))
                        )
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
                    beginFixing(dim.toString(), outPath.resolve(inPath.relativize(dim)).toString(), statusHandler);
                } catch (Throwable t) {
                    statusHandler.error("Could not fix dimension: " + dim.getFileName().toString(), t);
                }
            }
        }
    }

    private void fixSave(SaveSection3D save3dIn, SaveSection3D save3dOut, SaveSection2D save2dIn, SaveSection2D save2dOut, StatusHandler statusHandler) throws IOException {
        save2dIn.forAllKeys(location -> {
            submittedFix.incrementAndGet();
            fixingExecutor.submit(() -> {
                try {
                    ByteBuffer buf;
                    try {
                        buf = save2dIn.load(location, true).orElse(null);
                    } catch (Throwable t) {
                        statusHandler.error("Error loading column data " + location + ", skipping...", t);
                        return;
                    }
                    if (buf == null) {
                        statusHandler.warning("Column at " + location + " doesn't have any data! This should not be possible. Skipping...");
                        return;
                    }
                    statusHandler.chunkInfo(() -> "Fixing chunk " + location.getEntryX() + ", " + location.getEntryZ());
                    ByteBuffer newBuf = fix2dBuffer(statusHandler, location, buf);
                    if (newBuf == null) {
                        return;
                    }
                    submittedIo.incrementAndGet();
                    ioExecutor.submit(() -> {
                        try {
                            save2dOut.save(location, newBuf);
                        } catch (IOException e) {
                            statusHandler.error("An error occurred while saving column at " + location, e);
                        }
                        saved.incrementAndGet();
                        showProgress(statusHandler, false);
                    });
                } catch (Throwable t) {
                    statusHandler.error("An error occurred while fixing column at " + location, t);
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
                        statusHandler.error("Error loading chunk data " + location + ", skipping...", t);
                        return;
                    }
                    if (buf == null) {
                        statusHandler.warning("Cube at " + location + " doesn't have any data! This should not be possible. Skipping...");
                        return;
                    }

                    statusHandler.chunkInfo(() -> "Fixing chunk " + location.getEntryX() + ", " + location.getEntryY() + ", " + location.getEntryZ());
                    ByteBuffer newBuf = fixBuffer(statusHandler, location, buf);
                    if (newBuf == null) {
                        return;
                    }
                    submittedIo.incrementAndGet();
                    ioExecutor.submit(() -> {
                        try {
                            save3dOut.save(location, newBuf);
                        } catch (IOException e) {
                            statusHandler.error("An error occurred while saving chunk at " + location, e);
                        }
                        saved.incrementAndGet();
                        showProgress(statusHandler, false);
                    });
                } catch (Throwable t) {
                    statusHandler.error("An error occurred while fixing chunk at " + location, t);
                }
            });
        });
    }

    private void showProgress(StatusHandler statusHandler, boolean isDone) {
        statusHandler.progress(() -> isDone ? 1.0 : (saved.get() / (double) totalCount.get()), () -> isDone ? "DONE" : String.format("%d/%d/%d/%d (%.2f%%)",
            saved.get(), submittedIo.get(), submittedFix.get(), totalCount.get(), 100 * saved.get() / (double) totalCount.get()), isDone);
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
