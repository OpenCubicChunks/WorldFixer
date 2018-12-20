package io.github.opencubicchunks.worldfixer;

import cubicchunks.regionlib.api.region.IRegion;
import cubicchunks.regionlib.api.region.key.IKey;
import cubicchunks.regionlib.api.region.key.IKeyProvider;
import cubicchunks.regionlib.api.region.key.RegionKey;
import cubicchunks.regionlib.impl.EntryLocation3D;
import cubicchunks.regionlib.impl.save.SaveSection3D;
import cubicchunks.regionlib.lib.ExtRegion;
import cubicchunks.regionlib.lib.Region;
import cubicchunks.regionlib.lib.provider.CachedRegionProvider;
import cubicchunks.regionlib.lib.provider.SimpleRegionProvider;
import cubicchunks.regionlib.util.CheckedConsumer;
import net.kyori.nbt.CompoundTag;
import net.kyori.nbt.TagIO;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    private static final CompoundTag NULL_TAG = new CompoundTag();

    private static final ExecutorService fixingExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    private static final ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
    private static final AtomicInteger submittedFix = new AtomicInteger();
    private static final AtomicInteger submittedIo = new AtomicInteger();
    private static final AtomicInteger saved = new AtomicInteger();
    private static final Map<Path, SaveSection3D> savesToClose = new HashMap<>();

    public static void main(String... args) throws IOException, InterruptedException {
        if (args.length == 0 || args[0].equals("-h") || args[0].equals("--help") || args[0].equals("-?")) {
            printHelp();
            return;
        }
        if (args[0].equals("-w") || args[0].equals("--world")) {
            if (args.length != 2) {
                System.out.println("Expected 2 options but got " + args.length);
                printHelp();
                return;
            }
            fixWorld(args[1]);
            fixingExecutor.shutdown();
            fixingExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            ioExecutor.shutdown();
            ioExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            logProgress();
            System.out.println();
            savesToClose.forEach((p, s) -> {
                try {
                    System.out.println("Closing save " + p.toString());
                    s.close();
                } catch (IOException e) {
                    System.out.println("Error while closing save");
                    e.printStackTrace(System.out);
                }
            });
            System.out.println("Done!");
        } else {
            System.out.println("Unrecognized options:\n\t" + String.join("\n\t", args) + "\n");
            printHelp();
        }

    }

    private static void fixWorld(String worldLocation) throws IOException {
        Path path = Paths.get(worldLocation);
        System.out.println("\nScheduling chunks to fix in dimension in " + path.toAbsolutePath().toString());
        Path part2d = path.resolve("region2d");
        Files.createDirectories(part2d);

        Path part3d = path.resolve("region3d");
        Files.createDirectories(part3d);
        SaveSection3D save = new SaveSection3D(
                new CachedRegionProvider<>(
                        RegionProvider.createDefault(new EntryLocation3D.Provider(), part3d), 256
                ),
                new CachedRegionProvider<>(
                        new RegionProvider<>(new EntryLocation3D.Provider(), part3d,
                                (keyProvider, regionKey) -> new ExtRegion<>(part3d, Collections.emptyList(), keyProvider, regionKey)
                        ), 256
                ));
        fixSave(save);
        savesToClose.put(path, save);
        try (Stream<Path> stream = Files.list(path)) {
            Set<Path> dimensions = stream.filter(p -> p.getFileName().toString().startsWith("DIM") && Files.isDirectory(p)).collect(Collectors.toSet());
            for (Path dim : dimensions) {
                try {
                    fixWorld(dim.toString());
                } catch (Throwable t) {
                    System.out.println("Could not fix dimension at: " + dim.toString());
                    t.printStackTrace(System.out);
                }
            }
        }
    }

    private static void fixSave(SaveSection3D save) throws IOException {
        save.forAllKeys(location -> {
            ByteBuffer buf = save.load(location).orElse(null);
            if (buf == null) {
                return;
            }
            submittedFix.incrementAndGet();
            fixingExecutor.submit(() -> {
                try {
                    ByteBuffer newBuf = fixBuffer(buf);

                    submittedIo.incrementAndGet();
                    ioExecutor.submit(() -> {
                        try {
                            save.save(location, newBuf);

                        } catch (IOException e) {
                            System.out.println("An error occurred while saving chunk at " + location);
                            e.printStackTrace(System.out);
                        }
                        if (saved.incrementAndGet() % 256 == 0) {
                            logProgress();
                        }
                    });
                } catch (Throwable t) {
                    System.out.println("An error occurred while fixing a chunk at " + location);
                    t.printStackTrace(System.out);
                }
            });
        });
    }

    private static void logProgress() {
        System.out.print(String.format("\r%d/%d/%d (%.2f%%)",
                saved.get(), submittedIo.get(), submittedFix.get(), 100*saved.get() / (double) submittedFix.get()));
    }

    private static ByteBuffer fixBuffer(ByteBuffer buf) throws IOException {
        CompoundTag tag = TagIO.readCompressedInputStream(new BufferedInputStream(new ByteBufferBackedInputStream(buf)));
        tag.getCompound("Level", NULL_TAG).putBoolean("isSurfaceTracked", false);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferedOutputStream stream = new BufferedOutputStream(out)) {
            TagIO.writeCompressedOutputStream(tag, stream);
        }
        return ByteBuffer.wrap(out.toByteArray());
    }

    private static void printHelp() {
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

        public int read() throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get() & 0xFF;
        }

        public int read(byte[] bytes, int off, int len)
                throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }

            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }
    }

    private static class RegionProvider<K extends IKey<K>> extends SimpleRegionProvider<K> {

        private final IKeyProvider<K> prov;

        public RegionProvider(IKeyProvider<K> keyProvider, Path directory, RegionFactory<K> regionBuilder) {
            super(keyProvider, directory, regionBuilder);
            this.prov = keyProvider;
        }

        @Override
        public void forAllRegions(CheckedConsumer<? super IRegion<K>, IOException> consumer) throws IOException {
            Iterator<Path> it = allRegions();
            while (it.hasNext()) {
                Path path = it.next();
                K key;
                // TODO: temporary hack, this needs redesign of RegionLib
                try {
                    key = prov.fromRegionAndId(new RegionKey(path.getFileName().toString()), 0);
                } catch (RuntimeException ex) {
                    continue;
                }
                IRegion<K> r = getExistingRegion(key).orElse(null);
                if (r != null) {
                    consumer.accept(r);
                    r.close();
                }
            }
        }

        public static <K extends IKey<K>> RegionProvider<K> createDefault(IKeyProvider<K> keyProvider, Path directory) {
            return new RegionProvider<K>(keyProvider, directory, (keyProv, r) ->
                    new Region.Builder<K>()
                            .setDirectory(directory)
                            .setRegionKey(r)
                            .setKeyProvider(keyProv)
                            .setSectorSize(512)
                            .build()
            );
        }
    }
}
