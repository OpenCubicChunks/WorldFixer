/*
 *  This file is part of CubicChunksConverter, licensed under the MIT License (MIT).
 *
 *  Copyright (c) 2017 contributors
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
package io.github.opencubicchunks.worldfixer;

import cubicchunks.regionlib.util.CheckedConsumer;
import net.kyori.nbt.CompoundTag;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class Utils {

    @SuppressWarnings("unchecked")
    private static <T, E extends Throwable> T throwUnchecked(Throwable t) throws E {
        throw (E) t;
    }

    // Files.createDirectories doesn't handle symlinks
    public static void createDirectories(Path dir) throws IOException {
        if (Files.isDirectory(dir)) {
            return;
        }
        createDirectories(dir.getParent());
        try {
            Files.createDirectory(dir);
        } catch (FileAlreadyExistsException ex) {}
    }

    public static boolean isValidPath(String text) {
        try {
            Files.exists(Paths.get(text));
            return true;
        } catch (InvalidPathException e) {
            return false;
        }
    }

    public static boolean fileExists(String text) {
        try {
            return Files.exists(Paths.get(text));
        } catch (InvalidPathException e) {
            return false;
        }
    }

    public static int countFiles(Path f) throws IOException {
        try {
            return countFiles_do(f);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static int countFiles_do(Path f) {
        if (Files.isRegularFile(f)) {
            return 1;
        } else if (!Files.isDirectory(f)) {
            throw new UnsupportedOperationException();
        }

        try {
            return Files.list(f).map(p -> countFiles_do(p)).reduce((x, y) -> x + y).orElse(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void copyEverythingExcept(Path file, Path srcDir, Path dstDir, Predicate<Path> excluded, Consumer<Path> onCopy) throws IOException {
        try {
            Files.list(file).forEach(f -> {
                if (!excluded.test(f)) {
                    try {
                        copyFile(f, srcDir, dstDir);
                        if (Files.isRegularFile(f)) {
                            onCopy.accept(f);
                        }
                        if (Files.isDirectory(f)) {
                            copyEverythingExcept(f, srcDir, dstDir, excluded, onCopy);
                        } else if (!Files.isRegularFile(f)) {
                            throw new UnsupportedOperationException();
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    public static void copyFile(Path srcFile, Path srcDir, Path dstDir) throws IOException {
        Path relative = srcDir.relativize(srcFile);//
        Path dstFile = dstDir.resolve(relative);

        if (Files.exists(dstFile) && Files.isDirectory(dstFile)) {
            return; // already exists, stop here to avoid DirectoryNotEmptyException
        }
        Files.createDirectories(dstFile.getParent());
        Files.copy(srcFile, dstFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
    }

    /**
     * Deletes the specified file or directory, recursively
     */
    public static void rm(Path toDelete) throws IOException {
        if (Files.isDirectory(toDelete)) {
            try (Stream<Path> files = Files.list(toDelete)) {
                for (Path path : files.collect(Collectors.toList())) {
                    rm(path);
                }
            }
            Files.delete(toDelete);
        } else {
            Files.delete(toDelete);
        }
    }

    public static boolean isEmpty(final Path dirOrZip) throws IOException {
        if (Files.exists(dirOrZip) && dirOrZip.getFileName().toString().endsWith(".zip")) {
            return false; // existing zip = not empty
        }
        if (!Files.exists(dirOrZip)) {
            return true;
        }
        try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(dirOrZip)) {
            return !dirStream.iterator().hasNext();
        }
    }

    public static <E extends Throwable> void forEachDirectory(Path directory, CheckedConsumer<Path, E> consumer) throws E, IOException {
        try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            for (Path path : dirStream) {
                if (Files.isDirectory(path)) {
                    consumer.accept(path);
                }
            }
        }
    }

    public static Path removeTrailingSlash(Path path) {
        String pathStr = path.toString();
        if (pathStr.endsWith("/")) {
            pathStr = pathStr.substring(0, pathStr.length() - 1);
        }
        return path.getFileSystem().getPath(pathStr);
    }

    private enum OS {
        WINDOWS, MACOS, SOLARIS, LINUX, UNKNOWN;
    }

    private static OS getPlatform() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("win")) {
            return OS.WINDOWS;
        }
        if (osName.contains("mac")) {
            return OS.MACOS;
        }
        if (osName.contains("linux")) {
            return OS.LINUX;
        }
        if (osName.contains("unix")) {
            return OS.LINUX;
        }
        return OS.UNKNOWN;
    }

    public static Path getApplicationDirectory() {
        String userHome = System.getProperty("user.home", ".");
        Path workingDirectory;
        switch (getPlatform()) {
            case LINUX:
            case SOLARIS:
                workingDirectory = Paths.get(userHome, ".minecraft/");
                break;
            case WINDOWS:
                String applicationData = System.getenv("APPDATA");
                String folder = applicationData != null ? applicationData : userHome;

                workingDirectory = Paths.get(folder, ".minecraft/");
                break;
            case MACOS:
                workingDirectory = Paths.get(userHome, "Library/Application Support/minecraft");
                break;
            default:
                workingDirectory = Paths.get(userHome, "minecraft/");
        }
        return workingDirectory;
    }
}
