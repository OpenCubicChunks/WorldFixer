package io.github.opencubicchunks.worldfixer;

import cubicchunks.regionlib.api.region.IRegion;
import cubicchunks.regionlib.api.region.IRegionProvider;
import cubicchunks.regionlib.api.region.key.IKey;
import cubicchunks.regionlib.api.region.key.RegionKey;
import cubicchunks.regionlib.util.CheckedConsumer;
import cubicchunks.regionlib.util.CheckedFunction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegionCache <K extends IKey<K>> implements IRegionProvider<K> {

    private final IRegionProvider<K> sourceProvider;

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Map<SharedCacheKey<?>, IRegion<?>> regionLocationToRegion = new ConcurrentHashMap<>(256);
    private static final int maxCacheSize = 128; // Change from 256 because macos has 256 limit on file handles per process

    private boolean closed;

    /**
     * Creates a RegionProvider using the given {@code regionFactory} and {@code maxCacheSize}
     *
     * @param sourceProvider provider used as source of regions
     */
    public RegionCache(IRegionProvider<K> sourceProvider) {
        this.sourceProvider = sourceProvider;
    }

    @Override
    public <R> Optional<R> fromExistingRegion(K key, CheckedFunction<? super IRegion<K>, R, IOException> func) throws IOException {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        return fromRegion(key, func, false);
    }

    @Override
    public <R> R fromRegion(K key, CheckedFunction<? super IRegion<K>, R, IOException> func) throws IOException {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        return fromRegion(key, func, true).get();
    }

    @Override
    public void forRegion(K key, CheckedConsumer<? super IRegion<K>, IOException> cons) throws IOException {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        forRegion(key, cons, true);
    }

    @Override
    public void forExistingRegion(K key, CheckedConsumer<? super IRegion<K>, IOException> cons) throws IOException {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        forRegion(key, cons, false);
    }

    @SuppressWarnings("unchecked") @Override public IRegion<K> getRegion(K key) throws IOException {
        RegionCache.SharedCacheKey<?> sharedKey = new RegionCache.SharedCacheKey<>(key.getRegionKey(), sourceProvider);
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            IRegion<K> r = (IRegion<K>) regionLocationToRegion.get(sharedKey);
            if (r != null) {
                regionLocationToRegion.remove(sharedKey);
                return r;
            } else {
                return sourceProvider.getRegion(key);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @SuppressWarnings("unchecked") @Override public Optional<IRegion<K>> getExistingRegion(K key) throws IOException {
        RegionCache.SharedCacheKey<?> sharedKey = new RegionCache.SharedCacheKey<>(key.getRegionKey(), sourceProvider);
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            IRegion<K> r = (IRegion<K>) regionLocationToRegion.get(sharedKey);
            if (r != null) {
                regionLocationToRegion.remove(sharedKey);
                return Optional.of(r);
            } else {
                return sourceProvider.getExistingRegion(key);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override public void forAllRegions(CheckedConsumer<? super IRegion<K>, IOException> consumer) throws IOException {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        sourceProvider.forAllRegions(consumer);
    }

    @Override public void close() throws IOException {
        synchronized (regionLocationToRegion) {
            if (closed) {
                throw new IllegalStateException("Already closed");
            }
            clearRegions();
            this.sourceProvider.close();
            this.closed = true;
        }
    }

    @SuppressWarnings("unchecked")
    private void forRegion(K location, CheckedConsumer<? super IRegion<K>, IOException> cons, boolean canCreate) throws IOException {
        IRegion<K> region;
        Lock readLock = lock.readLock();
        Lock writeLock = lock.writeLock();
        RegionCache.SharedCacheKey<?> sharedKey = new RegionCache.SharedCacheKey<>(location.getRegionKey(), sourceProvider);
        boolean createNew = false;

        readLock.lock();
        try {
            region = (IRegion<K>) regionLocationToRegion.get(sharedKey);
            if (region == null) {
                region = sourceProvider.getExistingRegion(location).orElse(null);
                if (region != null) {
                    regionLocationToRegion.put(sharedKey, region);
                }
                if (region == null && canCreate) {
                    createNew = true;
                }
            }

            if (region != null) {
                cons.accept(region);
            }
        } finally {
            readLock.unlock();
        }
        if (createNew) {
            writeLock.lock();
            try {
                if (regionLocationToRegion.size() > maxCacheSize) {
                    clearRegions();
                }
                region = sourceProvider.getRegion(location);
                regionLocationToRegion.put(sharedKey, region);
                cons.accept(region);
            } finally {
                writeLock.unlock();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <R> Optional<R> fromRegion(K location, CheckedFunction<? super IRegion<K>, R, IOException> func, boolean canCreate) throws IOException {
        IRegion<K> region;
        Lock readLock = lock.readLock();
        Lock writeLock = lock.writeLock();
        RegionCache.SharedCacheKey<?> sharedKey = new RegionCache.SharedCacheKey<>(location.getRegionKey(), sourceProvider);
        boolean createNew = false;

        readLock.lock();
        try {
            region = (IRegion<K>) regionLocationToRegion.get(sharedKey);
            if (region == null) {
                region = sourceProvider.getExistingRegion(location).orElse(null);
                if (region == null && canCreate) {
                    createNew = true;
                }
            }

            if (region != null) {
                return Optional.of(func.apply(region));
            }
        } finally {
            readLock.unlock();
        }
        if (createNew) {
            writeLock.lock();
            try {
                if (regionLocationToRegion.size() > maxCacheSize) {
                    clearRegions();
                }
                region = sourceProvider.getRegion(location);
                regionLocationToRegion.put(sharedKey, region);
                return Optional.of(func.apply(region));
            } finally {
                writeLock.unlock();
            }
        }
        return Optional.empty();
    }

    public static synchronized void clearRegions() throws IOException {
        lock.writeLock().lock();
        try {
            Iterator<IRegion<?>> it = regionLocationToRegion.values().iterator();
            while (it.hasNext()) {
                it.next().close();
            }
            regionLocationToRegion.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class SharedCacheKey<K extends IKey<K>> {

        private final RegionKey regionKey;
        private final IRegionProvider<K> regionProvider;

        private SharedCacheKey(RegionKey regionKey, IRegionProvider<K> regionProvider) {
            this.regionKey = regionKey;
            this.regionProvider = regionProvider;
        }

        public RegionKey getRegionKey() {
            return regionKey;
        }

        public IRegionProvider<K> getRegionProvider() {
            return regionProvider;
        }

        @Override public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RegionCache.SharedCacheKey)) {
                return false;
            }

            RegionCache.SharedCacheKey<?> that = (RegionCache.SharedCacheKey<?>) o;

            if (!getRegionKey().equals(that.getRegionKey())) {
                return false;
            }
            if (!getRegionProvider().equals(that.getRegionProvider())) {
                return false;
            }

            return true;
        }

        @Override public int hashCode() {
            int result = getRegionKey().hashCode();
            result = 31 * result + getRegionProvider().hashCode();
            return result;
        }
    }
}
