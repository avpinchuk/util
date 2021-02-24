/*
 * Copyright (c) 2021, Alexander Pinchuk
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package io.github.avpinchuk.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * A cache supporting soft garbage collection semantics of entries,
 * full concurrency of retrievals, and adjustable expected concurrency
 * for updates.
 * <p/>
 * This cache is designed around specific advanced use-cases. If there is any
 * doubt whether this cache is for you, you most likely should be using
 * {@link java.util.concurrent.ConcurrentHashMap} instead.
 * <p/>
 * This cache supports soft entries. Entries of this table are periodically
 * removed once their corresponding entries are no longer referenced outside of
 * this cache. In other words, this table will not prevent an entry from being
 * discarded by the garbage collector. Once an entry has been discarded by the
 * collector, it is no longer visible to this cache; however, the entry may occupy
 * space until a future cache operation decides to reclaim it. In order to support
 * a high level of concurrency, stale entries are only reclaimed during blocking
 * (mutating) operations.
 * <p/>
 * This allows entries in this cache to remain until their space is absolutely
 * needed by the garbage collector. The primary use case is a cache, which ideally
 * occupies memory that is not in use for as long as possible.
 * <p/>
 * Retrieval operation generally do not block, so may overlap with update operations.
 * Retrievals reflect the results of the most recently <em>completed</em> update
 * operations holding upon their onset.
 * <p/>
 * The allowed concurrency among update operations is guided by the optional
 * {@code concurrencyLevel} constructor argument (default {@code 16}),
 * which is used as a hint for internal sizing. The cache is internally
 * partitioned to try to permit the indicated number of concurrent updates
 * without contention. Because placement in hash tables is essentially random,
 * the actual concurrency will vary. Ideally, you should choose a value to
 * accommodate as many threads as will ever concurrently modify the table. Using
 * a significantly higher value than you need can waste space and time, and a
 * significantly lower value can lead to thread contention. But overestimates
 * and underestimates within an order of magnitude do not usually have much
 * noticeable impact. A value of one is appropriate when it is known that only
 * one thread will modify and all others will only read. Also, resizing this or
 * any other kind of hash table is a relatively slow operation, so, when
 * possible, it is a good idea to provide estimates of expected table sizes in
 * constructors.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 *
 * @author Doug Lea
 * @author Jason T. Greene
 */
public class ConcurrentSoftReferenceCache<K, V> {
    /**
     * The maximum capacity, used if a higher value is implicitly
     * specified by either of the constructors with arguments. MUST
     * be a power of two <= 1 << 30 to ensure that entries are indexable
     * using ints.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The maximum number of segments to allow; used to bound
     * constructor arguments.
     */
    private static final int MAX_SEGMENTS = 1 << 16;    // slightly conservative

    /**
     * The default initial capacity for this table,
     * used when not otherwise specified in a constructor.
     */
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The default concurrency level for this table, used when not
     * otherwise specified in a constructor.
     */
    private static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The default load factor for this table, used when not
     * otherwise specified in a constructor.
     */
    private static final float DEFAULT_LOAD_FACTOR = .75f;

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    private final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    private final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table.
     */
    private final Segment<K, V>[] segments;

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentSoftReferenceCache uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     */
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash the hash code for the key
     * @return the segment
     */
    @SuppressWarnings("FinalPrivateMethod")
    private final Segment<K, V> segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    /**
     * Segments are specialized versions of hash tables.  This
     * subclasses from ReentrantLock opportunistically, just to
     * simplify some locking and avoid separate construction.
     *
     * <p>Segments maintain a table of entry lists that are ALWAYS
     * kept in a consistent state, so can be read without locking.
     * All fields of nodes are immutable (final).  All list
     * additions are performed at the front of each bin. This
     * makes it easy to check changes, and also fast to traverse.
     * When nodes would otherwise be changed, new nodes are
     * created to replace them. This works well for hash tables
     * since the bin lists tend to be short. (The average length
     * is less than two for the default load factor threshold.)
     *
     * Read operations can thus proceed without locking, but rely
     * on selected uses of volatiles to ensure that completed
     * write operations performed by other threads are
     * noticed. For most purposes, the "count" field, tracking the
     * number of elements, serves as that volatile variable
     * ensuring visibility.  This is convenient because this field
     * needs to be read in many read operations anyway:
     *
     *   - All (unsynchronized) read operations must first read the
     *     "count" field, and should not look at table entries if
     *     it is 0.
     *
     *   - All (synchronized) write operations should write to
     *     the "count" field after structurally changing any bin.
     *     The operations must not take any action that could even
     *     momentarily cause a concurrent read operation to see
     *     inconsistent data. This is made easier by the nature of
     *     the read operations in Map. For example, no operation
     *     can reveal that the table has grown but the threshold
     *     has not yet been updated, so there are no atomicity
     *     requirements for this with respect to reads.
     *
     * As a guide, all critical volatile reads and writes to the
     * count field are marked in code comments.
     */
    private static final class Segment<K, V> extends ReentrantLock {
        /**
         * The per-segment table.
         */
        private volatile HashEntry<K, V>[] table;

        /**
         * The collected weak-key reference queue for this segment.
         * This should be (re)initialized whenever table is assigned.
         */
        private volatile ReferenceQueue<Object> referenceQueue;

        /**
         * The number of elements in this segment's region.
         */
        private volatile int count;

        /**
         * The load factor for the hash table. Even though this value
         * is same for all segments, it is replicated to avoid needing
         * links to outer object.
         */
        private final float loadFactor;

        /**
         * The table is rehashed when its size exceeds this threshold.
         * (The value of this field is always {@code (int)(capacity * loadFactor)}.
         */
        private int threshold;

        public Segment(int initialCapacity, float loadFactor) {
            this.loadFactor = loadFactor;
            setTable(HashEntry.newArray(initialCapacity));
        }

        @SuppressWarnings("unchecked")
        public static <K, V> Segment<K, V>[] newArray(int capacity) {
            return (Segment<K, V>[]) Array.newInstance(Segment.class, capacity);
        }

        /**
         * Sets table to new HashEntry array.
         * Call only while holding lock or in constructor.
         */
        private void setTable(HashEntry<K, V>[] newTable) {
            threshold = (int) ( newTable.length * loadFactor );
            table = newTable;
            referenceQueue = new ReferenceQueue<>();
        }

        /**
         * Returns properly casted first entry of bin for given hash.
         */
        private HashEntry<K, V> getFirst(int hash) {
            HashEntry<K, V>[] tab = table;
            return tab[hash & (tab.length - 1)];
        }

        private HashEntry<K, V> newHashEntry(Map.Entry<K, V> entry, int hash, HashEntry<K, V> next) {
            return new HashEntry<>(entry, hash, next, referenceQueue);
        }

        public V computeIfAbsent(K key, int hash, Function<? super K, ? extends V> mapperFunction) {
            if (count != 0) {   // read-volatile
                V value = get(key, hash);
                if (value != null) {
                    return value;
                }
            }

            lock();
            try {
                V value;
                // re-check another thread already compute the value
                if (count != 0) {   // read-volatile
                    value = get(key, hash);
                    if (value != null) {
                        return value;
                    }
                }

                value = mapperFunction.apply(key);
                if (value != null) {
                    // remove stale (GC'd) entries only when
                    // we actually place mapping to the table,
                    // i.e. when mapping function returns not null
                    removeStale();

                    int c = count;
                    // ensure capacity
                    if (c++ > threshold) {
                        int reduced = rehash();
                        // adjust from possible GC references cleanup
                        if (reduced > 0) {
                            count = (c -= reduced) - 1; // write-volatile
                        }
                    }

                    HashEntry<K, V>[] tab = table;
                    int index = hash & (tab.length - 1);
                    HashEntry<K, V> first = tab[index];
                    first = newHashEntry(new AbstractMap.SimpleImmutableEntry<>(key, value), hash, first);
                    tab[index] = first;
                    count = c;  // write-volatile
                }
                return value;
            } finally {
                unlock();
            }
        }

        private V get(K key, int hash) {
            HashEntry<K, V> e = getFirst(hash);
            while (e != null) {
                Map.Entry<K, V> entry = e.get();
                if (entry != null && e.hash == hash && key.equals(entry.getKey())) {
                    return entry.getValue();
                }
                e = e.next;
            }
            return null;
        }

        /**
         * Reclassify nodes in each list to new Map.  Because we are
         * using power-of-two expansion, the elements from each bin
         * must either stay at same index, or move with a power of two
         * offset. We eliminate unnecessary node creation by catching
         * cases where old nodes can be reused because their next
         * fields won't change. Statistically, at the default
         * threshold, only about one-sixth of them need cloning when
         * a table doubles. The nodes they replace will be garbage
         * collectable as soon as they are no longer referenced by any
         * reader thread that may be in the midst of traversing table
         * right now.
         */
        private int rehash() {
            HashEntry<K, V>[] oldTable = table;
            int oldCapacity = oldTable.length;
            if (oldCapacity >= MAXIMUM_CAPACITY) {
                return 0;
            }

            HashEntry<K, V>[] newTable = HashEntry.newArray(oldCapacity << 1);
            threshold = (int) (newTable.length * loadFactor);
            int sizeMask = newTable.length - 1;
            int reduce = 0;
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < oldCapacity; i++) {
                // We need to guarantee that any existing reads of old Map can
                // proceed. So we cannot yet null out each bin.
                HashEntry<K, V> e = oldTable[i];

                if (e != null) {
                    HashEntry<K, V> next = e.next;
                    int index = e.hash & sizeMask;

                    // Single node on list
                    if (next == null) {
                        newTable[index] = e;
                    } else {
                        // Reuse trailing consecutive sequence at same slot
                        HashEntry<K, V> lastRun = e;
                        int lastIndex = index;
                        for (HashEntry<K, V> last = next; last != null; last = last.next) {
                            int k = last.hash & sizeMask;
                            if (k != lastIndex) {
                                lastIndex = k;
                                lastRun = last;
                            }
                        }
                        newTable[lastIndex] = lastRun;
                        // Clone all remaining nodes
                        for (HashEntry<K, V> p = e; p != lastRun; p = p.next) {
                            // Skip GC'd refs
                            Map.Entry<K, V> entry = p.get();
                            if (entry == null) {
                                reduce++;
                                continue;
                            }
                            int k = p.hash & sizeMask;
                            HashEntry<K, V> n = newTable[k];
                            newTable[k] = newHashEntry(entry, p.hash, n);
                        }
                    }
                }
            }
            table = newTable;
            return reduce;
        }

        @SuppressWarnings("unchecked")
        private void removeStale() {
            HashEntry<K, V> stale;
            while ((stale = (HashEntry<K, V>) referenceQueue.poll()) != null) {
                int c = count - 1;
                HashEntry<K, V>[] tab = table;
                int index = stale.hash & (tab.length - 1);
                HashEntry<K, V> first = tab[index];
                HashEntry<K, V> e = first;
                while (e != null && e != stale) {
                    e = e.next;
                }

                if (e != null) {
                    // All entries following removed node can stay
                    // in list, but all preceding ones need to be
                    // cloned
                    HashEntry<K, V> newFirst = e.next;
                    for (HashEntry<K, V> p = first; p != e; p = p.next) {
                        Map.Entry<K, V> entry = p.get();
                        // Skip GC'd entries
                        if (entry == null) {
                            c--;
                            continue;
                        }
                        newFirst = newHashEntry(entry, p.hash, newFirst);
                    }
                    tab[index] = newFirst;
                    count = c;  // write-volatile
                }
            }
        }
    }

    /**
     * A list entry. Note that this is never exported
     * out as a user-visible Map.Entry.
     *
     * HotSpot add acquire membar after load from Reference.referent field to prevent
     * commoning of loads across safepoint since GC can change its value. This guarantee
     * visibility of cleaning a reference by GC.
     */
    private static final class HashEntry<K, V> extends SoftReference<Map.Entry<K, V>> {
        private final int hash;
        private final HashEntry<K, V> next;

        public HashEntry(Map.Entry<K, V> entry, int hash, HashEntry<K, V> next, ReferenceQueue<Object> referenceQueue) {
            super(entry, referenceQueue);
            this.hash = hash;
            this.next = next;
        }

        @SuppressWarnings("unchecked")
        public static <K, V> HashEntry<K, V>[] newArray(int capacity) {
            return (HashEntry<K, V>[]) Array.newInstance(HashEntry.class, capacity);
        }
    }

    /**
     * Creates a new, empty cache with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param initialCapacity the initial capacity. The implementation
     * performs internal sizing to accommodate this many elements.
     * @param loadFactor the load factor threshold, used to control resizing.
     * Resizing may be performed when the average number of elements per
     * bin exceeds this threshold.
     * @param concurrencyLevel the estimated number of concurrently
     * updating threads. The implementation performs internal sizing
     * to try to accommodate this many threads.
     *
     * @throws IllegalArgumentException if the {@code initialCapacity} is
     * negative or the {@code loadFactor} or {@code concurrencyLevel} are
     * non-positive.
     */
    public ConcurrentSoftReferenceCache(int initialCapacity, float loadFactor, int concurrencyLevel) {
        if (!(loadFactor > 0) || initialCapacity < 0 || concurrencyLevel <= 0) {
            throw new IllegalArgumentException();
        }

        if (concurrencyLevel > MAX_SEGMENTS) {
            concurrencyLevel = MAX_SEGMENTS;
        }

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        this.segmentShift = Integer.SIZE - sshift;
        this.segmentMask = ssize - 1;
        this.segments = Segment.newArray(ssize);

        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }

        int c = initialCapacity / ssize;
        if (c * ssize < initialCapacity) {
            ++c;
        }
        int cap = 1;
        while (cap < c) {
            cap <<= 1;
        }

        for (int i = 0; i < this.segments.length; i++) {
            this.segments[i] = new Segment<>(cap, loadFactor);
        }
    }

    /**
     * Creates a new, empty cache with the specified initial capacity
     * and load factor, and concurrencyLevel ({@code 16}).
     *
     * @param initialCapacity The implementation performs internal
     * sizing to accommodate this many elements.
     * @param loadFactor the load factor threshold, used to control resizing.
     * Resizing may be performed when the average number of elements per
     * bin exceeds this threshold.
     *
     * @throws IllegalArgumentException if the {@code initialCapacity} of
     * elements is negative or the {@code loadFactor} is non-positive
     */
    public ConcurrentSoftReferenceCache(int initialCapacity, float loadFactor) {
        this(initialCapacity, loadFactor, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * Creates a new, empty cache with the specified initial capacity,
     * and with default load factor ({@code 0.75f}) and concurrencyLevel ({@code 16}).
     *
     * @param initialCapacity the initial capacity. The implementation
     * performs internal sizing to accommodate this many elements.
     *
     * @throws IllegalArgumentException if the {@code initialCapacity} of
     * elements is negative.
     */
    public ConcurrentSoftReferenceCache(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * Creates a new, empty map with a default initial capacity ({@code 16}),
     * load factor ({@code 0.75f}) and concurrencyLevel ({@code 16}).
     */
    public ConcurrentSoftReferenceCache() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * If the specified {@code key} is not already associated with a value, attempts
     * to compute its value using the given mapping function and enters it into this
     * cache unless {@code null}. The entire method invocation is performed atomically,
     * so the function is applied at mos once per key. Some attempted update operations
     * on this cache by other thread may be blocked while computation is in progress,
     * so the computation should be short and simple, and must not attempt to update any
     * other mappings of this cache.
     *
     * @param key key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return The current (existing or computed) value associated with the specified key,
     * or {@code null} if computed value is {@code null}.
     * @throws IllegalArgumentException if the specified {@code key} or {@code mappingFunction}
     * is {@code null}
     * @throws RuntimeException if the {@code mappingFunction} does so, in which case the mapping
     * is left unestablished
     * @throws Error if the {@code mappingFunction} does so, in which case the mapping is left unestablished
     */
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        if (key == null || mappingFunction == null) {
            throw new NullPointerException();
        }
        int hash = hash(key.hashCode());
        return segmentFor(hash).computeIfAbsent(key, hash, mappingFunction);
    }
}
