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
package io.github.avpinchuk.util;

import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * A bounded concurrent open-addressing cache with linear probing optimized for reading.
 *
 * <p>When cache is full, replaces an old elements. Work best on small sets of elements
 * when number of elements is below half of the capacity, i.e. the load factor is below 0.5f.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 */
public class BoundedOptimisticReaderCache<K, V> {

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by the constructor.
     * MUST be a power of two <= 1 << 30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The table. When allocated, length is always a power of two.
     */
    private final Map.Entry<K, V>[] table;

    /**
     * The table lock, used for the put operations.
     */
    private final Lock lock;

    /**
     * Table length.
     */
    private final int size;

    /**
     * The table size mask, used for compute the index in the table.
     */
    private final int sizeMask;

    /**
     * The number of elements in the table.
     */
    private volatile int count;

    /**
     * Creates a new, empty cache with the specified size.
     *
     * @param capacity cache size
     * @throws IllegalArgumentException if the cache size is non-positive
     */
    @SuppressWarnings("unchecked")
    public BoundedOptimisticReaderCache(int capacity) {
        this.table = (Map.Entry<K, V>[]) Array.newInstance(Map.Entry.class, adjustCapacity(capacity));
        this.lock = new ReentrantLock();
        this.size = capacity;
        this.sizeMask = capacity - 1;
    }

    /**
     * Returns a power of two size for the given target capacity.
     */
    private int adjustCapacity(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        if (capacity > MAXIMUM_CAPACITY) {
            return MAXIMUM_CAPACITY;
        }
        // already power of two
        if (Integer.bitCount(capacity) == 1) {
            return capacity;
        }
        return Integer.highestOneBit(capacity) << 1;
    }

    /**
     * Computes {@code key.hashCode()} and spreads (XORs) higher bits of hash
     * to lower. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will always collide.
     * (Among known examples are sets of {@code Float} keys holding consecutive
     * whole numbers in small tables). So we apply a transform that spreads the
     * impact of higher bits downward. There is a tradeoff between speed, utility,
     * and quality of bit-spreading. Because many common sets of hashes are already
     * reasonably distributed (so don't benefit from spreading), we just XOR some
     * shifted bits in the cheapest possible way to reduce systematic lossage,
     * as well as to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     */
    @SuppressWarnings("FinalPrivateMethod")
    private final int hash(Object key) {
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    /**
     * If the specified key is not already associated with a value, attempts to compute
     * its value using the given mapping function and enters it into this map unless
     * {@code null}. The entire method invocation is performed atomically, so the function
     * is applied at most once per key. Some attempted update operations on this cache by
     * other threads may be blocked while computation is in progress, so the computation
     * should be short and simple, and must not attempt to update any other mappings of this map.
     *
     * @param key key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return The current (existing or computed) value associated with the specified key,
     * or {@code null} if the computed value is {@code null}.
     * @throws NullPointerException if the specified {@code key} or {@code mappingFunction} is {@code null}
     * @throws RuntimeException if the {@code mappingFunction} does so, in which case the mapping is
     * left unestablished
     * @throws Error of the {@code mappingFunction} does so, in which case the mapping is left unestablished
     */
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        if (key == null || mappingFunction == null) {
            throw new NullPointerException();
        }

        int index = hash(key) & sizeMask;

        // try to optimistic read
        if (count != 0) {   // read-volatile
            for (int i = index; i < index + size; i++) {
                Map.Entry<K, V> entry = table[i & sizeMask];
                if (entry != null) {
                    if (key.equals(entry.getKey())) {
                        return entry.getValue();
                    }
                } else {
                    break;
                }
            }
        }

        lock.lock();
        try {
            int putIndex = index;
            // re-read under lock to exclude an
            // intermediate write by other thread
            if (count != 0) {
                for (int i = index; i < index + size; i++) {
                    Map.Entry<K, V> entry = table[i & sizeMask];
                    if (entry != null) {
                        if (key.equals(entry.getKey())) {
                            return entry.getValue();
                        }
                    } else {
                        putIndex = i & sizeMask;
                        break;
                    }
                }
            }
            int c = count;
            V value = mappingFunction.apply(key);
            // map contract: doesn't record the mapping
            // if the mapping function returns null
            if (value != null) {
                table[putIndex] = new AbstractMap.SimpleImmutableEntry<>(key, value);
                c++;
            }
            count = Math.min(c, size);  // write-volatile
            return value;
        } finally {
            lock.unlock();
        }
    }
}
