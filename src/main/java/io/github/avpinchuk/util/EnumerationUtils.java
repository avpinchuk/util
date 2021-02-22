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

import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class consist exclusively of static methods that operate on or return
 * enumerations.
 */
public final class EnumerationUtils {

    private EnumerationUtils() {
        throw new AssertionError();
    }

    /**
     * Enumeration to iterator adapter.
     *
     * @param enumeration an adapted enumeration
     * @param <T> type of an enumeration's elements
     * @return An iterator that has same elements in same order as input enumeration.
     */
    public static <T> Iterator<T> iterator(Enumeration<T> enumeration) {
        return new Iterator<T>() {
            private final Enumeration<T> e = enumeration;

            @Override
            public boolean hasNext() {
                return e.hasMoreElements();
            }

            @Override
            public T next() {
                if (e.hasMoreElements()) {
                    return e.nextElement();
                }
                throw new NoSuchElementException();
            }
        };
    }
}
