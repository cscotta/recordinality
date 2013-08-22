/*
 * Copyright 2013, C. Scott Andreas (@cscotta / scott@paradoxica.net)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.cscotta.recordinality;

import java.util.Set;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import com.google.common.collect.ImmutableSet;

public class Recordinality<T> {

    private final int sampleSize;
    private final int seed = new Random().nextInt();
    private final HashFunction hash = Hashing.murmur3_128(seed);
    private final AtomicLong modifications = new AtomicLong(0);
    private final AtomicLong cachedMin = new AtomicLong(Long.MIN_VALUE);
    private final ConcurrentSkipListMap<Long, Element> kMap =
        new ConcurrentSkipListMap<Long, Element>();
    private volatile int kMapSize = 0;

    /*
     * Initializes a new Recordinality instance with a configurable 'k'-size.
     */
    public Recordinality(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    /*
     * Observes a value in a stream.
     */
    public void observe(T element) {
        boolean inserted = insertIfFits(element);
        if (inserted) modifications.incrementAndGet();
    }

    /*
     * Returns an estimate of the stream's cardinality. For a description
     * of this estimator, see see §2 Theorem 1 in the Recordinality paper:
     * http://www-apr.lip6.fr/~lumbroso/Publications/HeLuMaVi12.pdf
     */
    public long estimateCardinality() {
        long pow = modifications.get() - sampleSize + 1;
        double estimate = (sampleSize * (Math.pow(1 + (1.0 / sampleSize), pow))) - 1;
        return (long) estimate;
    }

    /*
     * Returns an estimate of Recordinality's error for this stream,
     * expressed in terms of standard error. For a description of this estimator,
     * see §2 Theorem 2: http://www-apr.lip6.fr/~lumbroso/Publications/HeLuMaVi12.pdf
     */
    public double estimateError() {
        double estCardinality = estimateCardinality();
        double error = Math.sqrt(Math.pow(
                (estCardinality / (sampleSize * Math.E)), (1.0 / sampleSize)) - 1);
        return error;
    }

    /*
     * Returns the current set of k-records observed in the stream.
     */
    public Set<Element> getSample() {
        return ImmutableSet.copyOf(kMap.values());
    }

    /*
     * Inserts a record into our k-set if it fits.
    */
    private boolean insertIfFits(T element) {
        long hashedValue = 0L;

        if (element.getClass() == String.class)
            hashedValue = hash.hashString((String)element).asLong();
        else if (element.getClass() == Long.class)
            hashedValue = hash.hashLong((Long)element).asLong();
        else if (element.getClass() == Integer.class)
            hashedValue = hash.hashInt((Integer)element).asLong();
        else
            throw new ClassCastException();

        // Short-circuit if our k-set is saturated. Common case.
        if (hashedValue < cachedMin.get() && kMapSize >= sampleSize)
            return false;

        synchronized (this) {
            assert(kMapSize <= sampleSize);

            if (kMapSize < sampleSize || hashedValue >= cachedMin.get()) {
                Element existing = kMap.get(hashedValue);
                if (existing != null) {
                    existing.count.incrementAndGet();
                    return false;
                } else {
                    long lowestKey = (kMapSize > 0) ? kMap.firstKey() : Long.MIN_VALUE;
                    if (hashedValue > lowestKey || kMapSize < sampleSize) {
                        kMap.put(hashedValue, new Element(element));
                        kMapSize++;
                        cachedMin.set(lowestKey);
                        if (kMapSize > sampleSize) {
                            kMap.remove(lowestKey);
                            kMapSize = sampleSize;
                        }
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            return false;
        }
    }

    @Override
    public String toString() {
        synchronized (this) {
            return "Recordinality{" +
                    "sampleSize=" + sampleSize +
                    ", hash=" + hash +
                    ", modifications=" + modifications.get() +
                    ", cachedMin=" + cachedMin.get() +
                    ", mapSize=" + kMap.size() +
                    ", estCardinality=" + estimateCardinality() +
                    ", estError=" + estimateError() +
                    '}';
        }
    }

    /*
     * Inner class representing a pair of an observed k-record in the stream
     * along with the number of times it has been observed.
     */
    public class Element {

        public final T value;
        public final AtomicLong count;

        public Element(T value) {
            this.value = value;
            this.count = new AtomicLong(1);
        }

        @Override
        public String toString() {
            return "Element{" +
                    "value='" + value + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
