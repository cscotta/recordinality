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

public class Recordinality {

    private final int sampleSize;
    private final int seed = new Random().nextInt();
    private final HashFunction hash = Hashing.murmur3_128(seed);
    private final AtomicLong modifications = new AtomicLong(0);
    private final AtomicLong cachedMin = new AtomicLong(Long.MIN_VALUE);
    private final ConcurrentSkipListMap<Long, Element> kMap =
        new ConcurrentSkipListMap<Long, Element>();

    /*
     * Initializes a new Recordinality instance with a configurable 'k'-size.
     */
    public Recordinality(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    /*
     * Observes a value in a stream.
     */
    public void observe(String element) {
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
    private boolean insertIfFits(String element) {
        long hashedValue = hash.hashString(element).asLong();

        // Short-circuit if our k-set is saturated. Common case.
        if (kMap.size() >= sampleSize && hashedValue < cachedMin.get())
            return false;

        synchronized (this) {
            int mapSize = kMap.size();
            assert(mapSize <= sampleSize);

            if (mapSize < sampleSize || hashedValue >= cachedMin.get()) {
                Element existing = kMap.get(hashedValue);
                if (existing != null) {
                    existing.count.incrementAndGet();
                    return false;
                } else {
                    long lowestKey = (mapSize > 0) ? kMap.firstKey() : Long.MIN_VALUE;
                    if (hashedValue > lowestKey || mapSize < sampleSize) {
                        kMap.put(hashedValue, new Element(element));
                        cachedMin.set(lowestKey);
                        if (kMap.size() > sampleSize) kMap.remove(lowestKey);
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

        public final String value;
        public final AtomicLong count;

        public Element(String value) {
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
