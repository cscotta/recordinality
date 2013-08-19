package com.cscotta.recordinality;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.Lists;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.io.FileUtils;

public class RecordinalityTest {

    @Test
    public void testRecordinality() throws Exception {
        final int numRuns = 10000;
        final int[] kSizes = new int[]{4,8,16,32,64,128,256,512};

        // Build our lowercased, trimmed list of words from our source file.
        final String msnd = "/midsummer-nights-dream-gutenberg.txt";
        final URI uri = getClass().getResource(msnd).toURI();
        final List<String> input = FileUtils.readLines(new File(uri));
        final List<String> words = Lists.newArrayList();
        for (String line : input) words.add(line.trim().toLowerCase());

        // Compute the actual cardinality of our set.
        final Set<String> actualSet = new HashSet<String>();
        for (String line : words) actualSet.add(line);
        final double actualSize = actualSet.size();
        System.out.println("Actual cardinality: " + actualSet.size());

        // Run 'recordinality' repeatedly over the stream.
        final ExecutorService exec = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors());

        final List<Future<Result>> resultFutures = Lists.newArrayList();
        for (int kSize : kSizes) {
            resultFutures.add(exec.submit(buildRun(kSize, numRuns, words)));
        }

        final double[] errorBounds = new double[]{2.5, 1, 0.6, 0.4, 0.25, 0.2, 0.1, 0.05};
        int errorBoundIdx = 0;
        for (Future<Result> future : resultFutures) {
            Result result = future.get();
            System.out.println("k-size: " + result.kSize);
            System.out.println("Mean est: " + result.mean);
            System.out.println("Std. err: " + result.stdError);
            System.out.println("Mean run time: " + (result.runTime / numRuns) + "ms.");
            System.out.println("Mean estimate is off by a factor of " + ((result.mean / actualSize) - 1));
            System.out.println("\n==================================================\n");
            assertTrue(result.stdError <= errorBounds[errorBoundIdx]);
            errorBoundIdx += 1;
        }

        exec.shutdown();
    }

        private Callable<Result> buildRun(final int kSize, final int numRuns,
                                          final List<String> lines) {
            return new Callable<Result>() {
                public Result call() throws Exception {
                    long start = System.currentTimeMillis();
                    final double[] results = new double[numRuns];
                    for (int i = 0; i < numRuns; i++) {
                        Recordinality rec = new Recordinality(kSize);
                        for (String line : lines) rec.observe(line);
                        results[i] = rec.estimateCardinality();
                    }
                    double mean = new Mean().evaluate(results);
                    double stdDev = new StandardDeviation().evaluate(results);
                    double stdError = stdDev / 3193;
                    long runTime = System.currentTimeMillis() - start;
                    return new Result(kSize, mean, stdError, runTime);
                }
            };
        }


    private static final class Result {
        int kSize;
        double mean;
        double stdError;
        long runTime;

        public Result(int kSize, double mean, double stdError, long runTime) {
            this.kSize = kSize;
            this.mean = mean;
            this.stdError = stdError;
            this.runTime = runTime;
        }
    }

}
