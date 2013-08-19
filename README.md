#### Cardinality Estimation and Distinct Value Sampling with Recordinality
###### C. Scott Andreas | Aug 19, 2013

**Cardinality Estimation**

Determining the number of unique elements that make up a stream is a frequently-encountered problem in stream processing. A few applications include counting the number of unique visitors to a website or determining the number of unique hosts communicating with an application cluster. In both cases, the number can be arbitrarily large, making it infeasible to maintain a map containing each unique value and just counting the elements. Instead, we turn to a category of algorithms called "[sketches](http://blog.aggregateknowledge.com/2011/09/13/streaming-algorithms-and-sketches/)." Sketches help us estimate the cardinality of infinite streams using very little memory and with bounded error estimates.

[HyperLogLog](http://blog.aggregateknowledge.com/2012/10/25/sketch-of-the-day-hyperloglog-cornerstone-of-a-big-data-infrastructure/) is an excellent choice for streaming cardinality estimation in most cases. However, HLL is simply an estimator - it is a tool that does one thing, and one thing very well. At Aggregate Knowledge's May SketchConf, Jérémie Lumbroso introduced me to another lesser-known sketch called "Recordinality." Here is [the paper](http://www-apr.lip6.fr/~lumbroso/Publications/HeLuMaVi12.pdf) describing it.

**Recordinality**

Recordinality is unique in that it provides cardinality estimation like HLL, but also offers "distinct value sampling." This means that Recordinality can allow us to fetch a random sample of distinct elements in a stream, invariant to cardinality. Put more succinctly, given a stream of elements containing 1,000,000 occurrences of 'A' and one occurrence each of 'B' - 'Z', the probability of any letter appearing in our sample is equal. Moreover, we can also efficiently store the number of times elements in our distinct sample have been observed. This can help us to understand the distribution of occurrences of elements in our stream. With it, we can answer questions like "do the elements we've sampled present in a power law-like pattern, or is the distribution of occurrences relatively even across the set?"

**The Algorithm**

Beyond these unique properties, Recordinality is especially interesting due to its simplicity. Here's how it works:

  1. Initialize a set of size k (the size of this set determines the accuracy of our estimates)
  2. Compute the hash of each incoming element in the stream using a hash function such as [MurmurHash3](https://sites.google.com/site/murmurhash/).
  3. If the k-set is not full, insert the hash value. If the set is full, determine if the value of the hash is greater than the lowest hash currently in the set. If so, replace it. If not, ignore it.
  4. Each time we add or replace an element in the k-set, increment a counter.

The premise is straightforward: the cardinality of a stream can be estimated by hashing each element to a random value and counting the number of times a set containing the max of these values is mutated. We gain unique sampling if we switch from a set to a map and store the original value in our k-set. We gain counts of unique values observed by storing both the original value and incrementing a counter each time it's observed. In addition to being easy to reason about, it's also extraordinarily efficient. The common case requires only hashing an element and comparing two integers.

**Implementing Recordinality**

Interest piqued, Jérémie challenged me over dinner to implement it. Armed with Timon's advice on "[how to implement a paper](http://taco.cat/files/Screen%20Shot%202013-08-19%20at%202.58.36%20PM-N2VAMCNBev.png)" ([slides](https://docs.google.com/presentation/d/12mMdn5cjA-MhrbJSP6ThjIAs-YACWJ1p6BeSTXW0P4Q/edit?usp=sharing)), I read and re-read it making notes. A Saturday morning found me at [Sightglass](https://sightglasscoffee.com/) sitting down with the paper, a cup of Blueboon, and my laptop to begin implementation. One cup and a couple bugs later, I arrived at a working implementation of Recordinality and shuffled home to verify my results against those claimed by the paper against a known input set, which matched.

Here's an implementation of Recordinality in Java, comments added:
https://github.com/cscotta/recordinality/blob/master/src/main/java/com/cscotta/recordinality/Recordinality.java

This implementation is both threadsafe and lockless in the common case, allowing all mutators and readers safe concurrent access without synchronization. If cardinality estimation with distinct value sampling is of interest to you, please consider this implementation as a starting point. Translations to other languages should be straightforward; please let me know if you attempt one and I'll list it here.


**Performance**

The Recordinality paper includes mean cardinality and error estimates for k-values from 4 to 512 against a publicly-available dataset – namely, the text of A Midsummer Night's Dream. Here is a comparison of the results included in the paper versus those emitted by the unit test included with this implementation. The values recorded below are the results of 10,000 runs.

[Note: The source text used in the paper is listed as containing 3031 distinct words. The copy I've obtained for verification and based implementation stats on below from Project Gutenberg contained 3193 distinct words. It is included in this repository.]

| Size | Paper Mean (Expected: 3031) | Paper Error | Impl Mean (Expected: 3193) | Impl Error | Mean Run Time |
|--------|---------------------------|-------------|----------------------------|------------|---------------|
| 4      | 2737 | 1.04 | 3127 | 1.61 | 5ms   |
| 8      | 2811 | 0.73 | 3205 | 0.92 | 5ms   |
| 16     | 3040 | 0.54 | 3204 | 0.55 | 6ms   |
| 32     | 3010 | 0.34 | 3195 | 0.35 | 7ms   |
| 64     | 3020 | 0.22 | 3200 | 0.22 | 10ms  |
| 128    | 3042 | 0.14 | 3194 | 0.13 | 19ms  |
| 256    | 3044 | 0.08 | 3193 | 0.08 | 38ms  |
| 512    | 3043 | 0.04 | 3191 | 0.04 | 116ms |

You can run this test yourself by cloning the repo and typing `mvn test`. Here is an example of the expected output: https://gist.github.com/cscotta/8fddc06871d8543df897

**Epilogue**

The implementation of Recordinality was driven by practical needs as much as it was by a desire to encourage greater cross-pollination between industry and academia. This is the first known open source implementation of this algorithm I'm aware of, and the only cardinality estimation sketch that provides distinct value sampling and the frequency of each. Working with Jérémie to understand and implement Recordinality was a pleasure (as most evenings talking shop that end at 2 am at Pilsner Inn are)! It's always a delight to see unique and useful algorithms spawn open source implementations for use by others coming after.

Thanks, Jérémie!

---

Here are the [slides](https://speakerdeck.com/timonk/philippe-flajolets-contribution-to-streaming-algorithms) presented by Jérémie, along with a [video of the original presentation](http://www.youtube.com/watch?v=Xigaf8npHoI).
