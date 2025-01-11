# Lecture 1

What is streaming pipeline ? 
- process data in low latency

- streaming: Flink, data processed as generated (**continuous** processing)
- near real-time/microbatch: Spark structured Streaming, batches every few minutes

Streaming only use cases: detecting fraud, high freqency trading, live event processing, sports analytics.

Stream vs batch pipeline ?
- streaming ppl run 24/7
- streaming pipeline are much **software oriented** (more like web server and not DAGs) -> so need unit+integration tests

real time is a myth: (network)
Structure of streaming pipeline: 
- source: kafka= like a firehose in **one single stream of events**, cannot really diverge
- source: rabitmq= not much throuput so does not scale as well as Kafka, but much complex routing mechanism so easier for pubsub/msg broker
- source: side inputs= data table (iceberge, postgres, ...) -> enrich event data. Refresh on a cadence (/3h)

Destinations (or sink). Common sinks:
- another Kafka topic
- Iceberg (allow to append data to partition while Hive was only overriding. Hive was batch-oriented)
- Postgres

Streaming challenges:
- out of order events. Flink **watermarking** deals with it.
- late arriving data
- recovering from failures.

Recovering from failures
- offsets: choice when Flink starts: *earliest* vs *latest* vs *specific timestamp* (eg. timestamp when failed) offset, or checkpoint or savepoint
- checkpoints (save state of job, where to read/write). internal to Flink binaries
- savepoints. More agnostic, can be used by another system than Flink, like CSV file of data

Late arriving data:
- **how late** is too late ?
- Watermarking handles 95 of late arriving data
- the rest is **long tail** coming exceptionnally late

## Lesson 2

Competing **architectures for streaming**: Lambda vs Kappa.
- Lambda: batch + streaming pipelines. for latency and correctness, double code base, easier for DQ checks on batch side
- Kappa: Streaming only, least complex, great latency wins. But painfully for backfill history. But Iceberg (allows append), delta lake, Hudi is making this architecture more viable.

Flink UDFs: Python UDFs are way less perf then Scala/Java UDFs bcs Flink is JVM native. Spak has same issue

Flink windows:
- data driven window: 
    - Count -> opens till N events occur. Useful for funnel with predictable number of events. With a *timeout*
- time-driven window: 
    - tumbling -> fixed siez, no overlab, similar to hourly (like in batch world), great for chuncking data
    - sliding -> fixed with, can overlap, good for finding "peak-use" windows, good at handling accross midnight exceptions of batch.
    - session -> **user specific**, based on activity, variable length

Allowed lateness vs Watermarking:
Watermarking:
- little late, great
- reordering events in this window

Allowed lateness:
- minutes late
- but will reprocesse data that was in closed previous window -> will roduce new/updated record 

## Lab 2


