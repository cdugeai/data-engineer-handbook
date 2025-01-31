# Lecture 1

have to maintain pipelines. 

Ownership models. Who owns what:
- datasets
- data pipeline
- data documentaion
- metrics
- experiments

Centralized vs embedded teams.

Common issues on pipeline:
- OOM (bcs of skewed data -> use "enable adaptative execution" in Spark 3 OR add **skew join salt**)
- missing data / schema change upstream -> pre-check upstream data, talk to the owner
- backfills -> backfills patterns
- business questions -> set SLAs to answer business questions, put questions to FAQ, 

# Lecture 2 - Signals of tech debt in DE


painful pipelines. Solutions:
- new tech: migrade to Spark, or Flink (if real time)
- sampling: if need directionnality, not exact metric
- **bucketing**: when expensive high-cardinality JOIN or GROUP BY. Shuffle does not exist when bucketing first
- better DModeling,
- delete pipeline :)



large cloud bill:
- cloud cost: IO > compute > storage. Dupplicated data models, excessive backfills, not sampling, not subpartitioning correctly (**predicate pushdown**)

multiple source truth:
- document all source and discrepancies
- understand stakeholders on why there are multiple truth and
- build a spec on which everybody agreed upon

Build pipeline spec: 
- capture needs of stakeholders
- get all multiples sources of truth owners to sign off (helps with migrations later)

Getting ahead of tech debts:
- fix as you go
- allocate a portion of time this quarter "tech excellence week" (at Airbnb)
- use on-call person for fixing

undocumented datasets

Notes:
- Hive makes calculation on disk vs Spark (on RAM)
- article [predicate pushdown](https://airbyte.com/data-engineering-resources/predicate-pushdown)


Data migrations models. strategies:
- cautious: parallel pipelines for months
- bull in a china shop: kill the legacy soon

Oncall responsabilities:
- set proper expectations with stakeholders
- document every failure
- oncall handoff: 20 meet to pass context

Runbooks. Complex pipelines need Runbooks. Must contains:
- primary and secondary owners
- upstream owners teams
- common issues
- critical downstream owners
- SLAs and agreements

Quarterly meeting with every owner of upstream and downstream data consumers.



