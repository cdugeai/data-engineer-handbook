Fact= something that happenened, atomic. Transactions, user log in app
Facts are **not** changing
Facts need a **WHEN** column (datetime).

Issues:
- size, many
- dupplicates

Normalized vs denormalized Facts. Denormalized bring some dimensional attributes for quicker analysis.

Raw logs: ugly schema, quality error
Fact data: nice columns, quality guarantees

Ex of denormalisation: add `appnme` in raw logs to prevent a huge join afterwards

Logging should conform to a schema. Ex: Apache Thrift.

Workaround for high volume data:
- sampling
- bucketing: bucket joins faster than shuffle joins. SMB (sort-merged bucked) joins can do join without shuffle.

Retention:
Dedup fact data: 
- dedup on a **time window**, otherwise ignore

Streaming to dedup: using window. But cannot dedup on day with this bcs streaming window of 24h is too big.
Hourly microbatch dedup: 

## Lecture 2

aggregate facts to turn them into dimensions with **bucketizing**, or class (ex: scoring class)
Bucketize with **quantiles**

Facts vs dimensions:
- dim: show in group by, come from the state of a thing
- facts: aggregate on it (sum, count), higher volumes than dim, generally comes from logs

Existence-based facts/dims (ex: dim_has_ever_booked, dim_is_active).

Categorial facts/dims (ex: scoring_class) often calculated by CASE WHEN and bucketizing.

## Lecture 3

Why should shuffle be minimized ?

extremly parrallel: select, from where
kinda parralel: group by, join, having
not parallel: order by

shuffle happens when you nee to have all the data with a specific key on a single/specific machine.

Make group BY more efficient: use buckets and tell spark to shuffle on these keys + reduce data volume first.

How to reduce fact data volume ? Group by day/week/year.

Long array metrics:

| user_id | metric_name | month_start | value_array    |
|---------|-------------|-------------|----------------|
| 3       | likes_given | 2023-07-01  | [34,3,3,...,9] |
| 3       | likes_given | 2023-08-01  | [12,4,14,...,12] |
| 3       | likes_given | 2023-09-01  | [4,6,6,...,21] |

Daily dates are here found by the index being an offset in days of the month start.