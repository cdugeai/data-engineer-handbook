# Lecture 1

Coupple of != patterns for data analytics pipelines:
- Growth accounting (= **state change change tracking**)
- Survivor analysis pattern (retention number)
- Window based analysis

Repeatable analysis are best friend.

Common patterns:
- aggregation-based patterns
- cumulation-based patterns
- window-based patterns

Aggregation-based patterns: trend analysis, root cause analysis
- GROUP BY
- upstream dataset is often "daily metrics"
- patterns: root cause, trends, composition

Cumulation-based patterns: state transition tracking, retention (*j curve*)?
- FULL OUTER JOIN
- time is significantly different dimension vs others
- patterns: state change tracking, survival analysis (= retention)

Growth accountting: special case of state transition track with 5 states:
- NEW, RETAINED, CHURNED, RESURRECTED, STALE, DEACTIVATED

J-curve: curve, state check, reference date.

|Curve                                                  |State check        |Reference Date  |
|-------------------------------------------------------|-------------------|----------------|
|Users who stay active                                  |Activity on the app|Sign up date    |
|Cancer patients who continue to live                   |Not dead           |Diagnosis date  |
|Smokers who remain smoke-free after quitting           |Not smoking        |Quit date       |
|Boot camp attendees who keep attending all the sessions|Activity on Zoom   |Enrollment date |

*A state that is prolonged througout time that is checked vs a reference date*.

Window based analysis: DoD/WoW/MoM/YoY, rolling sum/average, rank.
- `FUNCTION() OVER (PARTITION BY keys ORDER BY sort ROWS BETWEEN n PRECEDING AND CURRENT ROW)`
- make sure to PARTITION ON some dimension to reduce volume

# Lecture 2

Funnels. 
use `COUNT(CASE WHEN)` to reduce nb of table scans
Advanced sql: 
- grouping: GROUPING SETS, GROUP BY CUBE, GROUP BY ROLLUP (multiple aggregations in on query without union all)
- self joins
- window functions: lag, lead, ROWS
- CROSS JOIN UNNEST (same as LATERAL VIEW EXPLODE)

Before using GROUPIN SETS, COALESCE grouping column to default value (ex: 'NA'). 
GROUP BY ROLLUP -> use for hierachycla data (ex: country, state, city). 
Dont use rank, use DENSE_RANK or ROWNUMBER


