# Lecture 1

types of impact: measurable, hard to measure, immeasurable(team culture improvments, being glue person).

maintain trust in data you produce: DQ checks, docs, engineering practices, use SLA, 

increase data pipeline efficiency: proper data modeling, reduce data volumes, picking right tool for th job (ie migrations)

# Lecture 2

Best practice: pre-aggregate the data (use `GROUPING SETS`), dont join, use low latency tools like *Druid* or *Pino*.

nUMBER THAT MATTERS:
- TOTAL AGGREGATES: reported to wall St, can be used in material, represent how the busines is going overall
- time based aggregated: catch trends early, identify growth and trends
- time & entity based agg: used in AB test, used by DScientists, often included in daily master data
- derivative metrics: + sensitive to chg, use % increase, 
- dimensional mix: idenitfy impact opportunities, spot trends in populations (not just over time)
- retention/survivorship (% left after N days): also called **J-curve**, indicates stickiness of app

# Lab 2




