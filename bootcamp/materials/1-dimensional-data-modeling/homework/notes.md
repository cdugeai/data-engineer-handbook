

model actor_films in + efficient way

## Additive vs non Additive dimensions

honda driver is non additive= driver can drive 2 cars
honda cars are additive: 

a dimension can be additive on a small engough timescale (cannot drive 2 cars in 1 second)
can apply to some metric and not all. Ex: miles driven SUM ok but COUNT ko

## when should use enums ?

rule  thumb enum if cardinality < 50
why use it:
- built in data quality
- built in static fields
- built in doc
- good for subpartition for big data


## Flexible schema

Leverage the **Map** datatype.
Benefits:
- no ALTER TABLE anymore when add columns -> add element to a Map (max 65k keys)
- not a ton of NULLs
- use columns "other_properties"

drawbacks:
- compression is bad (col names are store in each map)
- readability, quryability

## Graph modeling - how is it different ?

relashionship focused != entity focused 

Trick: this schema works for every usecase:
*Entities*:
- Identifier: string
- Type: string
- Properties: Map<str, str>
*Relations*:
- subject_identifier: string
- subject_type: vertex_type (person doing)
- object_identifier: string
- object_type: vertex_type (person done on)
- edge_type: edge_type (verb)
- Properties: Map<str, str>


