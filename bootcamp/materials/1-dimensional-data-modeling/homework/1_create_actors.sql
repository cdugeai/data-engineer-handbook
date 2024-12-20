DROP TYPE public.film CASCADE;

CREATE TYPE film AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

DROP TYPE quality_class CASCADE;

CREATE TYPE quality_class AS
ENUM ('bad', 'average', 'good', 'star');

DROP TABLE IF EXISTS actors;

CREATE TABLE public.actors (
    actorid text NOT NULL,
    films film [],
    quality_class quality_class,
    is_active boolean,
    current_year INTEGER,

    CONSTRAINT actor_pkey PRIMARY KEY (actorid, current_year)
);
