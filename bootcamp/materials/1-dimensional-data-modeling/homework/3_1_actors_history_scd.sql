DROP TABLE public.actors_history_scd;

CREATE TABLE public.actors_history_scd (
    actorid text NOT NULL,
    quality_class quality_class,
    is_active boolean,
    start_date integer,
    end_date integer,
    current_year integer,
    CONSTRAINT actors_history_scd_pkey PRIMARY KEY (actorid, start_date)
);
