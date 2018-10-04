DROP SCHEMA IF EXISTS sqb_test CASCADE;
CREATE SCHEMA sqb_test AUTHORIZATION postgres;

CREATE TABLE sqb_test.regions
(
    id character varying(10) COLLATE pg_catalog."default" NOT NULL,
    name character varying(16) COLLATE pg_catalog."default",
    CONSTRAINT regions_pkey PRIMARY KEY (id)
);

CREATE TABLE sqb_test.airports
(
    id character varying(10) COLLATE pg_catalog."default" NOT NULL,
    shortname character varying(10) COLLATE pg_catalog."default",
    name character varying(32) COLLATE pg_catalog."default",
    region character varying(5) COLLATE pg_catalog."default",
    icao character varying(10) COLLATE pg_catalog."default",
    flags integer,
    catalog integer,
    length integer,
    elevation integer,
    runway character varying(5) COLLATE pg_catalog."default",
    frequency float,
    latitude character varying(10) COLLATE pg_catalog."default",
    longitude character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT airports_pkey PRIMARY KEY (id),
    CONSTRAINT fk_airports_region FOREIGN KEY (region)
        REFERENCES sqb_test.regions (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);