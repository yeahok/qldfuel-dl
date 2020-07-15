CREATE TABLE public.region
(
    id serial,
    name character varying,
    original_id integer,
    geographical_level integer,
    abbreviation character varying,
    region_parent_id integer,
	active boolean,
    PRIMARY KEY (id),
	FOREIGN KEY (region_parent_id)
        REFERENCES public.region (id) MATCH SIMPLE
		ON UPDATE CASCADE
		ON DELETE SET NULL
        NOT VALID,
	UNIQUE (original_id, geographical_level)
);


CREATE TABLE public.brand
(
    id integer,
    name character varying,
	active boolean,
    PRIMARY KEY (id)
);


CREATE TABLE public.fuel
(
    id integer,
    name character varying,
	active boolean,
    PRIMARY KEY (id)
);


CREATE TABLE public.site
(
    id integer,
    name character varying,
    brand_id integer,
    address character varying,
    post_code character varying,
    latitude numeric,
    longitude numeric,
    modified_date timestamp with time zone,
	active boolean,
    PRIMARY KEY (id),
    FOREIGN KEY (brand_id)
        REFERENCES public.brand (id) MATCH SIMPLE
        ON UPDATE CASCADE
		ON DELETE SET NULL
        NOT VALID
);

CREATE TABLE public.site_region
(
    id serial,
    site_id integer,
	region_id integer,
	UNIQUE (site_id, region_id),
    PRIMARY KEY (id),
	FOREIGN KEY (site_id)
        REFERENCES public.site (id) MATCH SIMPLE
        ON UPDATE CASCADE
		ON DELETE SET NULL
        NOT VALID,
	FOREIGN KEY (region_id)
        REFERENCES public.region (id) MATCH SIMPLE
        ON UPDATE CASCADE
		ON DELETE SET NULL
        NOT VALID
);

CREATE TABLE public.site_fuel
(
	id serial,
	site_id integer,
	fuel_id integer,
    active boolean,
	UNIQUE (site_id, fuel_id),
	PRIMARY KEY (id),
	FOREIGN KEY (site_id)
        REFERENCES public.site (id) MATCH SIMPLE
        ON UPDATE CASCADE
		ON DELETE SET NULL
        NOT VALID
);

CREATE TABLE public.price
(
    id serial,
    site_id integer,
    fuel_id integer,
    collection_method character varying,
    amount integer,
    transaction_date timestamp with time zone,
    active boolean,
	UNIQUE (site_id, fuel_id, transaction_date),
    PRIMARY KEY (id),
    FOREIGN KEY (site_id)
        REFERENCES public.site (id) MATCH SIMPLE
        ON UPDATE CASCADE
		ON DELETE SET NULL
        NOT VALID,
    FOREIGN KEY (fuel_id)
        REFERENCES public.fuel (id) MATCH SIMPLE
        ON UPDATE CASCADE
		ON DELETE SET NULL
        NOT VALID
);