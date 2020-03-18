--
-- PostgreSQL database dump
--

-- Dumped from database version 12.2
-- Dumped by pg_dump version 12.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

-- CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO postgres;

--
-- TOC entry 2841 (class 0 OID 0)
-- Dependencies: 3
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA public IS 'standard public schema';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 202 (class 1259 OID 16459)
-- Name: prices; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.prices (
    "TransactionId" integer NOT NULL,
    "SiteId" integer NOT NULL,
    "Fuel_Type" character varying NOT NULL,
    "Price" integer NOT NULL,
    "TransactionDateutc" timestamp without time zone NOT NULL
);


ALTER TABLE public.prices OWNER TO postgres;

--
-- TOC entry 203 (class 1259 OID 16465)
-- Name: prices_TransactionId_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public."prices_TransactionId_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."prices_TransactionId_seq" OWNER TO postgres;

--
-- TOC entry 2842 (class 0 OID 0)
-- Dependencies: 203
-- Name: prices_TransactionId_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public."prices_TransactionId_seq" OWNED BY public.prices."TransactionId";


--
-- TOC entry 204 (class 1259 OID 16467)
-- Name: sites; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.sites (
    "SiteId" integer NOT NULL,
    "Site_Name" character varying,
    "Site_Brand" character varying,
    "Sites_Address_Line_1" character varying,
    "Site_Suburb" character varying,
    "Site_State" character varying,
    "Site_Post_Code" integer,
    "Site_Latitude" numeric,
    "Site_Longitude" numeric
);


ALTER TABLE public.sites OWNER TO postgres;

--
-- TOC entry 205 (class 1259 OID 16473)
-- Name: updates_per_station; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.updates_per_station WITH (security_barrier='false') AS
 SELECT sites."Site_Name",
    prices."SiteId",
    count(prices."SiteId") AS count
   FROM (public.prices
     JOIN public.sites ON ((prices."SiteId" = sites."SiteId")))
  WHERE ((prices."Fuel_Type")::text = 'e10'::text)
  GROUP BY sites."Site_Name", prices."SiteId"
  ORDER BY (count(prices."SiteId")) DESC;


ALTER TABLE public.updates_per_station OWNER TO postgres;

--
-- TOC entry 206 (class 1259 OID 16477)
-- Name: view_all_prices; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.view_all_prices AS
 SELECT sites."SiteId",
    sites."Site_Name",
    prices."Fuel_Type",
    prices."Price",
    prices."TransactionDateutc"
   FROM (public.sites
     JOIN public.prices ON ((sites."SiteId" = prices."SiteId")));


ALTER TABLE public.view_all_prices OWNER TO postgres;

--
-- TOC entry 2701 (class 2604 OID 16481)
-- Name: prices TransactionId; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.prices ALTER COLUMN "TransactionId" SET DEFAULT nextval('public."prices_TransactionId_seq"'::regclass);


--
-- TOC entry 2704 (class 2606 OID 16483)
-- Name: prices prices_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.prices
    ADD CONSTRAINT prices_pkey PRIMARY KEY ("TransactionId");


--
-- TOC entry 2706 (class 2606 OID 16485)
-- Name: sites sites_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sites
    ADD CONSTRAINT sites_pkey PRIMARY KEY ("SiteId");


--
-- TOC entry 2702 (class 1259 OID 16486)
-- Name: fki_sites; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fki_sites ON public.prices USING btree ("SiteId");


--
-- TOC entry 2707 (class 2606 OID 16487)
-- Name: prices sites; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.prices
    ADD CONSTRAINT sites FOREIGN KEY ("SiteId") REFERENCES public.sites("SiteId") NOT VALID;



--
-- PostgreSQL database dump complete
--

