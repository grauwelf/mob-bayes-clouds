CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS btree_gist CASCADE;

CREATE SCHEMA IF NOT EXISTS clouds;

-- STAGE 0: Create necessary functions and tables

-- DROP FUNCTION clouds.construct_TTB
CREATE OR REPLACE FUNCTION clouds.construct_TTB(
    origin GEOMETRY,
    inner_radius NUMERIC,
    outer_radius NUMERIC,
    start_angle NUMERIC,
    stop_angle NUMERIC)
RETURNS GEOMETRY
AS $$
DECLARE
    bisec_angle NUMERIC;
BEGIN
        bisec_angle = 0.5*(stop_angle + start_angle);
        IF ABS(stop_angle - start_angle) > 120 THEN bisec_angle := (bisec_angle + 180) % 360; END IF;

        RETURN ST_GeomFromText(
        'CURVEPOLYGON(COMPOUNDCURVE(CIRCULARSTRING(' ||
            CAST(inner_radius * COS(RADIANS(start_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(inner_radius * SIN(RADIANS(start_angle)) + ST_Y(origin) AS VARCHAR)  || ', ' ||
            CAST(inner_radius * COS(RADIANS(bisec_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(inner_radius * SIN(RADIANS(bisec_angle)) + ST_Y(origin) AS VARCHAR) || ', ' ||
            CAST(inner_radius * COS(RADIANS(stop_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(inner_radius * SIN(RADIANS(stop_angle)) + ST_Y(origin) AS VARCHAR)  || '), ' ||
        'LINESTRING(' ||
            CAST(inner_radius * COS(RADIANS(stop_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(inner_radius * SIN(RADIANS(stop_angle)) + ST_Y(origin) AS VARCHAR)  || ', ' ||
            CAST(outer_radius * COS(RADIANS(stop_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(outer_radius * SIN(RADIANS(stop_angle)) + ST_Y(origin) AS VARCHAR)  || '), ' ||
        'CIRCULARSTRING(' ||
            CAST(outer_radius * COS(RADIANS(stop_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(outer_radius * SIN(RADIANS(stop_angle)) + ST_Y(origin) AS VARCHAR)  || ', ' ||
            CAST(outer_radius * COS(RADIANS(bisec_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(outer_radius * SIN(RADIANS(bisec_angle)) + ST_Y(origin) AS VARCHAR) || ', ' ||
            CAST(outer_radius * COS(RADIANS(start_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(outer_radius * SIN(RADIANS(start_angle)) + ST_Y(origin) AS VARCHAR)  || '), ' ||
        'LINESTRING(' ||
            CAST(outer_radius * COS(RADIANS(start_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(outer_radius * SIN(RADIANS(start_angle)) + ST_Y(origin) AS VARCHAR)  || ', ' ||
            CAST(inner_radius * COS(RADIANS(start_angle)) + ST_X(origin) AS VARCHAR) || ' ' ||
            CAST(inner_radius * SIN(RADIANS(start_angle)) + ST_Y(origin) AS VARCHAR)  || ')))',
        2039);
END
$$ LANGUAGE plpgsql;

-- Create place for table containing spatial grid.
-- Grid is copied from external data source for compatibility.
-- DROP TABLE clouds.grid250;
CREATE TABLE clouds.grid250 (
    element_id serial,
    x_min bigint,
    y_max bigint,
    x_max bigint,
    y_min bigint,
    grid_element geometry(Polygon, 2039)
);

COPY clouds.grid250
FROM 'data/grid250.csv'
WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);

-- Indexing to speed up
CREATE INDEX sidx_elements_250
ON clouds.grid250
USING gist(grid_element);

-- Create place for the antennas dataset supplied by
-- mobile phone operator company
-- DROP TABLE clouds.antennas_staging
CREATE TABLE IF NOT EXISTS clouds.antennas_staging (
    cell_id    varchar(127),
    cgi_hex    varchar(127),
    location2039 geometry(Point,2039),
    longitude    float8,
    latitude    float8,
    azimuth    int8,
    start_angle    int8,
    stop_angle    int8
);

COPY clouds.antennas_staging
FROM 'data/antennas_data.csv'
WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);

-- Prepare place for the PRACH distributions supplied by
-- mobile phone operator company
-- DROP TABLE antennas_prach_staging;
CREATE TABLE clouds.antennas_prach_staging (
    cell_id varchar(127),
    distance NUMERIC,
    prach numeric
);

COPY clouds.antennas_prach_staging
FROM 'data/prach_data.csv'
WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);

-- DROP TABLE antennas_prach;
CREATE TABLE clouds.antennas_prach (
    cell_id    varchar(127),
    inner_radius NUMERIC,
    outer_radius NUMERIC,
    prach NUMERIC
);

-- STAGE 1: Preparing data for calculations

-- Build table with PRACH distribution by TTBs for each antenna
INSERT INTO clouds.antennas_prach
SELECT
    cell_id,
    CASE
        WHEN
        nth_value(distance, 1) OVER (
            PARTITION BY cell_id
            ORDER BY distance
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
        - GREATEST (0, nth_value(distance, 1) OVER (
            PARTITION BY cell_id
            ORDER BY distance
            ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
        )) = 0
        THEN 0
        ELSE nth_value(distance, 1) OVER (
            PARTITION BY cell_id
            ORDER BY distance
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
    END AS inner_radius,
    nth_value(distance, 1) OVER (
        PARTITION BY cell_id
        ORDER BY distance
        ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
    ) as outer_radius,
    nth_value(prach, 1) OVER (
        PARTITION BY cell_id
        ORDER BY distance
        ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
    ) as prach
FROM clouds.antennas_prach_staging;

-- Create table for TTBs of the all antennas
-- DROP TABLE clouds.antennas;
CREATE TABLE IF NOT EXISTS clouds.antennas (
    aid serial PRIMARY KEY,
    cgi_hex    varchar(127),
    cell_id    varchar(127),
    azimuth    int8,
    start_angle    int8,
    stop_angle    int8,
    location2039    geometry(POINT, 2039),
    longitude    float8,
    latitude    float8,
    inner_radius    NUMERIC,
    outer_radius    NUMERIC,
    connection_count    NUMERIC,
    connection_density numeric,
    coverage_area NUMERIC,
    TTB geometry
);

INSERT INTO clouds.antennas (
    cgi_hex, cell_id,
    azimuth, start_angle, stop_angle,
    location2039, longitude, latitude,
    inner_radius, outer_radius,
    connection_count,
    connection_density,
    coverage_area,
    TTB
)
SELECT
    cgi_hex, cell_id,
    azimuth, stop_angle AS start_angle, start_angle AS stop_angle,
    location2039, longitude, latitude,
    inner_radius, outer_radius,
    prach AS connection_count,
    prach / ABS(PI() * (stop_angle - start_angle) * (outer_radius * outer_radius - inner_radius * inner_radius) / 360),
    ABS(PI() * (stop_angle - start_angle) * (outer_radius * outer_radius - inner_radius * inner_radius) / 360),
    clouds.construct_TTB(location2039, inner_radius, outer_radius, start_angle, stop_angle)
FROM clouds.antennas_staging
    LEFT JOIN clouds.antennas_prach USING(cell_id);

-- Create spatial indexies for antennas' location and TTBs
CREATE INDEX sidx_antennas_location2039
ON clouds.antennas
USING gist (location2039);

CREATE INDEX sidx_antennas_ttb
ON clouds.antennas
USING gist(TTB);

CREATE INDEX ttb_density_idx
ON clouds.antennas
USING gist(TTB, connection_density);

DROP TABLE clouds.antennas_staging CASCADE;
DROP TABLE clouds.antennas_prach_staging CASCADE;
DROP TABLE clouds.antennas_prach CASCADE;

-- STAGE 2: Calculation of intersections between grid and TTBS
-- Threshold value 0.00016 means 'at least 10 connections per
-- grid element for a month period'.
SELECT
    antennas.aid,
    antennas.cell_id,
    antennas.TTB,
    antennas.connection_density,
    grid250.element_id,
    grid250.grid_element,
    CASE
    WHEN ST_COVEREDBY(grid_element, TTB)
        THEN ST_AREA(grid_element)
    WHEN ST_COVEREDBY(TTB, grid_element)
        THEN ST_AREA(TTB)
    ELSE
        ST_AREA(ST_INTERSECTION(TTB, grid_element))
    END AS intersection_area,
    0.0 AS total_conn_in_element
INTO bayes.ttb_grid_intersections
FROM bayes.antennas
    INNER JOIN bayes.grid250
    ON (bayes.antennas.connection_density >= 0.00016::numeric) AND ST_INTERSECTS(TTB, grid_element);

-- Indexing to speed up
DROP INDEX IF EXISTS element_idx;
CREATE INDEX element_idx
ON bayes.ttb_grid_intersections
USING btree(element_id);

-- Temporary table for calculation of the overall connections per grid_element
SELECT
    element_id,
    SUM(connection_density * intersection_area) AS conn_in_element
INTO bayes.tmp_intersections
FROM bayes.ttb_grid_intersections
GROUP BY element_id;

DROP INDEX IF EXISTS tmp_element_idx;
CREATE INDEX tmp_element_idx
ON bayes.tmp_intersections
USING btree(element_id);

UPDATE bayes.ttb_grid_intersections
SET total_conn_in_element = bayes.tmp_intersections.conn_in_element
FROM bayes.tmp_intersections
WHERE bayes.ttb_grid_intersections.element_id = bayes.tmp_intersections.element_id;

DROP TABLE bayes.tmp_intersections CASCADE;

-- Indexing to speed up
DROP INDEX IF EXISTS TTB_element_idx;
CREATE INDEX TTB_element_idx
ON bayes.ttb_grid_intersections
USING gist(TTB, grid_element);

DROP INDEX IF EXISTS TTB_grid_element_density_idx;
CREATE INDEX TTB_grid_element_density_idx
ON bayes.ttb_grid_intersections
USING hash(connection_density);

DROP INDEX IF EXISTS cell_idx;
CREATE INDEX cell_idx
ON bayes.ttb_grid_intersections
USING hash(cell_id);

DROP INDEX IF EXISTS cell_element_idx;
CREATE INDEX cell_element_idx
ON bayes.ttb_grid_intersections
USING btree(cell_id, element_id);

-- STAGE 3: Calculation of the Bayesian estimates

-- Calculation of the a posteriori probabilities P(A_j | Delta_i)
SELECT
    cell_id,
    element_id,
    SUM(POW(intersection_area, 2::double precision) * connection_density::double precision /
        (total_conn_in_element * 250::numeric * 250::numeric)::double precision) AS p_antenna_element,
    0.0::double precision AS p_element_antenna
INTO bayes.prob_antenna_grid_element
FROM bayes.ttb_grid_intersections
GROUP BY cell_id, element_id;

-- Estimation of P(Delta_i | A_j)
WITH overall_antennas_probs AS (
    SELECT
        bayes.prob_antenna_grid_element.cell_id,
        SUM(bayes.prob_antenna_grid_element.p_antenna_element) AS p_antenna_overall
    FROM bayes.prob_antenna_grid_element
    GROUP BY bayes.prob_antenna_grid_element.cell_id
), joined AS (
    SELECT *
    FROM bayes.prob_antenna_grid_element LEFT JOIN overall_antennas_probs
    USING(cell_id)
)
UPDATE bayes.prob_antenna_grid_element
SET p_element_antenna = joined.p_antenna_element / (joined.p_antenna_overall)
FROM joined
WHERE
    (bayes.prob_antenna_grid_element.cell_id = joined.cell_id)
    AND
    (bayes.prob_antenna_grid_element.element_id = joined.element_id);

-- Indexing to speed up
DROP INDEX IF EXISTS prob_element_idx;
CREATE INDEX prob_element_idx
ON bayes.prob_antenna_grid_element
USING hash(element_id);

DROP INDEX IF EXISTS prob_cell_idx;
CREATE INDEX prob_cell_idx
ON bayes.prob_antenna_grid_element
USING hash(cell_id);

DROP INDEX IF EXISTS prob_element_antenna_idx;
CREATE INDEX prob_element_antenna_idx
ON bayes.prob_antenna_grid_element
USING btree(p_element_antenna DESC);

-- Create function to get probabilistic cloud for a given antenna (by antenna's CELL_ID)
-- DROP FUNCTION bayes.get_cloud;
CREATE OR REPLACE FUNCTION bayes.get_cloud(target_cell_id VARCHAR, prob_cut NUMERIC)
RETURNS TABLE (
    cell_id VARCHAR,
    element_id INTEGER,
    p_antenna_element DOUBLE PRECISION,
    p_element_antenna DOUBLE PRECISION,
    probs_cumsum DOUBLE PRECISION
)
AS $$
DECLARE
BEGIN
    RETURN QUERY
    WITH cumulative_probs_table AS (
        SELECT
            probs_table.*,
            SUM(probs_table.p_element_antenna) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS probs_cumsum
        FROM (
            SELECT *
            FROM bayes.prob_antenna_grid_element
            WHERE bayes.prob_antenna_grid_element.cell_id = target_cell_id
            ORDER BY bayes.prob_antenna_grid_element.p_element_antenna DESC
        ) AS probs_table
    )
    SELECT *
    FROM cumulative_probs_table
    WHERE cumulative_probs_table.probs_cumsum <= prob_cut;
END
$$ LANGUAGE plpgsql;

-- Save map of 95-% probabilistic clouds
WITH ant AS (
    SELECT DISTINCT cell_id, cgi_hex
    FROM bayes.antennas
)
SELECT *
INTO bayes.prob_map095
FROM ant JOIN bayes.get_cloud(ant.cell_id, 0.95) USING (cell_id);

-- Indexing to speed up
CREATE INDEX cgihex_prob_map095_idx
ON bayes.prob_map095
USING hash(cgi_hex);

CREATE INDEX cell_prob_map095_idx
ON bayes.prob_map095
USING hash(cell_id);

CREATE INDEX element_prob_map095_idx
ON bayes.prob_map095
USING hash(element_id);

CREATE INDEX cumsum_prob_map095_idx
ON bayes.prob_map095
USING hash(probs_cumsum);

CREATE INDEX p_element_antenna_prob_map095_idx
ON bayes.prob_map095
USING hash(p_element_antenna);
