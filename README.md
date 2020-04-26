# Bayesian estimate of position in mobile phone network

This code is used for the study of probabilistic based positioning of the
mobile phone devices. Applying Bayesian inference technique, we obtain a set of
grid cells - a discritized "cloud" of possible location of a device that is
registered at a given antenna.

Study is based on the information on 22K antennas of one of the cellular
cerrier that serves the entire area of Israel. For each antenna we possess
knowledge on its location (via the cell tower location), azimuth and monthly
PRACH curves, and, based on that, we have estimated a posteriori distributions
of each antenna’s connection. Each antenna of this MPN serves devices up to a
distance of 30-40 km, and its PRACH curve is presented by 40 circular
Trip-Time-Bands (TTBs). Coverage area was discretized into a grid of 250x250 m
cells. To reduce computational cost, we consider only those TTBs whose average
monthly density of connections is at least 10 per 250x250 m grid cell per
month, that is, 160 connections per 1 sq. km per month. This limitation
resulted in excluding 0.02% of all connections.

All calculations were performed in the PostgreSQL database with the use of the 
PostGIS GIS extension for performing spatial operations.

Used software: 
- PostgreSQL 11.5
- Postgres extensions:
    - PostGIS 2.5.2
    - btree_gist 1.5

## Publications
*The paper is submitted for the review*

## Authors
*Information is hidden until the end of the review process*

## Acknowledgments
*Information is hidden until the end of the review process*
