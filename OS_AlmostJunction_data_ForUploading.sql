/*
Reconstructed SQL scripts for "Almost Junction" rule of Osmose
Adapted from: https://github.com/osm-fr/osmose-backend/blob/master/analysers/analyser_osmosis_highway_almost_junction.py
By Sam A. T.
Date: June 2019
---
Input: table "ways, nodes" of imported OSM Extract in PostgreSQL with PostGIS extenstion
*/

DROP TABLE IF EXISTS highways;
CREATE UNLOGGED TABLE highways AS
SELECT
    id,
    nodes,
    tags,
    tags->'highway' AS highway,
    geom as linestring,
    ST_Transform(geom, 3857) AS linestring_proj,
    0 as is_polygon,
    tags->'highway' LIKE '%_link' AS is_link,
    (tags?'junction' AND tags->'junction' = 'roundabout') AS is_roundabout,
    (tags?'oneway' AND tags->'oneway' IN ('yes', 'true', '1', '-1')) AS is_oneway,
    CASE tags->'highway'
        WHEN 'motorway' THEN 1
        WHEN 'primary' THEN 1
        WHEN 'trunk' THEN 1
        WHEN 'motorway_link' THEN 2
        WHEN 'primary_link' THEN 2
        WHEN 'trunk_link' THEN 2
        WHEN 'secondary' THEN 2
        WHEN 'secondary_link' THEN 2
        WHEN 'tertiary' THEN 3
        WHEN 'tertiary_link' THEN 3
        WHEN 'unclassified' THEN 4
        WHEN 'unclassified_link' THEN 4
        WHEN 'residential' THEN 4
        WHEN 'residential_link' THEN 4
        WHEN 'living_street' THEN 5
        WHEN 'track' THEN 5
        WHEN 'cycleway' THEN 5
        WHEN 'service' THEN 5
        WHEN 'road' THEN 5
        ELSE NULL
    END AS level
FROM
    ways
WHERE
    tags != ''::hstore AND
    tags?'highway' AND
    tags->'highway' NOT IN ('services', 'planned', 'proposed', 'construction', 'rest_area', 'razed', 'no') AND
    (NOT tags?'area' OR tags->'area' = 'no') AND
    ST_NPoints(geom) >= 2;

CREATE INDEX idx_highways_linestring ON highways USING gist(linestring);
CREATE INDEX idx_highways_linestring_proj ON highways USING gist(linestring_proj);
CREATE INDEX idx_highways_id ON highways(id);
CREATE INDEX idx_highways_highway ON highways(highway);

CREATE OR REPLACE FUNCTION ends(nodes bigint[]) RETURNS SETOF bigint AS $$
DECLARE BEGIN
    RETURN NEXT nodes[1];
    RETURN NEXT nodes[array_length(nodes,1)];
    RETURN;
END
$$ LANGUAGE plpgsql
   IMMUTABLE
   RETURNS NULL ON NULL INPUT;


DROP TABLE IF EXISTS way_ends;
CREATE UNLOGGED TABLE way_ends AS
SELECT
	t.id,
	t.nid,
	t.nodes,
	geom AS ogeom,
	ST_Transform(nodes.geom, 3857) AS geom
FROM (
	SELECT
		t.id,
		t.nid,
		t.nodes
	FROM (
		SELECT
			id,
			linestring,
			ends(nodes) AS nid,
			nodes
		FROM
			highways
		WHERE
			highway NOT IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'service', 'footway', 'platform', 'steps') AND
			ST_Length(linestring_proj) > 10
		) AS t
    LEFT JOIN highways ON
		highways.id != t.id AND
		highways.linestring && t.linestring AND
		t.nid = ANY(highways.nodes)
		WHERE
			highways.id IS NULL
	) as t
  JOIN nodes as nodes ON
    nodes.id = t.nid AND
    NOT (
		nodes.tags?'noexit' OR
		(nodes.tags?'highway' AND nodes.tags->'highway' IN ('turning_circle', 'bus_stop')) OR
		(nodes.tags?'railway' AND nodes.tags->'railway' IN ('subway_entrance')) OR
		nodes.tags?'amenity' OR
		nodes.tags?'barrier');

SELECT DISTINCT
	way_ends.id,
	way_ends.nid,
	ST_AsText(way_ends.ogeom),
	way_ends.ogeom
FROM
	way_ends
	JOIN highways ON
		ST_DWithin(way_ends.geom, highways.linestring_proj, 10) AND
		highways.id != way_ends.id AND
		NOT way_ends.nodes && highways.nodes AND
		NOT highways.tags ?| ARRAY ['tunnel', 'bridge']
	LEFT JOIN highways AS h2 ON
		h2.linestring && highways.linestring AND
		h2.nodes && highways.nodes AND
		h2.id != highways.id AND
		h2.id != way_ends.id AND
		way_ends.nodes && h2.nodes
WHERE
	h2.id IS NULL
