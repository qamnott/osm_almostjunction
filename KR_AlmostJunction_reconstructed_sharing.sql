/*
Reconstructed SQL scripts for "Almost Junction" rule of KeepRight
Adapted from: https://github.com/keepright/keepright/blob/master/checks/0050_almost-junctions.php
By Sam A. T.
Date: June 2019
---
Input: table "ways, nodes" of imported OSM Extract in PostgreSQL with PostGIS extenstion
*/

DROP TABLE IF EXISTS _tmp_ways;
CREATE UNLOGGED TABLE _tmp_ways (
	way_id bigint NOT NULL,
	first_node_id bigint,
	last_node_id bigint,
	layer text DEFAULT '0',
	PRIMARY KEY (way_id));
SELECT AddGeometryColumn('_tmp_ways', 'geom', 3857, 'LINESTRING', 2);

INSERT INTO _tmp_ways (way_id, first_node_id, last_node_id, geom)
SELECT w.id, w.nodes[1] AS first_node_id, w.nodes[array_length(nodes,1)] AS last_node_id, ST_Transform(w.geom, 3857) AS geom
FROM ways AS w
WHERE w.geom IS NOT NULL AND w.tags?'highway' AND NOT avals(w.tags) && ARRAY['construction','proposed','platform'];

CREATE INDEX idx_tmp_ways_geom ON _tmp_ways USING gist (geom);
CREATE INDEX idx_tmp_ways_first_node_id ON _tmp_ways (first_node_id);
CREATE INDEX idx_tmp_ways_last_node_id ON _tmp_ways (last_node_id);

ANALYZE _tmp_ways;


UPDATE _tmp_ways
		SET layer = '1'
		FROM ways
		WHERE ways.id = _tmp_ways.way_id AND
		(tags?'bridge' AND tags->'bridge' NOT IN ('no', 'false', '0'))

UPDATE _tmp_ways
		SET layer = '-1'
		FROM ways
		WHERE ways.id = _tmp_ways.way_id AND
		(tags?'tunnel' AND tags->'tunnel' NOT IN ('no', 'false', '0'))

DROP TABLE IF EXISTS _tmp_end_nodes;
CREATE UNLOGGED TABLE _tmp_end_nodes (
	way_id bigint NOT NULL,
	node_id bigint NOT NULL,
	x double precision,
	y double precision,
	layer text DEFAULT '0',
	PRIMARY KEY (node_id));

SELECT AddGeometryColumn('_tmp_end_nodes', 'geom', 3857, 'POINT', 2);

INSERT INTO _tmp_end_nodes (way_id, node_id, layer)
	SELECT w.way_id, w.first_node_id, w.layer
	FROM 
		_tmp_ways AS w 
		INNER JOIN way_nodes AS wn 
		ON w.first_node_id=wn.node_id
	GROUP BY w.way_id, w.first_node_id, w.layer
	HAVING COUNT(wn.way_id)=1;

INSERT INTO _tmp_end_nodes (way_id, node_id, layer)
	SELECT w.way_id, w.last_node_id, w.layer
	FROM 
		_tmp_ways AS w 
		INNER JOIN way_nodes AS wn 
		ON w.last_node_id=wn.node_id
	WHERE NOT EXISTS (
		SELECT 1 FROM _tmp_end_nodes AS tmp
		WHERE tmp.node_id=w.last_node_id
	)
	GROUP BY w.way_id, w.last_node_id, w.layer
	HAVING COUNT(wn.way_id)=1;

ANALYZE _tmp_end_nodes;

DELETE FROM _tmp_end_nodes AS en
	WHERE en.node_id IN (
		SELECT id
		FROM nodes AS t
		WHERE (
			(t.tags?'noexit' AND avals(t.tags) && ARRAY['yes', 'true', '1']) OR
			(t.tags?'highway' AND avals(t.tags) && ARRAY['turning_circle']) OR
			(t.tags?'highway' AND avals(t.tags) && ARRAY['bus_stop']) OR
			t.tags?'amenity'
		)
	);

DELETE FROM _tmp_end_nodes AS en
	WHERE en.way_id IN (
		SELECT id
		FROM ways AS t
		WHERE t.tags?'noexit' AND (avals(t.tags) && ARRAY['yes', 'true', '1'])
	);

UPDATE _tmp_end_nodes en
	SET geom = ST_Transform(n.geom, 3857), x = ST_X(ST_Transform(n.geom, 3857)), y = ST_Y(ST_Transform(n.geom, 3857))
	FROM nodes n
	WHERE en.node_id=n.id;

CREATE INDEX idx_tmp_end_nodes_geom ON _tmp_end_nodes USING gist (geom);

ANALYZE _tmp_end_nodes;

DROP TABLE IF EXISTS _tmp_barriers;
CREATE UNLOGGED TABLE _tmp_barriers (
	way_id bigint NOT NULL,
	layer text DEFAULT '0',
	PRIMARY KEY (way_id)
	);	
SELECT AddGeometryColumn('_tmp_barriers', 'geom', 3857, 'LINESTRING', 2);

INSERT INTO _tmp_barriers (way_id, geom)
	SELECT w.id, ST_Transform(w.geom, 3857)
	FROM ways AS w
	WHERE w.geom IS NOT NULL AND w.tags?'barrier' ;

CREATE INDEX idx_tmp_barriers_geom ON _tmp_barriers USING gist (geom);

ANALYZE _tmp_barriers;

DROP TABLE IF EXISTS _tmp_error_candidates;
CREATE UNLOGGED TABLE _tmp_error_candidates (
		way_id bigint NOT NULL,
		node_id bigint NOT NULL,
		node_x double precision NOT NULL,
		node_y double precision NOT NULL,
		nearby_way_id bigint NOT NULL,
		distance double precision NOT NULL);

VACUUM FULL _tmp_end_nodes;
VACUUM FULL _tmp_ways;
VACUUM FULL _tmp_barriers;


INSERT INTO _tmp_error_candidates (way_id, node_id, node_x, node_y, nearby_way_id, distance)
	SELECT 
		en.way_id, en.node_id, en.x AS node_x, en.y AS node_y, 
		w.way_id AS nearby_way_id, 
		ST_distance(w.geom, en.geom) AS distance
	FROM 
		_tmp_end_nodes AS en
		INNER JOIN 
		_tmp_ways AS w 
		ON 
			ST_DWithin(w.geom, en.geom, 10.0) 
			AND en.way_id<>w.way_id 
			AND en.layer=w.layer
	LEFT JOIN _tmp_barriers AS b 
	ON 
		b.layer=en.layer 
		AND 
		ST_Intersects(b.geom, ST_ShortestLine(w.geom, en.geom))
	WHERE 
		b.way_id IS NULL AND
		NOT EXISTS(
			SELECT 1
			FROM nodes AS nt
			WHERE nt.id=en.node_id AND (
				nt.tags?'barrier' OR
				(nt.tags?'railway' AND avals(nt.tags) && ARRAY['subway_entrance'])
			)
	);

INSERT INTO _tmp_error_candidates (way_id, node_id, node_x, node_y, nearby_way_id, distance)
	SELECT 
		en1.way_id, en1.node_id, en1.x AS node_x, en1.y AS node_y, 
		en2.way_id AS nearby_way_id, 
		ST_distance(en2.geom, en1.geom) AS distance
	FROM 
		_tmp_end_nodes en1, _tmp_end_nodes en2
	WHERE 
		ST_DWithin(en1.geom, en2.geom, 10.0) 
		AND en1.way_id<>en2.way_id 
		AND en1.layer<>en2.layer
		AND NOT EXISTS (
			SELECT 1
			FROM _tmp_barriers b
			WHERE 
				b.layer IN (en1.layer, en2.layer) 
				AND ST_Intersects(b.geom, ST_ShortestLine(en1.geom, en2.geom))
		) 
		AND NOT EXISTS(
			SELECT 1
			FROM nodes AS nt
				WHERE nt.id=en1.node_id AND (
					nt.tags?'barrier' OR
					(nt.tags?'railway' AND avals(nt.tags) && ARRAY['subway_entrance'])
				)
    	);
		
		
CREATE INDEX idx_tmp_error_candidates_way_id ON _tmp_error_candidates (way_id);
CREATE INDEX idx_tmp_error_candidates_nearby_way_id ON _tmp_error_candidates (nearby_way_id);
CREATE INDEX idx_tmp_error_candidates_node ON _tmp_error_candidates (node_id, distance);

ANALYZE _tmp_error_candidates;


DROP TABLE IF EXISTS _tmp_error_candidates2;
CREATE UNLOGGED TABLE _tmp_error_candidates2 (
		ID serial NOT NULL PRIMARY KEY,
		node_id bigint NOT NULL,
		nearby_way_id bigint NOT NULL,
		distance double precision NOT NULL);

DROP TABLE IF EXISTS _tmp_way_nodes;
CREATE UNLOGGED TABLE _tmp_way_nodes AS (
	SELECT wn.*, ST_X(ST_Transform(n.geom, 3857)) AS x, ST_Y(ST_Transform(n.geom, 3857)) AS y
	FROM way_nodes AS wn
	JOIN nodes AS n
	ON (wn.node_id = n.id)
);
CREATE INDEX idx_tmp_way_nodes_way_id ON _temp_way_nodes (way_id);
CREATE INDEX idx_tmp_way_nodes_node_id ON _temp_way_nodes (node_id);


INSERT INTO _tmp_error_candidates2 (node_id, nearby_way_id, distance)
	SELECT node_id, nearby_way_id, distance
	FROM _tmp_error_candidates AS C
	WHERE NOT EXISTS (
		SELECT 1
		FROM 
			(SELECT * FROM _temp_way_nodes 
				WHERE way_id=C.nearby_way_id) AS wn1 
		INNER JOIN 
			(SELECT * FROM _temp_way_nodes 
				WHERE way_id=C.way_id) AS wn2 
		USING (node_id)
		WHERE (wn1.x - C.node_x) ^ 2 + (wn1.y - C.node_y) ^ 2 <= (3*10.0) ^ 2
	);

DROP TABLE IF EXISTS _tmp_errors;
CREATE UNLOGGED TABLE _tmp_errors AS (
	SELECT 
		'Almost_Junction' AS error_type, 
		'node' AS object_type, 
		C.node_id AS object_id, 
		C.distance AS distance, 
		C.nearby_way_id AS txt1, 
		NOW() AS last_checked
	FROM _tmp_error_candidates2 C
	WHERE ID=(
		SELECT ID
		FROM _tmp_error_candidates2 T
		WHERE T.node_id=C.node_id
		ORDER BY C.distance
		LIMIT 1
	)
);