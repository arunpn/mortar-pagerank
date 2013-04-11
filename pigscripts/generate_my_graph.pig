/*
 * Simple scenario:
 * You already have links for each object and don't have any weights.
 */

%default LINK_DATA_INPUT 's3n://my-bucket/path/to/my/data'
%default LINK_GRAPH_OUTPUT 's3n://my-bucket/path/where/output/graph/should/go'

link_data       =   LOAD '$LINK_DATA_INPUT' USING PigStorage()
                        AS (object_id: chararray,
                            links: {t: (object_id: charrarray)});

-- the field names must be "from", "to", and "weight" for the pagerank script to work
link_data_edges =  FOREACH link_data GENERATE
                        object_id AS from,
                        FLATTEN(links) AS to,
                        1.0 AS weight: float;

rmf $LINK_GRAPH_OUTPUT;
STORE link_data_edges INTO '$LINK_GRAPH_OUTPUT' USING PigStorage();

/*
 * More complex scenario:
 *
 * You have a set of events, where each event has an id and a bag of objects it involves.
 * You want the edges of your graph to be between objects which have been in the same events,
 * with weights equal to the number of events they've been in together.
 *
 * For example, events could be movies, and objects would be the actors in them;
 * or events could be academic papers and object would be their authors.
 */

%default EVENT_DATA_INPUT 's3n://my-bucket/path/to/my/data'
%default EVENT_GRAPH_OUTPUT 's3n://my-bucket/path/where/output/graph/should/go'

events              =   LOAD '$EVENT_DATA_INPUT' USING PigStorage()
                        AS (event_id: chararray, objects: {t: (object_id: chararray)});

flattened           =   FOREACH events GENERATE
                            event_id,
                            FLATTEN(objects) AS object_id;

-- we need to do a self join, so we generate a copy to have the data under two aliases
flattened_copy      =   FOREACH flattened GENERATE *;
joined              =   JOIN flattened BY event_id, flattened_copy BY event_id;

in_event_together   =   FOREACH joined GENERATE
                            flattened::object AS object_1,
                            flattened_copy::object AS object_2;
object_links        =   FILTER in_event_together BY (object_1 != object_2);

grouped             =   GROUP object_links BY (object_1, object_2);
event_data_edges    =   FOREACH grouped GENERATE
                            FLATTEN(group) AS (from, to),
                            COUNT(object_links) AS weight;
                            
rmf $EVENT_GRAPH_OUTPUT;
STORE event_data_edges INTO $EVENT_GRAPH_OUTPUT USING PigStorage();