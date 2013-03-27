-- Adapted from Alan Gates' Programming Pig - http://ofps.oreilly.com/titles/9781449302641/embedding.html

previous_pageranks      =   LOAD '$INPUT_PATH' USING PigStorage()
                            AS (node: chararray, 
                                pagerank: double, 
                                edges: {t: (to: chararray, weight: double)},
                                sum_of_edge_weights: double);

outbound_pageranks_temp =   FOREACH previous_pageranks GENERATE
                                FLATTEN(edges) AS (to: chararray, weight: double),
                                sum_of_edge_weights,
                                pagerank AS source_pagerank;

outbound_pageranks      =   FOREACH outbound_pageranks_temp GENERATE
                                to, (weight / sum_of_edge_weights) * source_pagerank AS pagerank;

cogrouped               =   COGROUP previous_pageranks BY node, outbound_pageranks BY to;
new_pageranks           =   FOREACH cogrouped GENERATE
                                group AS node,
                                ((1.0 - $DAMPING_FACTOR) / $NUM_NODES)
                                    + $DAMPING_FACTOR * SUM(outbound_pageranks.pagerank) AS pagerank,
                                FLATTEN(previous_pageranks.edges) AS edges,
                                FLATTEN(previous_pageranks.sum_of_edge_weights) AS sum_of_edge_weights, 
                                FLATTEN(previous_pageranks.pagerank) AS previous_pagerank;

no_nulls                =   FILTER new_pageranks BY pagerank is not null AND previous_pagerank is not null;
pagerank_diffs          =   FOREACH no_nulls GENERATE ABS(pagerank - previous_pagerank);
max_diff                =   FOREACH (GROUP pagerank_diffs ALL) GENERATE MAX($1);

rmf $PAGERANKS_OUTPUT_PATH;
rmf $MAX_DIFF_OUTPUT_PATH;
STORE new_pageranks INTO '$PAGERANKS_OUTPUT_PATH' USING PigStorage();
STORE max_diff INTO '$MAX_DIFF_OUTPUT_PATH' USING PigStorage();
