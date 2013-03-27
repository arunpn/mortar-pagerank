%default OUTPUT_PATH 's3n://$OUTPUT_BUCKET/$MORTAR_EMAIL_S3_ESCAPED/pagerank'
%default NODE_NAMES_INPUT_DELIMITER '\\t'

final_pageranks     =   LOAD '$PAGERANKS_INPUT_PATH' USING PigStorage() AS (node: chararray, pagerank: double);
node_names          =   LOAD '$NODE_NAMES_INPUT_PATH' USING PigStorage('$NODE_NAMES_INPUT_DELIMITER') 
                        AS (node: chararray, name: chararray);

joined              =   JOIN final_pageranks BY node, node_names BY node;
projected           =   FOREACH joined GENERATE node_names::name AS node, final_pageranks::pagerank AS pagerank;
ordered             =   ORDER projected BY pagerank DESC;
top_nodes           =   LIMIT ordered $TOP_N;

rmf $OUTPUT_PATH;
STORE top_nodes INTO '$OUTPUT_PATH' USING PigStorage();
