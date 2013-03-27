%default OUTPUT_PATH 's3n://$OUTPUT_BUCKET/$MORTAR_EMAIL_S3_ESCAPED/$OUTPUT_FOLDER'

final_pageranks     =   LOAD '$PAGERANKS_INPUT_PATH' USING PigStorage() AS (node: chararray, pagerank: double);
ordered             =   ORDER final_pageranks BY pagerank DESC;
top_nodes           =   LIMIT ordered $TOP_N;

rmf $OUTPUT_PATH;
STORE top_nodes INTO '$OUTPUT_PATH' USING PigStorage();
