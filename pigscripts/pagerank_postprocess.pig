%default OUTPUT_PATH 's3n://$OUTPUT_BUCKET/$MORTAR_EMAIL_S3_ESCAPED/$OUTPUT_DIRECTORY'

-- project out the link data from the pagerank iteration output
final_pageranks     =   LOAD '$PAGERANKS_INPUT_PATH' USING PigStorage() AS (node: chararray, pagerank: double);
ordered             =   ORDER final_pageranks BY pagerank DESC;

rmf $OUTPUT_PATH;
STORE ordered INTO '$OUTPUT_PATH' USING PigStorage();
