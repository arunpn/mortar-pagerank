%default OUTPUT_PATH 's3n://$OUTPUT_BUCKET/$MORTAR_EMAIL_S3_ESCAPED/twitter-pagerank/pagerank'

final_pageranks     =   LOAD '$PAGERANKS_INPUT_PATH' USING PigStorage() AS (user: int, pagerank: double);
usernames           =   LOAD '$USERNAMES_INPUT_PATH' USING PigStorage(' ') AS (user: int, username: chararray);

joined              =   JOIN final_pageranks BY user, usernames BY user;
projected           =   FOREACH joined GENERATE usernames::username AS user, final_pageranks::pagerank AS pagerank;
ordered             =   ORDER projected BY pagerank DESC;
top_users           =   LIMIT ordered $TOP_N;

rmf $OUTPUT_PATH;
STORE top_users INTO '$OUTPUT_PATH' USING PigStorage();
