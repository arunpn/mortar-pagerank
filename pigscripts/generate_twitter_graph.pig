/*
 * The full Twitter graph data we are using is 1.5B edges in a dataset from 2010,
 * so we take only edges between the top N users. Running pagerank on this
 * subset has the effect of finding "who do influential people think is important".
 */

%default EDGES_INPUT_PATH 's3n://mortar-example-data/twitter-pagerank/twitter_user_graph/*.gz'
%default USERNAMES_INPUT_PATH 's3n://mortar-example-data/twitter-pagerank/twitter_usernames.gz'

%default EDGES_OUTPUT_PATH 's3n://jpacker-dev/twitter-pagerank/twitter_influential_user_graph.gz'
-- %default EDGES_OUTPUT_PATH 's3n://mortar-example-output-data/twitter-pagerank/twitter_influential_user_graph.gz'

%default N 100000

edges                   =   LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
                                AS (user: chararray, follower: chararray);
usernames               =   LOAD '$USERNAMES_INPUT_PATH' USING PigStorage(' ') 
                                AS (user: chararray, username: chararray);

-- find the users with the most followers
edges_by_user           =   GROUP edges BY user;
users                   =   FOREACH edges_by_user GENERATE 
                                group AS user, 
                                COUNT(edges) AS num_followers;
users_ordered           =   ORDER users BY num_followers DESC;
influential_users       =   LIMIT users_ordered $N;

-- find the usernames of the influential users
influential_user_ids    =   FOREACH influential_users GENERATE user;
influential_users_jnd   =   JOIN influential_user_ids BY user, usernames BY user;
influential_usernames   =   FOREACH influential_users_jnd GENERATE
                                influential_user_ids::user AS user,
                                usernames::username AS username;

-- find edges where both the followed user and the following user are influential
-- also replace user-ids with usernames
edges_jnd_1             =   JOIN edges BY user, influential_usernames BY user;
followed_is_influential =   FOREACH edges_jnd_1 GENERATE influential_usernames::username AS user, edges::follower AS follower;
edges_jnd_2             =   JOIN followed_is_influential BY follower, influential_usernames BY user;
relevant_edges          =   FOREACH edges_jnd_2 GENERATE
                                influential_usernames::username AS from,
                                followed_is_influential::user AS to,
                                1.0 AS weight: double;

rmf $EDGES_OUTPUT_PATH;
STORE relevant_edges INTO '$EDGES_OUTPUT_PATH' USING PigStorage();
