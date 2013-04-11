%default INPUT_PATH 's3n://mortar-example-data/pagerank/patents-pagerank/patent_data'
%default GRAPH_OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/patents-pagerank/patent_organization_citation_graph'

IMPORT '../macros/patents.pig';

patents             =   LOAD_PATENTS('$INPUT_PATH');

-- generate patent -> patent citations
citations           =   FOREACH patents GENERATE
                            application.doc_num AS id,
                            FLATTEN(citations.doc_num) AS citation,
                            FLATTEN(datafu.pig.bags.FirstTupleFromBag(assignees.orgname, null)) AS organization;

cits_with_org       =   FILTER citations BY organization is not null;
cits_with_org_copy  =   FOREACH cits_with_org GENERATE *;

-- generate organization -> organization citations
-- there will be many duplicates

joined              =   JOIN cits_with_org BY id, cits_with_org_copy BY citation;
org_links           =   FOREACH joined GENERATE
                            cits_with_org::organization AS from,
                            cits_with_org_copy::organization AS to;
filtered_org_links  =   FILTER org_links BY (SIZE(from) > 0 AND SIZE(to) > 0);

-- aggregate organization -> organization duplicate citations into weighted edges,
-- where weight = # citations from patents of organization "from" to patents of organization "to"

aggregate_org_links =   GROUP filtered_org_links BY (from, to);
edges               =   FOREACH aggregate_org_links GENERATE
                            group.$0 AS from,
                            group.$1 AS to,
                            COUNT($1) AS weight;

rmf $GRAPH_OUTPUT_PATH;
STORE edges INTO '$GRAPH_OUTPUT_PATH' USING PigStorage();