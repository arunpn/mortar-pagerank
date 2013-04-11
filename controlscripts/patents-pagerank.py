from pagerank_lib import Pagerank

# A directed graph with the schema "from, to, weight" and a tab delimiter.
EDGES_INPUT = "s3n://mortar-example-data/patents-pagerank/patent_organization_citation_graph"

# Iteration Parameters -- see README.md for more information
DAMPING_FACTOR        = 0.7
CONVERGENCE_THRESHOLD = 0.0001
MAX_NUM_ITERATIONS    = 20

# Temporary data is stored in HDFS for better performance
TEMPORARY_OUTPUT_PREFIX = "hdfs:///patents-pagerank"

# By default, final output is sent to the S3 bucket mortar-example-output-data,
# in a special directory permissioned for your account.
# See my-pagerank.py for an example of outputting to your own S3 bucket.

if __name__ == "__main__":
    pagerank = Pagerank(EDGES_INPUT,
                        damping_factor=DAMPING_FACTOR,
                        convergence_threshold=CONVERGENCE_THRESHOLD,
                        max_num_iterations=MAX_NUM_ITERATIONS,
                        temporary_output_prefix=TEMPORARY_OUTPUT_PREFIX)
    pagerank.run_pagerank()
