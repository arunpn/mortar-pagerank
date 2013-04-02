from pagerank_lib import Pagerank

# A directed graph with the schema "from, to, weight" and a tab delimiter.
EDGES_INPUT = "s3n://jpacker-dev/twitter-pagerank/twitter_influential_user_graph.gz"

# Iteration Parameters -- see README.md for more information
DAMPING_FACTOR        = 0.85
CONVERGENCE_THRESHOLD = 0.00125 # we set the convergence parameter higher than usual, for sake of speeding up the example
MAX_NUM_ITERATIONS    = 20

# Temporary data is stored in HDFS for better performance
TEMPORARY_OUTPUT_PREFIX = "hdfs:///twitter-pagerank"

# Final output is stored to S3.
OUTPUT_PATH = "s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter-pagerank"

if __name__ == "__main__":
    pagerank = Pagerank(EDGES_INPUT,
                        damping_factor=DAMPING_FACTOR,
                        convergence_threshold=CONVERGENCE_THRESHOLD,
                        max_num_iterations=MAX_NUM_ITERATIONS,
                        temporary_output_prefix=TEMPORARY_OUTPUT_PREFIX,
                        output_path=OUTPUT_PATH)
    pagerank.run_pagerank()
