from pagerank_lib import Pagerank

# A directed graph with the schema "from, to, weight" and a tab delimiter.
#
# "from" and "to" can be of datatypes int, long, chararray, and bytearray.
# "weight" can be of datatypes int, long, float, and double.
#
# If your graph is undirected, you must add two edges for each connection, ex. A to B and B to A
# If your graph is unweighted, simply set each weight to 1.0
#
# If you want to run this script in local mode, it is advised that you download your data
# and change this path to a local filesystem path.
#
EDGES_INPUT                      = "s3n://my-bucket/path/to/my/graph"

# Iteration Parameters
DAMPING_FACTOR        = 0.85    # 1.0 - DAMPING_FACTOR is the change of teleporting to a random node at each iteration step
                                # (instead of following a link). Iteration will converge faster if this is set lower,
                                # but the output pageranks will then reflect less of the true link structure of the graph.
CONVERGENCE_THRESHOLD = 0.0001  # See README.md for an explanation of the convergence metric.
                                # We found this to be a reasonable default value, but you should decide
                                # how much accuracy is necessary for your use case.
MAX_NUM_ITERATIONS    = 20      # Always stop after this number of iterations regardless of convergence.

# Temporary data is stored in HDFS for better performance
# If you want to run this script in local mode, change the hdfs path to a local filesystem path
TEMPORARY_OUTPUT_PREFIX = "hdfs:///patents-pagerank"

# Final output is stored to s3
OUTPUT_BUCKET    = "mortar-example-output-data"
OUTPUT_DIRECTORY = "patents-pagerank"

if __name__ == "__main__":
    pagerank = Pagerank(EDGES_INPUT,
                        damping_factor=DAMPING_FACTOR,
                        convergence_threshold=CONVERGENCE_THRESHOLD,
                        max_num_iterations=MAX_NUM_ITERATIONS,
                        temporary_output_prefix=TEMPORARY_OUTPUT_PREFIX,
                        output_bucket=OUTPUT_BUCKET,
                        output_directory=OUTPUT_DIRECTORY)
    pagerank.run_pagerank()
