from pagerank_lib import Pagerank

# Your input data should be a directed graph with the schema "from, to, weight" and a tab delimiter.
#
# "from" and "to" can be of datatypes int, long, chararray, and bytearray.
# "weight" can be of datatypes int, long, float, and double.
#
# If your graph is undirected, you must add two edges for each connection, ex. A to B and B to A
# If your graph is unweighted, simply set each weight to 1.0
#
# If your input data is small (< 250 MB or so), you can configure this script to run in Mortar's "local mode".
# Local mode installs Pig and Jython so you can run your script on your local machine.
# This allows you to run your Mortar project for free and avoids the overhead time of Hadoop scheduling jobs on a cluster.
#
# If you want to run in local mode, download your data and change the following paths to local filesystem paths:
#     EDGES_INPUT, TEMPORARY_OUTPUT_PREFIX, OUTPUT_PATH
#
# When you have finished configuring this file, you can run the script on a cluster using the command:
#     mortar run my-pagerank -s N
# or run it locally using the command:
#     mortar local:run my-pagerank
#
EDGES_INPUT = "s3n://my-bucket/path/to/my/graph"

# Iteration Parameters
DAMPING_FACTOR        = 0.85    # 1.0 - DAMPING_FACTOR is the change of teleporting to a random node at each iteration step
                                # (instead of following a link). Iteration will converge faster if this is set lower,
                                # but the output pageranks will then reflect less of the true link structure of the graph.
CONVERGENCE_THRESHOLD = 0.0005  # See README.md for an explanation of the convergence metric. Your use case may warrant
                                # setting this higher (faster, less accurate) or lower (slower, more accurate).
MAX_NUM_ITERATIONS    = 20      # Always stop after this number of iterations regardless of convergence.

# Temporary data is stored into HDFS for better performance.
TEMPORARY_OUTPUT_PREFIX = "hdfs:///my-pagerank"

# Final output is stored to this location.
OUTPUT_PATH = "s3n://my-bucket/output/path"

if __name__ == "__main__":
    pagerank = Pagerank(EDGES_INPUT,
                        damping_factor=DAMPING_FACTOR,
                        convergence_threshold=CONVERGENCE_THRESHOLD,
                        max_num_iterations=MAX_NUM_ITERATIONS,
                        temporary_output_prefix=TEMPORARY_OUTPUT_PREFIX,
                        output_path=OUTPUT_PATH)
    pagerank.run_pagerank()
