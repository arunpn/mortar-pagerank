from pagerank_lib import Pagerank

# Your input data should be a directed graph with the schema "from, to, weight" and a tab delimiter.
#
# "from" and "to" can be of datatypes int, long, chararray, and bytearray.
# "weight" can be of datatypes int, long, float, and double.
#
# If your graph is undirected, you must add two edges for each connection, ex. A to B and B to A
# If your graph is unweighted, simply set each weight to 1.0
#
# If your input data is small (< 250 MB or so), you can configure this script to run in Mortar's "local mode"..
# Local mode will install Pig and Jython onto your local machine
# and run the script locally, which avoids the overhead of scheduling Hadoop jobs on a cluster.
# If you want to run in local mode, download your data and change the input path to a local filesystem path.
#
# When you have finished configuring this file, you can run the script on a cluster using the command:
#     mortar run my-pagerank -s N
# or run it locally using the command:
#     mortar local:run my-pagerank
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

# Temporary data is stored in HDFS for better performance.
# If running in local mode, change this to a local filesystem path.
TEMPORARY_OUTPUT_PREFIX = "hdfs:///patents-pagerank"

# Final output is stored to s3 by default.
# To run in local mode:
#     1) comment out references to OUTPUT_BUCKET and OUTPUT_DIRECTORY 
#     2) uncomment the references to LOCAL_OUTPUT_PATH
OUTPUT_BUCKET    = "mortar-example-output-data"
OUTPUT_DIRECTORY = "patents-pagerank"
#LOCAL_OUTPUT_PATH = "/local/filesystem/path"

if __name__ == "__main__":
    pagerank = Pagerank(EDGES_INPUT,
                        damping_factor=DAMPING_FACTOR,
                        convergence_threshold=CONVERGENCE_THRESHOLD,
                        max_num_iterations=MAX_NUM_ITERATIONS,
                        temporary_output_prefix=TEMPORARY_OUTPUT_PREFIX,
                        output_bucket=OUTPUT_BUCKET,
                        output_directory=OUTPUT_DIRECTORY)
                       #local_output_path=LOCAL_OUTPUT_PATH)
    pagerank.run_pagerank()
