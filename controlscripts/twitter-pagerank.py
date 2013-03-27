from org.apache.pig.scripting import Pig

# ---------------------------------------------------------
# Pagerank Parameters - See README.md for more information.
# ---------------------------------------------------------

DAMPING_FACTOR        = 0.85
CONVERGENCE_THRESHOLD = 0.00004
MAX_NUM_ITERATIONS    = 5

# ----------------
# Input Parameters
# ----------------

# A directed graph with the schema "from, to, weight"
EDGES_INPUT                      = "s3n://jpacker-dev/twitter-pagerank/twitter_influential_user_graph.gz"
# The delimiter character for the edges data. The empty string means use the default tab delimiter.
EDGES_INPUT_DELIMITER            = ""
# Whether to join the output with an index of node ids to human-readable node names.
# If your node names are already human-readable, you can set this to false and set NODE_NAMES_INPUT to an empty string.
POSTPROCESS_JOIN_WITH_NODE_NAMES = True
# The node id to name index has the schema "node, node_name".
NODE_NAMES_INPUT                 = "s3n://jpacker-dev/twitter-pagerank/twitter_influential_usernames.gz"
# The delimiter character for the node id to name index. The empty string means use the default tab delmiter.
NODE_NAMES_INPUT_DELIMITER       = ""

# -----------------
# Output Parameters
# -----------------

OUTPUT_BUCKET    = "mortar-example-output-data"
OUTPUT_DIRECTORY = "twitter-pagerank" 
NUM_TOP_NODES    = 1000 # The number of nodes from your graph with with the highest pagerank 
                        # to be returned in the final result.

# ------------------------------------------------------------------
# Internal Pagerank Parameters -- you shouldn't need to change these
# ------------------------------------------------------------------

# Pigscript paths
PREPROCESS_SCRIPT                    = "../pigscripts/pagerank_preprocess.pig"
PAGERANK_ITERATE_SCRIPT              = "../pigscripts/pagerank_iterate.pig"
POSTPROCESS_SCRIPT_WITH_NAME_JOIN    = "../pigscripts/pagerank_postprocess_with_name_join.pig"
POSTPROCESS_SCRIPT_WITHOUT_NAME_JOIN = "../pigscripts/pagerank_postprocess_without_name_join.pig"

# Temporary Data Paths - Use HDFS for better performance
HDFS_OUTPUT_PREFIX         = "hdfs:///twitter_pagerank"
PREPROCESS_PAGERANKS       = HDFS_OUTPUT_PREFIX + "/preprocess/pageranks"
PREPROCESS_NUM_NODES       = HDFS_OUTPUT_PREFIX + "/preprocess/num_nodes"
ITERATION_PAGERANKS_PREFIX = HDFS_OUTPUT_PREFIX + "/iteration/pageranks_"
ITERATION_MAX_DIFF_PREFIX  = HDFS_OUTPUT_PREFIX + "/iteration/max_diff_"

# -------------------------
# Pagerank Control Function
# -------------------------

def run_pagerank():
    """
    Calculates pageranks for directed graph of nodes and edges.

    Three main steps:
        1. Preprocessing: Process input data to:
             a) Count the total number of nodes.
             b) Prepare initial pagerank values for all nodes.
        2. Iteration: Calculate new pageranks for each node based on the previous pageranks of the
                      nodes with edges going into the given node.
        3. Postprocessing: Find the top pagerank nodes and join to a separate dataset to find their names.
    """
    # Preprocessing step:
    print "Starting preprocessing step."
    preprocess = Pig.compileFromFile(PREPROCESS_SCRIPT)
    preprocess_params = {
        "INPUT_PATH": EDGES_INPUT,
        "PAGERANKS_OUTPUT_PATH": PREPROCESS_PAGERANKS,
        "NUM_NODES_OUTPUT_PATH": PREPROCESS_NUM_NODES
    }
    if len(EDGES_INPUT_DELIMITER) > 0:
        preprocess_params["INPUT_DELIMITER"] = EDGES_INPUT_DELIMITER
    # otherwise use the default tab delimiter
    preprocess_bound = preprocess.bind(preprocess_params)
    preprocess_stats = preprocess_bound.runSingle()

    # Update convergence threshold based on the size of the graph (number of nodes)
    num_nodes = int(str(preprocess_stats.result("num_nodes").iterator().next().get(0)))
    convergence_threshold = CONVERGENCE_THRESHOLD / num_nodes

    # Iteration step:
    iteration = Pig.compileFromFile(PAGERANK_ITERATE_SCRIPT)
    for i in range(MAX_NUM_ITERATIONS):
        print "Starting iteration step: %s" % str(i + 1)

        # Append the iteration number to the input/output stems
        iteration_input = PREPROCESS_PAGERANKS if i == 0 else (ITERATION_PAGERANKS_PREFIX + str(i-1))
        iteration_pageranks_output = ITERATION_PAGERANKS_PREFIX + str(i)
        iteration_max_diff_output = ITERATION_MAX_DIFF_PREFIX + str(i)

        iteration_bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "DAMPING_FACTOR": DAMPING_FACTOR,
            "NUM_NODES": num_nodes,
            "PAGERANKS_OUTPUT_PATH": iteration_pageranks_output,
            "MAX_DIFF_OUTPUT_PATH": iteration_max_diff_output
        })
        iteration_stats = iteration_bound.runSingle()

        # If we're below the convergence_threshold break out of the loop.
        max_diff = float(str(iteration_stats.result("max_diff").iterator().next().get(0)))
        if max_diff < CONVERGENCE_THRESHOLD:
            print "Max diff %s under convergence threshold. Stopping." % max_diff
            break
        elif i == MAX_NUM_ITERATIONS-1:
            print "Max diff %s above convergence threshold but hit max number of iterations.  Stopping." \
                    % max_diff
        else:
            print "Max diff %s above convergence threshold. Continuing." % max_diff

    iteration_pagerank_result = ITERATION_PAGERANKS_PREFIX + str(i)

    # Postprocesing step:
    print "Starting postprocessing step."

    if (POSTPROCESS_JOIN_WITH_NODE_NAMES):
        postprocess = Pig.compileFromFile(POSTPROCESS_SCRIPT_WITH_NAME_JOIN)
    else:
        postprocess = Pig.compileFromFile(POSTPROCESS_SCRIPT_WITHOUT_NAME_JOIN)

    postprocess_params = {
        "PAGERANKS_INPUT_PATH": iteration_pagerank_result,
        "TOP_N": NUM_TOP_NODES,
        "OUTPUT_BUCKET": OUTPUT_BUCKET,
        "OUTPUT_DIRECTORY": OUTPUT_DIRECTORY
    }

    # If we are joining with a name index, set parameters used by that script
    if (POSTPROCESS_JOIN_WITH_NODE_NAMES):
        postprocess_params["NODE_NAMES_INPUT_PATH"] = NODE_NAMES_INPUT
        if len (NODE_NAMES_INPUT_DELIMITER) > 0:
            postprocess_params["NODE_NAMES_INPUT_DELIMITER"] = NODE_NAMES_INPUT_DELIMITER

    postprocess_bound = postprocess.bind(postprocess_params)
    postprocess_stats = postprocess_bound.runSingle()

if __name__ == "__main__":
    run_pagerank()
