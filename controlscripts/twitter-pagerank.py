from org.apache.pig.scripting import Pig

# ----------------
# Input Parameters
# ----------------

# A directed graph with the schema "from, to, weight" and a tab delimiter.
EDGES_INPUT                      = "s3n://jpacker-dev/twitter-pagerank/twitter_influential_user_graph.gz"

# Whether to join the output with an index of node-ids to human-readable node names.
# The node-id to name index has the schema: "node, node_name" and a tab delimiter, ex: "123	Barack Obama".
# If your node names are already human-readable, you can set this to false and set NODE_NAMES_INPUT to an empty string.

POSTPROCESS_JOIN_WITH_NODE_NAMES = True
NODE_NAMES_INPUT                 = "s3n://jpacker-dev/twitter-pagerank/twitter_influential_usernames.gz"

# ----------------------------------------------------------
# Iteration Parameters -- see README.md for more information
# ----------------------------------------------------------

DAMPING_FACTOR        = 0.85
CONVERGENCE_THRESHOLD = 0.001
MAX_NUM_ITERATIONS    = 12

# Temporary data is stored in HDFS for better performance
HDFS_OUTPUT_PREFIX    = "hdfs:///twitter-pagerank"

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
PREPROCESS_PAGERANKS          = HDFS_OUTPUT_PREFIX + "/preprocess/pageranks"
PREPROCESS_NUM_NODES          = HDFS_OUTPUT_PREFIX + "/preprocess/num_nodes"
ITERATION_PAGERANKS_PREFIX    = HDFS_OUTPUT_PREFIX + "/iteration/pageranks_"
ITERATION_RANK_CHANGES_PREFIX = HDFS_OUTPUT_PREFIX + "/iteration/aggregate_rank_change_"

# -----------------------------------------------------------------------------------------------
# Pagerank Control Function -- you shouldn't need to change this unless you want to add new logic
# -----------------------------------------------------------------------------------------------

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
    preprocess_bound = preprocess.bind(preprocess_params)
    preprocess_stats = preprocess_bound.runSingle()

    # Update convergence threshold based on the size of the graph (number of nodes)
    num_nodes = long(str(preprocess_stats.result("num_nodes").iterator().next().get(0)))
    convergence_threshold = long(CONVERGENCE_THRESHOLD * num_nodes * num_nodes)
    print "Calculated convergence threshold for %d nodes: %d" % (num_nodes, convergence_threshold) 

    # Iteration step:
    iteration = Pig.compileFromFile(PAGERANK_ITERATE_SCRIPT)
    for i in range(MAX_NUM_ITERATIONS):
        print "Starting iteration step: %s" % str(i + 1)

        # Append the iteration number to the input/output stems
        iteration_input = PREPROCESS_PAGERANKS if i == 0 else (ITERATION_PAGERANKS_PREFIX + str(i-1))
        iteration_pageranks_output = ITERATION_PAGERANKS_PREFIX + str(i)
        iteration_rank_changes_output = ITERATION_RANK_CHANGES_PREFIX + str(i)

        iteration_bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "DAMPING_FACTOR": DAMPING_FACTOR,
            "NUM_NODES": num_nodes,
            "PAGERANKS_OUTPUT_PATH": iteration_pageranks_output,
            "AGG_RANK_CHANGE_OUTPUT_PATH": iteration_rank_changes_output
        })
        iteration_stats = iteration_bound.runSingle()

        # If we're below the convergence threshold break out of the loop.
        aggregate_rank_change = long(str(iteration_stats.result("aggregate_rank_change").iterator().next().get(0)))
        if aggregate_rank_change < convergence_threshold:
            print "Sum of ordering-rank changes %d under convergence threshold %d. Stopping." \
                   % (aggregate_rank_change, convergence_threshold)
            break
        elif i == MAX_NUM_ITERATIONS-1:
            print ("Sum of ordering-rank changes %d " % aggregate_rank_change) + \
                  ("above convergence threshold %d but hit max number of iterations. " % convergence_threshold) + \
                   "Stopping."
        else:
            print "Sum of ordering-rank changes %d above convergence threshold %d. Continuing." \
                   % (aggregate_rank_change, convergence_threshold)

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

    postprocess_bound = postprocess.bind(postprocess_params)
    postprocess_stats = postprocess_bound.runSingle()

if __name__ == "__main__":
    run_pagerank()
