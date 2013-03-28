# TODO

- Add Patent Citation Graph example (will be referenced in blog post)
- Make template files `my-pagerank.py` and `generate_my_graph.pig`
- Rename this repo "mortar-pagerank"
- reset paths from jpacker-dev to production ready mortar-examples
- QA all the things

# Welcome to Mortar!

Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  You create your project using the Mortar Development Framework, deploy code using the Git revision control system, and Mortar does the rest.

# Getting Started

This Mortar project is a generic framework for calculating PageRanks for the nodes of any directed graph. There are two example scripts which demonstrate its use. `twitter-pagerank` runs PageRank on a graph of Twitter follower relationships to find the most influential Twitter users. `patents-pagerank` runs PageRank on a graph of Patent Citations to find which recently-granted patents were the most influential. Alternatively, you can configure the template script `my-pagerank` to run PageRank on your own data (see the last section of this README). Regardless of which you choose, there are some steps to get started:

1. [Signup for a Mortar account](https://app.mortardata.com/signup)
1. [Install the Mortar Development Framework](http://help.mortardata.com/#!/install_mortar_development_framework)
1. Clone this repository to your computer and register it as a project with Mortar:

        git clone git@github.com:mortardata/mortar-pagerank.git
        cd mortar-pagerank
        mortar register mortar-pagerank

Once everything is set up you can run the Twitter example by doing:

        mortar run twitter-pagerank --clustersize 5

By default this script will run on the 100K twitter users with the most followers and finish in a little over an hour using a 5 node cluster.

You can run the Patents example by doing:

        mortar run patents-pagerank --clustersize 5

By default SOMETHING SOMETHING SOMETHING.

# Twitter Data

The twitter data we're using cames from [What is Twitter, a Social Network or a News Media?](http://an.kaist.ac.kr/traces/WWW2010.html) and was generated in early 2010.

# Patent Data

INFO ON PATENT DATA. LINK TO SCRIPTS TO GENERATE PATENT DATA FROM PATENT XML.

# The Pagerank Algorithm

Pagerank simulates a random walk over a graph where each follower-followed relationship is an edge. The pagerank of a user is the probability that after a large number of steps (starting at a random node) the walk will end up at the user's node. There is also a chance at each step that the walk will "teleport" to a completely random node: this added factor allows the algorithm to function even if there are "attractors" (nodes with no outgoing edges) which would otherwise trap the walk.

Pagerank is an iterative algorithm.  Each pass through the algorithm relies on the previous pass' output pageranks (or in the case of the first pass a set of default pageranks generated for each node). The algorithm is considered done when a new pass through the data produces results that are "close enough" to the previous pass. See http://en.wikipedia.org/wiki/PageRank for a more detailed algorithm explanation.

# What's inside

## Control Scripts

The files `./controlscripts/twitter-pagerank.py` and `./controlscripts/patents-pagerank.py` are the top level scripts that we run in Mortar to find PageRanks, for the Twitter graph and the Patent Citations graph respectively.  Using [Embedded Pig](http://help.mortardata.com/reference/pig/embedded_pig), this Jython code is responsible for running our various pig scripts in the correct order and with the correct parameters.

The file `./controlscripts/my-pagerank.py` is the template from which the pre-made controlscripts were developed from, and is a good starting point for configuring PageRank to run on your own data.

For easier debugging of control scripts all print statements are included in the pig logs shown on the job details page in the Mortar web application.

## Pig Scripts

This project contains seven pig scripts: three for generating graphs to use with PageRank, two which implement the PageRank algorithm, and two which postprocess PageRank output in different ways.

### generate\_twitter\_graph.pig

This pig script takes the full Twitter follower graph from 2010 and returns the subset of the graph that includes only the top 100 000 users.

### generate\_patent\_citation\_graph.pig

This pig script takes the US Patent Grant dataset described above and extracts from it the citation graph between patents (an edge between patents A and B means that A cites B). We filter out citations for which the cited patent is outside of the timeframe we have data for (2007-2012).

### generate\_my\_graph.pig

This template script shows steps you might take to generate a graph from your own data to input into the PageRank scripts.

### pagerank\_preprocess.pig

This pig script takes an input graph and converts it into the format that we'll use for running the iterative pagerank algorithm. This script is also responsible for setting the starting pagerank values for each node.

### pagerank\_iterate.pig

This pig script calculates updated pagerank values for each node in the graph.  It takes as input the previous pagerank values calculated for each node.  This script also calculates a 'max\_diff' value that is the largest change in pagerank for any node in the graph.  This value is used by the control script to determine if its worth running another iteration to calculate even more accurate pagerank values.

### pagerank\_postprocess\_with\_name\_join.pig

This pig script takes the final iteration output of nodes and their pageranks and joins it to an index mapping each node ID to a human-readable name. It then outputs the TOP_N nodes by pagerank to S3.

### pagerank\_postprocess\_without\_name\_join.pig

This pig script takes the final iteration output of nodes and their pageranks and outputs the TOP_N nodes to S3. It _does not_ join to any name index. This is for nodes which have human-readable identifiers already.

# Pagerank Parameters

## Damping Factor

The damping factor determines the variance of the final output pageranks.  This is a number between 0 and 1 where (1 - DAMPING\_FACTOR) is the probability of the random walk teleporting to a random node in the graph. At 0 every node would have the same pagerank (since edges would never be followed).  Setting it to 1 would mean the walks get trapped by attractor nodes and would rarely visit nodes with no incoming edges.  A common value for the damping factor is 0.85.

## Convergence Threshold

Pagerank is an iterative algorithm where each run uses the previous run's results.  It stops when the maximum difference of a user's pagerank from one iteration to the next is less than the CONVERGENCE\_THRESHOLD.

## Maximum Number of Iterations

Some graphs take many iterations to mathematically converge, but the approximation of Pagerank after a reasonable number of iterations is often good enough. The iteration therefore also stops after MAX\_NUM\_ITERATIONS even if it is still over CONVERGENCE\_THRESHOLD.

# Using your own data

## Generate a graph

To run PageRank on your own data, you must first generate a directed graph from your data. The pigscript `my\_preprocessing\_script.pig` has some code snippets to help you get started. Your output should have a schema:

    from, to, weight

`from` and `to` are the ids of the nodes the edge comes from and goes to respectively. They can be of type int, long, chararray, or bytearray. `weight` is a measure of the magnitude or significance of an edge. It can be of type int, long, float, or double. Your output can have any delimiter (you will specify your choice in the controlscript); tabs are default for PigStorage.

If your graph is undirected, you must make two records for every connection, one from A to B and another from B to A. If your graph is unweighted, simply make every weight 1.0.

## Configuring the controlscript

Next, edit the controlscript `my-pagerank.py` to use your data. Each parameter that needs to be set is listed and explained in the template.

## Run your controlscript

Finally, to run PageRank on your data, do:

    mortar run my-pagerank --cluster-size N

where N is an appropriate number for the size of your data.