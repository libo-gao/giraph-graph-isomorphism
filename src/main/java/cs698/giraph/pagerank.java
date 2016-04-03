package cs698.giraph;


import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.examples.*;

/**
 * The PageRank algorithm, with uniform transition probabilities on the edges
 * http://en.wikipedia.org/wiki/PageRank
 */
public class pagerank extends RandomWalkComputation<NullWritable> {

  @Override
  protected double transitionProbability(
      Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
      double stateProbability, Edge<LongWritable, NullWritable> edge) {
    // Uniform transition probability
    return stateProbability / vertex.getNumEdges();
  }

  @Override
  protected double recompute(
      Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
      Iterable<DoubleWritable> partialRanks, double teleportationProbability) {
    // Rank contribution from incident neighbors
    double rankFromNeighbors = MathUtils.sum(partialRanks);
    // Rank contribution from dangling vertices
    double danglingContribution =
        getDanglingProbability() / getTotalNumVertices();

    // Recompute rank
    return (1d - teleportationProbability) *
        (rankFromNeighbors + danglingContribution) +
        teleportationProbability / getTotalNumVertices();
  }
}

