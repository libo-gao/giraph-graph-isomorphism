package cs698.giraph;


import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.giraph.conf.StrConfOption;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Set;

/**
 * Interface for defining a master vertex that can perform centralized
 * computation between supersteps. This class will be instantiated on the
 * master node and will run every superstep before the workers do.
 *
 * Communication with the workers should be performed via aggregators. The
 * values of the aggregators are broadcast to the workers before
 * vertex.compute() is called and collected by the master before
 * master.compute() is called. This means aggregator values used by the workers
 * are consistent with aggregator values from the master from the same
 * superstep and aggregator used by the master are consistent with aggregator
 * values from the workers from the previous superstep.
 */


public abstract class GraphIsomorphismMasterCompute extends DefaultMasterCompute {
	private static final Logger LOG = Logger.getLogger(GraphIsomorphismMasterCompute.class);


	/**private Set<queryGraphVertex> queryGraph;*/
	private queryGraph query;

	public static final StrConfOption inputFile =
			new StrConfOption("GraphIsomorphism.query", " ",
					"query graph file path");

	void load_queryGraph(){
		Path inputPath = null;
		try {
		inputPath = new Path(inputFile.get(getConf()));

		FileSystem fs = FileSystem.getLocal(getConf());
		BufferedReader in = new BufferedReader(new InputStreamReader(
				fs.open(inputPath), Charset.defaultCharset()));
		String line;
		while ((line = in.readLine()) != null) {
			String[] tokens = line.split(" ");
			query.insert(Long.parseLong(tokens[0]),Long.parseLong(tokens[1]));
		}
		in.close();
		} catch (IOException e) {
			LOG.error("Could not load local cache files: " + inputPath, e);
		}

	}

	/**
	 * Initialize the MasterCompute class, this is the place to register
	 * aggregators.
	 */
	@Override
	public void initialize() throws InstantiationException,
		IllegalAccessException{
		load_queryGraph();
		query.build();
		registerPersistentAggregator();
	}


}
