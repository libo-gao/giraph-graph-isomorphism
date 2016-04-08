package cs698.giraph;

import java.io.IOException;

import org.apache.giraph.Algorithm;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.*;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.ArrayWritable;
/*
* Type Parameters:
* I - Vertex id
* V - Vertex data
* E - Edge data
* M - Message type
*/

public class NaiveGraphIsomorphism extends BasicComputation<LongWritable, ArrayWritable, FloatWritable, DoubleWritable>{


	//first superstep store the innode
	@Override
	public void compute(Vertex<LongWritable, ArrayWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException{
		//first superstep does some preprocessing
		if(getSuperstep()==0){
			Iterable<Edge<LongWritable, FloatWritable>> edges = vertex.getEdges();
			for (Edge<LongWritable, FloatWritable> edge : edges) {
				sendMessage(edge.getTargetVertexId(), new DoubleWritable((double)vertex.getId().get()));
			}
		}
		else if(getSuperstep()==1){
			LongWritable[] tmp = vertex.getValue().get();

		}
		else if(getSuperstep()==2){
			((GraphIsomorphismWorkerContext)getWorkerContext()).getInVertex(new Long(0));

		}
		else{
			
		}
		vertex.voteToHalt();
	}
	
	
}