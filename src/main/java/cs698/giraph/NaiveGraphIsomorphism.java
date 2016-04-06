package cs698.giraph;

import java.io.IOException;

import org.apache.giraph.Algorithm;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.AllWorkersInfo;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
/*
* Type Parameters:
* I - Vertex id
* V - Vertex data
* E - Edge data
* M - Message type
*/

public class NaiveGraphIsomorphism extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>{

	  @Override
	  public void initialize(
	      GraphState graphState,
	      WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
	      CentralizedServiceWorker<I, V, E> serviceWorker,
	      WorkerGlobalCommUsage workerGlobalCommUsage) 
	  {
		  super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);
		  
	  }

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages) throws IOException{
		//first superstep does some preprocessing
		if(getSuperstep()==0){
			
		}
		else{
			
		}
		vertex.voteToHalt();
	}
	
	
}