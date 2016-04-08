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
import tl.lin.data.array.LongArrayWritable;
/*
* Type Parameters:
* I - Vertex id
* V - Vertex data
* E - Edge data
* M - Message type
*/

public class NaiveGraphIsomorphism extends BasicComputation<LongWritable, LongArrayWritable, FloatWritable, LongWritable>{


	//first superstep store the innode
	@Override
	public void compute(Vertex<LongWritable, LongArrayWritable, FloatWritable> vertex, Iterable<LongWritable> messages) throws IOException{
		//in superstep 0, 1 do not volt to halt
		//superstep 0: sends message of in-Vertex information
		//superstep 1: stores the in-Vertex information in vertex value
		//superstep 2: filters the unqualified vertexes
		if(getSuperstep()==0){
			Iterable<Edge<LongWritable, FloatWritable>> edges = vertex.getEdges();
			for (Edge<LongWritable, FloatWritable> edge : edges) {
				sendMessage(edge.getTargetVertexId(), vertex.getId());
			}
		}
		else if(getSuperstep()==1){
			int size = vertex.getValue().getArray().length;
			int num_message = 0;
			for (LongWritable item:messages) {
				num_message++;
			}
			long[] newArr = new long[size+num_message];
			System.arraycopy(vertex.getValue().getArray(),0,newArr,0,num_message);
			int i = 0;
			for (LongWritable item:messages){
				newArr[i+size]=item.get();
			}
			vertex.getValue().setArray(newArr);
		}
		else if(getSuperstep()==2){
			int query_in = ((GraphIsomorphismWorkerContext)getWorkerContext()).getInVertex(new Long(0)).size();
			int query_out = ((GraphIsomorphismWorkerContext)getWorkerContext()).getOutVertex(new Long(0)).size();
			int ver_in = vertex.getValue().size();
			int ver_out = 0;
			for (Edge<LongWritable,FloatWritable> item:vertex.getEdges()) {
				ver_out++;
			}
			if(ver_in>=query_in&&ver_out>=query_out){
				sendMessage(vertex.getId(),new LongWritable());
			}
		}
		else{

			vertex.voteToHalt();
		}
	}
	
	
}