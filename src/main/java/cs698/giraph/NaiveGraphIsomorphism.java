package cs698.giraph;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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
import tl.lin.data.pair.PairOfLongs;
/*
* Type Parameters:
* I - Vertex id
* V - Vertex data
* E - Edge data
* M - Message type
*/

public class NaiveGraphIsomorphism extends BasicComputation<LongWritable, LongArrayWritable, FloatWritable, LongArrayWritable>{


	//first superstep store the innode
	@Override
	public void compute(Vertex<LongWritable, LongArrayWritable, FloatWritable> vertex, Iterable<LongArrayWritable> messages) throws IOException{
		queryGraph graph = ((GraphIsomorphismWorkerContext)getWorkerContext()).getQueryGraph();
		List<PairOfLongs> graph_array = ((GraphIsomorphismWorkerContext)getWorkerContext()).getGraphArray();

		//in superstep 0 do not volt to halt
		//superstep 0: sends message of in-Vertex information
		//superstep 1: stores the in-Vertex information in vertex value, filters the unqualified vertexes
		if(getSuperstep()==0){
			Iterable<Edge<LongWritable, FloatWritable>> edges = vertex.getEdges();
			long[] temp;
			for (Edge<LongWritable, FloatWritable> edge : edges) {
				temp = new long[1];
				temp[0]=vertex.getId().get();
				sendMessage(edge.getTargetVertexId(), new LongArrayWritable(temp));
			}
		}
		else if(getSuperstep()==1){
			int size = vertex.getValue().getArray().length;
			int num_message = 0;
			for (LongArrayWritable item:messages) {
				num_message++;
			}
			long[] newArr = new long[size+num_message];
			System.arraycopy(vertex.getValue().getArray(),0,newArr,0,num_message);
			int i = 0;
			for (LongArrayWritable item:messages){
				newArr[i+size]=item.get(0);
			}
			vertex.getValue().setArray(newArr);

			int query_in = ((GraphIsomorphismWorkerContext)getWorkerContext()).getInVertex(new Long(0)).size();
			int query_out = ((GraphIsomorphismWorkerContext)getWorkerContext()).getOutVertex(new Long(0)).size();
			int ver_in = vertex.getValue().size();
			int ver_out = 0;
			for (Edge<LongWritable,FloatWritable> item:vertex.getEdges()) {
				ver_out++;
			}
			if(ver_in>=query_in&&ver_out>=query_out){
				long[] temp = new long[1];
				temp[0] = vertex.getId().get();
				sendMessage(vertex.getId(),new LongArrayWritable(temp));
			}
			vertex.voteToHalt();
		}
		else{
			int curr = ((GraphIsomorphismWorkerContext)getWorkerContext()).getCurr_node();
			int query_in = ((GraphIsomorphismWorkerContext)getWorkerContext()).getInVertex(new Long(curr)).size();
			int query_out = ((GraphIsomorphismWorkerContext)getWorkerContext()).getOutVertex(new Long(curr)).size();
			int ver_in = vertex.getValue().size();
			int ver_out = 0;
			for (Edge<LongWritable,FloatWritable> item:vertex.getEdges()) {
				ver_out++;
			}

			for (LongArrayWritable message: messages) {
				//if this vertex has been visted
				//1. backtracking
				//2. repeat -> ignore
				if(contains(message, vertex.getId().get())){
					int msg_len = message.getArray().length;
					long jump = ((GraphIsomorphismWorkerContext)getWorkerContext()).getGraphArray().get(msg_len-1).getRightElement();
					if(jump==new Long(0)){
						//repeat vertex. ignore
					}else{
						for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
							if(contains(message, edge.getTargetVertexId().get())) {
								sendMessage(edge.getTargetVertexId(), message);
							}
						}
					}
					continue;
				}

				//vertex has not been visited
				if(ver_in>=query_in&&ver_out>=query_out){
					if(!Connected(graph, graph_array, vertex, curr, message)) continue;
					if (graph_array.get(curr).getRightElement() != new Long(0)) {
						sendMessage(vertex.getId(), addOne(message,vertex.getId().get()));
					} else {
						for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
							sendMessage(edge.getTargetVertexId(), addOne(message,vertex.getId().get()));
						}
					}
				}
			}
			vertex.voteToHalt();
		}
	}

	boolean Connected(queryGraph graph, List<PairOfLongs> graph_array, Vertex<LongWritable, LongArrayWritable, FloatWritable> vertex, int curr, LongArrayWritable msg){
		//inEdge
		Set<Long> in = graph.getVertex(graph_array.get(curr).getLeftElement()).inNode;
		for (Long id:in) {
			int index = containsBefore(graph_array, curr,id);
			if(index==-1) continue;
			long potential_in = msg.get(index);
			if(!contains(vertex.getValue(),potential_in)) return false;
		}

		//outEdge
		Set<Long> out = graph.getVertex(graph_array.get(curr).getLeftElement()).outNode;
		for (Long id:out){
			int index = containsBefore(graph_array, curr, id);
			if(index==-1) continue;
			long potential_out = msg.get(index);
			int has = 0;
			for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
				if(edge.getTargetVertexId().get()==potential_out){
					has = 1;
					break;
				}
			}
			if(has==0) return false;
		}
		return true;
	}

	LongArrayWritable addOne(LongArrayWritable arr, long item){
		int len = arr.getArray().length;
		long[] temp = new long[len+1];
		System.arraycopy(arr.getArray(), 0, temp, 0, len);
		temp[len] = item;
		return new LongArrayWritable(temp);
	}

	int containsBefore(List<PairOfLongs> graph_array, int end, long id){
		for(int i = 0;i<end;i++){
			if(graph_array.get(i).getLeftElement()==id) return i;
		}
		return -1;
	}

	boolean contains(LongArrayWritable arr, long id){
		for (long item: arr.getArray()) {
			if(id==item) return true;
		}
		return false;
	}
}