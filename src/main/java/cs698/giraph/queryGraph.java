package cs698.giraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class queryGraph {
    HashMap<Long, queryGraphVertex> graph;

    queryGraph(){
        graph = new HashMap<Long, queryGraphVertex>();
    }

    queryGraphVertex getVertex(Long id){
        return graph.get(id);
    }

    void build(){
        for (Map.Entry<Long, queryGraphVertex> pair : graph.entrySet()) {
            for (Long dest:pair.getValue().outNode) {
                graph.get(dest).inNode.add(pair.getKey());
            }
        }
    }

    void insert(Long source, Long dest){
        if(graph.containsKey(source)){
            graph.get(source).outNode.add(dest);
            if(!graph.containsKey(dest)){
                graph.put(dest,new queryGraphVertex(dest));
            }
        }
        else{
            queryGraphVertex temp = new queryGraphVertex(source);
            temp.outNode.add(dest);
            graph.put(source, temp);
            if(!graph.containsKey(dest)){
                graph.put(dest,new queryGraphVertex(dest));
            }
        }
    }
}

class queryGraphVertex {
    public Set<Long> inNode;
    public Set<Long> outNode;
    public Long id;
    queryGraphVertex(Long nodeId){
        id = nodeId;
        inNode = new HashSet<Long>();
        outNode = new HashSet<Long>();
    }
}
