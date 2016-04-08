package cs698.giraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class queryGraph {
    HashMap<Long, queryGraphVertex> graph;

    queryGraph(){}

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
        }
        else{
            graph.put(source, new queryGraphVertex(dest));
        }
    }
}

class queryGraphVertex {
    public Set<Long> inNode;
    public Set<Long> outNode;
    public Long id;
    queryGraphVertex(Long nodeId){id = nodeId;}
}
