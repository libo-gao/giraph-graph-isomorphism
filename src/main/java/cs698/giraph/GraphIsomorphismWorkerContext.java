package cs698.giraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import tl.lin.data.pair.PairOfLongs;


/**
 * Worker context for graph isomorphism.
 */
public class GraphIsomorphismWorkerContext extends WorkerContext {

    private queryGraph query = new queryGraph();
    private ArrayList<PairOfLongs> graph_array = new ArrayList<PairOfLongs>();
    private int curr_node;
    private int addition;

    public static final StrConfOption inputFile =
            new StrConfOption("GraphIsomorphism.query", " ",
                    "query graph file path");

    /** Logger */
    private static final Logger LOG = Logger
            .getLogger(GraphIsomorphismWorkerContext.class);

    int getCurr_node(){
        return curr_node;
    }

    List<PairOfLongs> getGraphArray(){
        return graph_array;
    }

    queryGraph getQueryGraph(){
        if(query==null) System.out.println("find+++++++++++++++++++++++++++++++++++++++++++NULL+++++++++++++++++");
        return query;
    }

    //todo
    private void serializeGraph(){
        /***
        //leave this alone, just assume one graph structure
        //1->3->4->6
        //2->3->5
        graph_array.add(new PairOfLongs(new Long(1), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(3), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(4), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(6), new Long(3)));
        graph_array.add(new PairOfLongs(new Long(5), new Long(3)));
        graph_array.add(new PairOfLongs(new Long(2), new Long(0)));
        */
        /*
        //1->2->3->4
        //2->5
        graph_array.add(new PairOfLongs(new Long(1), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(2), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(3), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(4), new Long(2)));
        graph_array.add(new PairOfLongs(new Long(5), new Long(0)));
        */
        /*
        //circle
        //1->2->3->1
        graph_array.add(new PairOfLongs(new Long(1), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(2), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(3), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(4), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(5), new Long(0)));
        */
        /*
        //liner
        //1->2->3
        graph_array.add(new PairOfLongs(new Long(1), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(2), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(3), new Long(0)));
        */

        //star
        //    2
        // 5<-1->3
        //    4
        graph_array.add(new PairOfLongs(new Long(1), new Long(0)));
        graph_array.add(new PairOfLongs(new Long(2), new Long(1)));
        graph_array.add(new PairOfLongs(new Long(3), new Long(0)));
    }

    Set<Long> getOutVertex(Long id){
        return query.getVertex(id).outNode;
    }

    Set<Long> getInVertex(Long id){
        return query.getVertex(id).inNode;
    }

    /**
     * load the query graph from input file
     * @param configuration The configuration.
     * @return a (possibly empty) set of source vertices
     */
    private void loadQueryGraph(Configuration configuration){
        Path inputPath = null;
        try {
            inputPath = new Path(inputFile.get(configuration));

            FileSystem fs = FileSystem.get(getConf());
            BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(inputPath), "UTF-8"));
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split(" ");
                if(tokens[0]==""||tokens[1]=="") continue;
                query.insert(new Long(tokens[0]),new Long(tokens[1]));
            }
            in.close();
        } catch (IOException e) {
            LOG.error("Could not load local cache files: " + inputPath, e);
        }

    }
    /**
     * build the query graph by loading the graph and
     *
     * @param configuration the conf
     */
    private void buildQueryGraph(Configuration configuration) {
        loadQueryGraph(configuration);
        query.build();
        serializeGraph();
    }

    @Override
    public void preApplication() throws InstantiationException,
            IllegalAccessException {
        buildQueryGraph(getContext().getConfiguration());
        curr_node=1;
        addition = 1;
    }

    @Override
    public void preSuperstep() {
    }

    @Override
    public void postSuperstep() {
        if(getSuperstep()>1){
            if(addition==1){
                if(graph_array.get(curr_node).getRightElement()==new Long(0)) {
                    curr_node++;
                }
                else{
                    curr_node++;
                    addition = 0;
                }
            }
            else{
                addition = 1;
            }
        }
    }

    @Override
    public void postApplication() {
    }
}