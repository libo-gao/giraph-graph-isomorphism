package cs698.giraph;

import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.IOException;


import tl.lin.data.array.LongArrayWritable;
/**
 * Special version of vertex that holds the value in LongArrayWritable
 *
 * @param <I> Vertex id
 * @param <LongArrayWritable> Vertex data
 * @param <E> Edge data
 */
public class LongArrayValueVertex<I extends WritableComparable,
        LongArrayWritable extends Writable, E extends Writable>
        extends DefaultVertex<I, LongArrayWritable, E> {


    @Override
    public void initialize(I id, LongArrayWritable value, Iterable<Edge<I, E>> edges) {
        // Set the parent's value to null, and instead use our own setter
        LongArrayWritable temp = null;
        super.initialize(id, temp, edges);
    }

    @Override
    public void initialize(I id, LongArrayWritable value) {
        LongArrayWritable temp = null;
        super.initialize(id, null);
    }

}
