package org.apache.giraph.custom;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * @author ikroal
 * @date 2019-05-08
 * @time: 20:34
 * @version: 1.0.0
 */
public class Shortestpath extends BasicComputation<LongWritable, DoubleWritable,
        FloatWritable, DoubleWritable> {

    private static final int SOURCE = 0;

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                        Iterable<DoubleWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
            if (vertex.getId().get() == SOURCE) {
                vertex.setValue(new DoubleWritable(0));
                sendMinDist(vertex);
            }
        } else {
            double minDist = Double.MAX_VALUE;
            for (DoubleWritable msg : messages) {
                minDist = Double.min(minDist, msg.get());
            }
            if (minDist < vertex.getValue().get()) {
                vertex.setValue(new DoubleWritable(minDist));
                sendMinDist(vertex);
            }
        }
        vertex.voteToHalt();
    }

    private void sendMinDist(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex) {
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
            sendMessage(edge.getTargetVertexId(),
                    new DoubleWritable(edge.getValue().get() + vertex.getValue().get()));
        }
    }
}
