package io.lzy.popular_path.model;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import lombok.Data;

/**
 * @author zhiyan
 */
// FIXME(zhiyan): Return deep copied result to client if/when needed, internal objects exposed by result.
@Data
public class Edge {    // protected, use by Node.
    /**
     * The owner name of edge migrate to the next node from the parent node. In our case, this is user name.
     */
    private final String owner;

    /**
     * Parent node object.
     * When the edge link out to parent node, this field will be null.
     */
    private final Node inNode;

    /**
     * Next node object.
     */
    private final Node outNode;

    /**
     * The list of Pre-order edge which drives parent node access next node with this edge.
     * When the edge link out to parent node, this field will be null.
     */
    private final List<Edge> preEdges;

    /**
     * Create new edge.
     * @param owner The owner name of the edge. In our case, this is user name.
     * @param inNode Parent node.
     * @param outNode Next node.
     * @param preEdge The list of Pre-order edge which drive parent node access next node with this edge by a user.
     */
    public Edge(final String owner, final Node inNode, final Node outNode, final Edge preEdge) {
        Preconditions.checkNotNull(owner, "Edge owner name should not be null");
        Preconditions.checkNotNull(outNode, "Next node object should not be null");
        final String _owner = owner.trim();
        Preconditions.checkArgument(_owner.length() > 0, "Edge owner name should not be empty");

        this.owner = _owner;
        this.inNode = inNode;
        this.outNode = outNode;
        this.preEdges = new ArrayList<>();
        if (preEdge != null) {
            preEdges.add(preEdge);
        }
    }
}
