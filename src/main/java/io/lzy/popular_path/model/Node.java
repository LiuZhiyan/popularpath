package io.lzy.popular_path.model;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * @author zhiyan
 */
public class Node {

    private final ArrayList<Edge> inEdges;

    private final ArrayList<Edge> outEdges;

    @Getter
    private final String name;

    private final Map<String, AtomicInteger> refCount;

    /**
     * Create root node.
     * @param edgeOwner The owner name of edge migrates to the node from the parent. In our case, this is user name.
     */
    public static Map.Entry<Node, Edge> createRootNode(final String edgeOwner) {
        return createNode(Graph.ROOT_NODE_NAME, null, null, edgeOwner);
    }

    /**
     * Create child node.
     * @param name Node name.
     * @param parent Parent node object.
     *               Current interface layout align to the case of parsing log
     *               and creating node from root node. In other word,
     *               user will not access middle node directly without root node navigation.
     * @param preEdge The list of Pre-order edge which drive parent node access next node with this edge by an user.
     * @param edgeOwner The owner name of edge migrates to the node from the parent. In our case, this is user name.
     */
    public static Map.Entry<Node, Edge> createNode(
            final String name, final Node parent, final Edge preEdge, final String edgeOwner) {
        Preconditions.checkNotNull(edgeOwner, "Edge owner name should not be null");
        final String _edgeOwner = edgeOwner.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");

        Node node = new Node(name, edgeOwner);
        Edge edge = node.linkParent(parent, preEdge, _edgeOwner);

        return new AbstractMap.SimpleEntry<>(node, edge);
    }

    /**
     * Create child node.
     * @param name Node name.
     * @param edgeOwner The owner name of edge migrates to the node from the parent. In our case, this is user name.
     */
    private Node(final String name, final String edgeOwner) {
        Preconditions.checkNotNull(name, "Node name should not be null");
        Preconditions.checkNotNull(edgeOwner, "Edge owner name should not be null");
        final String _edgeOwner = edgeOwner.trim();
        final String _name = name.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");
        Preconditions.checkArgument(_name.length() > 0, "Node name should not be empty");

        this.name = _name;
        this.inEdges = new ArrayList<>();
        this.outEdges = new ArrayList<>();
        this.refCount = new ConcurrentHashMap<>();
        this.refCount.put(_edgeOwner, new AtomicInteger(1));
    }

    /**
     * Check if a node is parent.
     * @param parent The node to check.
     * @param edgeOwner The owner name of parent migrates to this node. In our case, this is user name.
     * @return Boolean value indicates if the node is the parent of this node.
     */
    boolean isParent(final Node parent, final String edgeOwner) {
        return this.inEdges.parallelStream().anyMatch(edge ->
                edge.getOwner().equals(edgeOwner) &&
                        edge.getInNode() != null &&
                        edge.getInNode().equals(parent));
    }

    /**
     * Check if an edge is a pre-order edge of this node.
     * @param parent Parent node.
     * @param edgeOwner The owner name of parent migrates to this node. In our case, this is user name.
     * @param preEdge The edge to check.
     * @return Boolean value indicates if the edge is the pre-order edge of this node.
     */
    boolean hasPreEdge(final Node parent, final String edgeOwner, final Edge preEdge) {
        Preconditions.checkNotNull(preEdge, "Pre-order edge should not be null");

        return this.inEdges.parallelStream().anyMatch(edge ->
                edge.getOwner().equals(edgeOwner) &&
                        edge.getInNode() != null &&
                        edge.getInNode().equals(parent) &&
                        edge.getPreEdges().contains(preEdge));
    }

    /**
     * Link this node to a parent node.
     * @param parent Parent node.
     * @param preEdge Pre-order edge of this node.
     * @param edgeOwner The owner name of parent migrates to this node. In our case, this is user name.
     * @return New edge, as the link of the parent migrates to this node.
     */
    Edge linkParent(final Node parent, final Edge preEdge, final String edgeOwner) {
        if (parent != null) {
            Preconditions.checkNotNull(preEdge, "Pre-order edge should not be null");
        }

        Edge edge = new Edge(edgeOwner, parent, this, preEdge);
        this.inEdges.add(edge);
        if (parent != null) {
            parent.outEdges.add(edge);
        }
        return edge;
    }

    /**
     * Link a pre-order edge to this node.
     * @param parent Parent node.
     * @param preEdge Pre-order edge.
     * @param edgeOwner The owner name of parent migrates to this node. In our case, this is user name.
     * @return The edge of the edge owner which linked to the pre-order edge.
     */
    Edge linkPreEdge(final Node parent, final Edge preEdge, final String edgeOwner) {
        Edge currentEdge = this.inEdges.parallelStream().filter(edge ->
                edge.getOwner().equals(edgeOwner) &&
                        edge.getInNode().equals(parent)).findFirst().get();
        currentEdge.getPreEdges().add(preEdge);
        return currentEdge;
    }

    /**
     * Count a reference for a edge owner (user).
     * @param edgeOwner The owner name of the edge which adds a reference to this node. In our case, this is user name.
     * @return New reference count.
     */
    int addRef(final String edgeOwner) {
        Preconditions.checkNotNull(edgeOwner, "Edge owner name should not be null");
        final String _edgeOwner = edgeOwner.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");

        synchronized (this.refCount) {
            AtomicInteger ai = this.refCount.get(_edgeOwner);
            if (ai == null) {
                ai = new AtomicInteger(0);
                this.refCount.put(_edgeOwner, ai);
            }
            return ai.incrementAndGet();
        }
    }

    /**
     * Retrieve reference count for the edge owner (user) on this node.
     * @param edgeOwner The owner name of the edge which refers to this node. In our case, this is user name.
     * @return Reference count
     */
    int getRefCount(final String edgeOwner) {
        synchronized (this.refCount) {
            AtomicInteger ai = this.refCount.get(edgeOwner);
            if (ai == null) {
                return 0;
            } else {
                return ai.get();
            }
        }
    }

    /**
     * Retrieve all edges point to this node.
     * @return Edge list.
     */
    public List<Edge> getInEdges() {
        return ImmutableList.copyOf(this.inEdges);
    }

    /**
     * Retrieve all edges point to other node from this node.
     * @return Edge list.
     */
    public List<Edge> getOutEdges() {
        return ImmutableList.copyOf(this.outEdges);
    }

    /**
     * Showing node name gives developer help when debug in a modern IDE, like IDEA I used.
     * @return Node name
     */
    @Override
    public String toString(){
        return this.name;
    }
}
