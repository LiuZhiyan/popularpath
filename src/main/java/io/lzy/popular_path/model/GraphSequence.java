package io.lzy.popular_path.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * @author zhiyan
 *
 * A Graph implementation which supports to generate graph base on sequence node access (node touch).
 *  It requires client provides the number of sequential nodes of the path when creating graph.
 *  So the dynamics about popular path query of {@link GraphRandom} is better than this implementation however
 *  this graph provides much better query performance especially when client query more then once.
 *
 * Other limitation on client usage as following, you might think this kind of graph more like a AOE network.
 *  1. There is no any cycle in the graph, the out going edge of the node should always to next depth level.
 */
public class GraphSequence extends Graph {

    private final int maxPathDepthEvaluation;

    private final Map<String, List<Map.Entry<String, Integer>>> popularPathMap;

    public GraphSequence() {
        this(DEFAULT_PATH_DEPTH);
    }

    /**
     * Create a graph.
     * @param depth Indicates M sequential nodes in popular path.
     */
    public GraphSequence(final int depth) {
        Preconditions.checkArgument(depth > 1, "Path depth parameter should greater than 1");

        this.maxPathDepthEvaluation = depth;
        this.popularPathMap = new ConcurrentHashMap<>();
    }

    /**
     * Evaluate a path.
     * @param lastNode The start node of the path to evaluate. In our case, this indicates end node.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @param maxPathDepth max depth of the path to evaluate.
     * @return Evaluation result. A list of the "path" => "frequency" pair.
     */
    private List<Map.Entry<String, Integer>> evaluatePath(
            final Node lastNode, final String edgeOwner, final int maxPathDepth) {
        return super.evaluatePath(lastNode, null, edgeOwner, maxPathDepth);
    }

    /**
     * Check if a node is root node.
     * @param node Node to check.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @return Boolean value indicates if the node is root node.
     */
    @Override
    protected boolean isLastNode(final Node node, final Edge preEdge, final String edgeOwner) {
        return node.getInEdges().parallelStream().filter(edge ->
                edge.getOwner().equals(edgeOwner) && edge.getInNode() != null).count() == 0;
    }

    /**
     * Generate full path according to previous full path.
     * @param node The current node in the path evaluation process.
     * @param preFullPath The previous full path in the path evaluation process.
     * @return The full path including current node.
     */
    @Override
    protected String genCurrentPath(final Node node, final String preFullPath) {
        return String.format("%s%s%s", node.getName(),
                node.getName().equals(ROOT_NODE_NAME) || preFullPath.isEmpty() ? "" : NODE_PATH_SEPARATOR, preFullPath);
    }

    /**
     * Get next batch of edges to evaluate.
     * @param node The source node.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @return Boolean value indicates if the edge is the next one to evaluate.
     */
    @Override
    protected Stream<Edge> getNextEdges(final Node node, final Edge preEdge, final String edgeOwner) {
        return node.getInEdges().parallelStream().filter(edge -> edge.getOwner().equals(edgeOwner));
    }

    /**
     * Get next node to evaluate.
     * @param edge The edge to follow.
     * @return The next node.
     */
    @Override
    protected Node getNextNode(final Edge edge) {
        return edge.getInNode();
    }

    /**
     * Add new node or increase existing node reference.
     * @param nodeName Node name.
     * @param parent Parent node.
     *               The interface in sequence graph aligns to the case of parsing log
     *               and creating node from root node. In other word,
     *               user will not access middle node directly without root node navigation.
     * @param preEdge Pre-order edge, as a edge of parent node which drives parent node access next node.
     * @param edgeOwner The owner name of edge migrate to the node from the parent. In our case, this is user name.
     * @return Added node and edge. A null value will be returned if input node is duplicated with parent one.
     */
    @Override
    public Map.Entry<Node, Edge> touchNode(
            final String nodeName, final Node parent, final Edge preEdge, final String edgeOwner) {
        final String _edgeOwner = edgeOwner.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");

        final Map.Entry<Node, Edge> ret = super.touchNode(nodeName, parent, preEdge, edgeOwner);

        // in-time booking
        if (ret.getKey() != null) {
            synchronized (this.popularPathMap) {
                final List<Map.Entry<String, Integer>> items =
                        evaluatePath(ret.getKey(), _edgeOwner, this.maxPathDepthEvaluation);
                List<Map.Entry<String, Integer>> _itemList = this.popularPathMap.get(_edgeOwner);
                if (_itemList == null) {
                    _itemList = new LinkedList<>();
                    this.popularPathMap.put(_edgeOwner, _itemList);
                }

                final List<Map.Entry<String, Integer>> itemList = _itemList;    // final variable in lambda is required
                items.forEach(item -> {
                    int pos = 0;
                    for (pos = 0; pos < itemList.size(); pos++) {
                        if (item.getValue() > itemList.get(pos).getValue()) {
                            break;
                        }
                    }
                    itemList.add(pos, item);
                });
            }
        }

        return ret;
    }

    /**
     * Find the top N most popular 3-node paths, where a path is three sequential path visits by an user.
     * @param topN Indicates top N.
     * @return A result map. Key set contains all users in the graph,
     *      value lists top N most popular 3-node paths for the user key gives.
     */
    public Map<String, List<Map.Entry<String, Integer>>> getPopularPath(final int topN) {
        final Map<String, List<Map.Entry<String, Integer>>> ret = new ConcurrentHashMap<>();

        synchronized (this.popularPathMap) {
            final Map<String, List<Node>> nodeMap = getNodeMapByOwner();

            synchronized (nodeMap) {
                nodeMap.keySet().parallelStream().forEach(edgeOwner -> {
                    List<Map.Entry<String, Integer>> items = this.popularPathMap.get(edgeOwner);
                    if (items.size() > 0) {
                        items.subList(0, Math.min(topN, items.size())).forEach(item -> {
                            synchronized (ret) {
                                List<Map.Entry<String, Integer>> itemList = ret.get(edgeOwner);
                                if (itemList == null) {
                                    itemList = new LinkedList<>();
                                    ret.put(edgeOwner, itemList);
                                }

                                int pos = 0;
                                for (pos = 0; pos < itemList.size(); pos++) {
                                    if (item.getValue() > itemList.get(pos).getValue()) {
                                        break;
                                    }
                                }
                                itemList.add(pos, item);
                                if (itemList.size() > topN) {
                                    itemList.subList(topN, itemList.size()).clear();
                                }
                            }
                        });
                    }
                });
            }
        }

        return ret;
    }

    /**
     * Find the top N most popular 3-node paths, where a path is three sequential path visits by the user.
     * @param topN Indicates top N.
     * @param edgeOwner Indicates user name who access the popular paths.
     * @return A result list contains top N most popular 3-node paths for the user.
     */
    public List<Map.Entry<String, Integer>> getPopularPath(final int topN, final String edgeOwner) {
        Preconditions.checkArgument(topN > 0, "Top N parameter should greater than 0");
        Preconditions.checkNotNull(edgeOwner);
        final String _edgeOwner = edgeOwner.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");

        synchronized (this.popularPathMap) {
            final List<Map.Entry<String, Integer>> items = this.popularPathMap.get(edgeOwner);

            if (items == null) {
                return new LinkedList<>();
            } else {
                // FIXME(zhiyan): Return deep copied result to client if/when needed, internal status exposed by result.
                return items.subList(0, Math.min(topN, items.size()));
            }
        }
    }
}
