package io.lzy.popular_path.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * @author zhiyan
 *
 * A Graph implementation which supports to generate graph base on random node access (node touch) and
 *  allow client ad-hoc query popular path contains any number of sequential path.
 *
 * If you want to get better query performance, you might check {@link GraphSequence}.
 */
public class GraphRandom extends Graph {

    /**
     * Evaluate a path.
     * @param startNode The start node of the path to evaluate. In our case, this indicates entry node.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @param maxPathDepth max depth of the path to evaluate.
     * @return Evaluation result. A list of the "path" => "frequency" pair.
     */
    @Override
    protected List<Map.Entry<String, Integer>> evaluatePath(
            final Node startNode, final Edge preEdge, final String edgeOwner, final int maxPathDepth) {
        Preconditions.checkNotNull(preEdge, "Pre-order edge should not be null");
        return super.evaluatePath(startNode, preEdge, edgeOwner, maxPathDepth);
    }

    /**
     * Check if a node is the last one of the path to evaluate.
     * @param node Node to check.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @return Boolean value indicates if the node is the last one of the path to evaluate.
     */
    @Override
    protected boolean isLastNode(final Node node, final Edge preEdge, final String edgeOwner) {
        return node.getOutEdges().parallelStream().filter(edge ->
                edge.getOwner().equals(edgeOwner) && edge.getPreEdges().contains(preEdge)).count() == 0;
    }

    /**
     * Generate full path according to previous full path.
     * @param node The current node in the path evaluation process.
     * @param preFullPath The previous full path in the path evaluation process.
     * @return The full path including current node.
     */
    @Override
    protected String genCurrentPath(final Node node, final String preFullPath) {
        return String.format("%s%s%s", preFullPath,
                preFullPath.isEmpty() || preFullPath.equals(ROOT_NODE_NAME) ? "" : NODE_PATH_SEPARATOR, node.getName());
    }

    /**
     * Get next batch of edges to evaluate.
     * @param node The source node.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @return The stream can fetch proper edges out to evaluate.
     */
    @Override
    protected Stream<Edge> getNextEdges(final Node node, final Edge preEdge, final String edgeOwner) {
        return node.getOutEdges().parallelStream().filter(
                edge -> edge.getOwner().equals(edgeOwner) && edge.getPreEdges().contains(preEdge));
    }

    /**
     * Get next node to evaluate.
     * @param edge The edge to follow.
     * @return The next node.
     */
    @Override
    protected Node getNextNode(final Edge edge) {
        return edge.getOutNode();
    }

    /**
     * Find the top N most popular 3-node paths, where a path is three sequential path visits by a user.
     * @param topN Indicates top N.
     * @return A result map. Key set contains all users in the graph,
     *      value lists top N most popular 3-node paths for the user key gives.
     */
    public Map<String, List<Map.Entry<String, Integer>>> getAllPopularPath(final int topN) {
        return getAllPopularPath(DEFAULT_PATH_DEPTH, topN);
    }

    /**
     * Find the top N most popular M-node paths, where a path is M sequential path visits by a user.
     * @param depth Indicates M sequential nodes in popular path.
     * @param topN Indicates top N.
     * @return A result map. Key set contains all users in the graph,
     *      value lists top N most popular M-node paths for the user key gives.
     */
    public Map<String, List<Map.Entry<String, Integer>>> getAllPopularPath(final int depth, final int topN) {
        final List<String> edgeOwners;
        final Map<String, List<Map.Entry<String, Integer>>> ret = new ConcurrentHashMap<>();
        final Map<String, List<Node>> nodeMap = getNodeMapByOwner();

        synchronized (nodeMap) {
            edgeOwners = new ArrayList<>(nodeMap.keySet());
        }

        edgeOwners.parallelStream().forEach(edgeOwner -> ret.put(edgeOwner, getPopularPath(depth, topN, edgeOwner)));
        return ret;
    }

    /**
     * Find the top N most popular 3-node paths, where a path is three sequential path visits by the user.
     * @param topN Indicates top N.
     * @param edgeOwner Indicates user name who access the popular paths.
     * @return A result list contains top N most popular 3-node paths for the user.
     */
    public List<Map.Entry<String, Integer>> getPopularPath(final int topN, final String edgeOwner) {
        return getPopularPath(DEFAULT_PATH_DEPTH, topN, edgeOwner);
    }


    /**
     * Find the top N most popular M-node paths, where a path is M sequential path visits by the user.
     * @param depth Indicates M sequential path in popular path.
     * @param topN Indicates top N.
     * @param edgeOwner Indicates user name who access the popular paths.
     * @return A result list contains top N most popular M-node paths for the user.
     */
    public List<Map.Entry<String, Integer>> getPopularPath(final int depth, final int topN, final String edgeOwner) {
        Preconditions.checkArgument(depth > 1, "Path depth parameter should greater than 1");
        Preconditions.checkArgument(topN > 0, "Top N parameter should greater than 0");
        Preconditions.checkNotNull(edgeOwner, "Edge owner name should not be null");
        final String _edgeOwner = edgeOwner.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");

        final List<Map.Entry<String, Integer>> ret = new LinkedList<>();
        final Map<String, List<Node>> nodeMap = getNodeMapByOwner();

        synchronized (nodeMap) {
            final List<Node> nodes = nodeMap.get(_edgeOwner);
            if (nodes != null) {
                nodes.parallelStream().forEach(node ->
                node.getInEdges().parallelStream().forEach(edge ->
                        evaluatePath(node, edge, _edgeOwner, depth).parallelStream().forEach(item -> {
                            synchronized (ret) {
                                int pos = 0;
                                for (pos = 0; pos < ret.size(); pos++) {
                                    if (item.getValue() > ret.get(pos).getValue()) {
                                        break;
                                    }
                                }
                                ret.add(pos, item);
                                if (ret.size() > topN) {
                                    ret.subList(topN, ret.size()).clear();
                                }
                            }
                        })
                    )
                );
            }
        }

        return ret;
    }
}
