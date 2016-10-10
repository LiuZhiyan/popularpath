package io.lzy.popular_path.model;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhiyan
 *
 * Base Graph implementation, which contains all generaic functions.
 */
@Slf4j
public abstract class Graph {

    public final static String ROOT_NODE_NAME = "/";
    public final static String NODE_PATH_SEPARATOR = ROOT_NODE_NAME;
    public final static int DEFAULT_PATH_DEPTH = 3;

    /**
     * All nodes.
     * The key is node name of the node, will be used to accelerate
     * node reference operation with O(1) time complexity when the map used as a book.
     */
    private final Map<String, Node> nodeMap;

    /**
     * All nodes group by edge owner.
     * The key is edge owner of the node, will be used to accelerate
     * popular node path search operation as a book.
     */
    private final Map<String, List<Node>> nodeMapByOwner;

    /**
     * Create a graph.
     */
    Graph() {
        this.nodeMap = new LinkedHashMap<>();
        this.nodeMapByOwner = new LinkedHashMap<>();
    }

    /**
     * Get node map which stores all nodes group by edge owner.
     * @return node map
     */
    Map<String, List<Node>> getNodeMapByOwner() {
        return this.nodeMapByOwner;
    }

    /**
     * Path evaluation result collector.
     * @see java.util.stream.Collector
     */
    final class Collector implements java.util.stream.Collector<
            List<Map.Entry<Integer, Map.Entry<String, Integer>>>,
            List<Map.Entry<Integer, Map.Entry<String, Integer>>>,
            List<Map.Entry<Integer, Map.Entry<String, Integer>>>> {

        final int currentDepth;

        final int maxPathDepth;

        /**
         * Create new collector.
         * @param currentDepth current path depth of the evaluation result in path evaluation recursion context.
         * @param maxPathDepth max depth of the path evaluation.
         */
        Collector(final int currentDepth, final int maxPathDepth) {
            Preconditions.checkArgument(maxPathDepth > 1, "Max path depth parameter should greater than 1");
            Preconditions.checkArgument(currentDepth > 0 && currentDepth <= maxPathDepth);

            this.currentDepth = currentDepth;
            this.maxPathDepth = maxPathDepth;
        }

        /**
         * Creates and returns a new mutable result container.
         * @return A function which returns a new, mutable evaluation result container,
         * which is a list of the "path" => "frequency" pair.
         */
        @Override
        public Supplier<List<Map.Entry<Integer, Map.Entry<String, Integer>>>> supplier() {
            return LinkedList::new; // faster insert than array based list implementation.
        }

        /**
         * Folds a value into a mutable evaluation result container.
         * @return A function which folds a value into a mutable evaluation result container.
         */
        @Override
        public BiConsumer<List<Map.Entry<Integer, Map.Entry<String, Integer>>>,
                List<Map.Entry<Integer, Map.Entry<String, Integer>>>> accumulator() {
            return (list, items) -> items.parallelStream().forEach(item -> {
                // skip short path, to speed up the sort process on path set at final stage.
                if (item.getKey() - 1 != this.currentDepth || item.getKey() == this.maxPathDepth) {
                    int pos = 0;
                    for (pos = 0; pos < list.size(); pos++) {
                        if (item.getValue().getValue() > list.get(pos).getValue().getValue()) {
                            break;
                        }
                    }
                    list.add(pos, item);
                }
            });
        }

        /**
         * A function that accepts two partial results and merges them.
         * @return A function which combines two partial results into a combined result.
         */
        @Override
        public BinaryOperator<List<Map.Entry<Integer, Map.Entry<String, Integer>>>> combiner() {
            return (left, right) -> {
                left.addAll(right);
                return left;
            };
        }

        /**
         * Perform the final transformation from the intermediate accumulation evaluation result
         * to the final evaluation result.
         * @return A finisher function.
         */
        @Override
        public Function<List<Map.Entry<Integer, Map.Entry<String, Integer>>>,
                List<Map.Entry<Integer, Map.Entry<String, Integer>>>> finisher() {
            return i -> (List<Map.Entry<Integer, Map.Entry<String, Integer>>>) i;
        }

        /**
         * Returns a characteristics list indicating the characteristics of this Collector.
         * @return an immutable set of collector characteristics.
         */
        @Override
        public Set<Collector.Characteristics> characteristics() {
            // Indicates that the finisher function is the identity function and can be elided.
            return Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));
        }
    }

    /**
     * Add new node or increase existing node reference.
     * @param nodeName Node name.
     * @param parent Parent node.
     * @param preEdge Pre-order edge, as an edge of parent node which drives parent node access next node.
     * @param edgeOwner The owner name of edge migrate to the node from the parent. In our case, this is user name.
     * @return Added node and edge. A null value will be returned if input node is duplicated with parent one.
     */
    public Map.Entry<Node, Edge> touchNode(
            final String nodeName, final Node parent, final Edge preEdge, final String edgeOwner) {
        Preconditions.checkNotNull(edgeOwner, "Edge owner name should not be null");
        Preconditions.checkNotNull(nodeName, "Node name should not be null");
        String _edgeOwner = edgeOwner.trim();
        String _nodeName = nodeName.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");
        Preconditions.checkArgument(_nodeName.length() > 0, "Node name should not be empty");

        synchronized (this.nodeMap) {
            Map.Entry<Node, Edge> item;
            Node node = this.nodeMap.get(_nodeName);
            Edge edge = null;

            if (node == null) {                             // new node
                if (_nodeName.equals(ROOT_NODE_NAME)) {
                    item = Node.createRootNode(_edgeOwner);
                    node = item.getKey();
                    edge = item.getValue();
                } else {
                    Preconditions.checkNotNull(parent, "internal error");
                    Preconditions.checkNotNull(preEdge, "internal error");
                    item = Node.createNode(_nodeName, parent, preEdge, _edgeOwner);
                    node = item.getKey();
                    edge = item.getValue();
                }

                this.nodeMap.put(_nodeName, node);
                List<Node> nodes = this.nodeMapByOwner.get(_edgeOwner);
                if (nodes == null) {
                    nodes = new ArrayList<>();
                    this.nodeMapByOwner.put(_edgeOwner, nodes);
                }
                nodes.add(node);

                log.debug(String.format("New node added. Edge owner: %s, parent node name: %s, child node name: %s",
                        _edgeOwner, parent == null ? "<null>" : parent.getName(), node.getName()));
            } else if (!node.equals(parent)) {              // add node reference, skip node accessing refresh.
                int refCount = node.addRef(_edgeOwner);

                if (!node.isParent(parent, _edgeOwner)) {    // new node in-edge
                    edge = node.linkParent(parent, preEdge, _edgeOwner);
                } else if (!node.hasPreEdge(parent, _edgeOwner, preEdge)) {
                    edge = node.linkPreEdge(parent, preEdge, _edgeOwner);
                }

                List<Node> nodes = this.nodeMapByOwner.get(_edgeOwner);
                if (nodes == null) {
                    nodes = new ArrayList<>();
                    this.nodeMapByOwner.put(_edgeOwner, nodes);
                }
                if (refCount == 1) {
                    nodes.add(node);
                }

                log.debug(String.format(
                        "Existing node touched. Edge owner: %s, parent node name: %s, child node name: %s",
                        _edgeOwner, parent == null ? "<null>" : parent.getName(), node.getName()));
            } else {
                // needn't to add new node
                node = null;
            }

            return new AbstractMap.SimpleEntry<>(node, edge);
        }
    }

    /**
     * Evaluate a path.
     * @param startNode The start node of the path to evaluate.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @param maxPathDepth max depth of the path to evaluate.
     * @return Evaluation result. A list of the "path" => "frequency" pair.
     */
    protected List<Map.Entry<String, Integer>> evaluatePath(
            final Node startNode, final Edge preEdge, final String edgeOwner, final int maxPathDepth) {
        Preconditions.checkNotNull(startNode, "Start node should not be null");
        Preconditions.checkNotNull(edgeOwner, "Edge owner name should not be null");
        final String _edgeOwner = edgeOwner.trim();
        Preconditions.checkArgument(_edgeOwner.length() > 0, "Edge owner name should not be empty");
        Preconditions.checkArgument(maxPathDepth > 0, "Path depth parameter should greater than 0");

        synchronized (startNode) {   // will parallel process previous nodes
            return evaluatePath(startNode, preEdge, _edgeOwner, maxPathDepth, 1, "", 0).stream()
                    .filter(item -> item.getKey() == maxPathDepth)  // for single node case
                    .map(Map.Entry::getValue).collect(Collectors.toList());
        }
    }

    /**
     * Evaluate a path recursively.
     * @param node The current node of the path evaluation recursion.
     * @param preEdge The pre-order edge which drives parent node access current node of the path evaluation recursion.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @param maxPathDepth Max depth of the path to evaluate.
     * @param currentDepth The current depth of the path evaluation recursion.
     * @param fullPath The accumulative path of the path evaluation.
     * @param nodeRefCount The accumulative frequency of all nodes in the path of the path evaluation.
     * @return Evaluation result of the path evaluation recursion. A list of the "path" => "frequency" pair.
     */
    protected List<Map.Entry<Integer, Map.Entry<String, Integer>>> evaluatePath(
            final Node node, final Edge preEdge, final String edgeOwner, final int maxPathDepth,
            final int currentDepth, final String fullPath, final int nodeRefCount) {
        final String currentPath = genCurrentPath(node, fullPath);
        final int refCount = nodeRefCount + node.getRefCount(edgeOwner);
        if (currentDepth == maxPathDepth || isLastNode(node, preEdge, edgeOwner)) { // end of recursion
            return Arrays.asList(new AbstractMap.SimpleEntry<>(
                    currentDepth, new AbstractMap.SimpleEntry<>(currentPath, refCount)));
        } else {    // handle follow nodes
            return getNextEdges(node, preEdge, edgeOwner)
                    .map(edge -> evaluatePath(
                            getNextNode(edge), edge, edgeOwner, maxPathDepth, currentDepth + 1, currentPath, refCount))
                    .collect(new Collector(currentDepth, maxPathDepth));
        }
    }

    /**
     * Check if a node is the last one of the path to evaluate.
     * @param node Node to check.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @return Boolean value indicates if the node is the last one of the path to evaluate.
     */
    protected abstract boolean isLastNode(final Node node, final Edge preEdge, final String edgeOwner);

    /**
     * Generate full path according to previous full path.
     * @param node The current node in the path evaluation process.
     * @param preFullPath The previous full path in the path evaluation process.
     * @return The full path including current node.
     */
    protected abstract String genCurrentPath(final Node node, final String preFullPath);

    /**
     * Get next batch of edges to evaluate.
     * @param node The source node.
     * @param preEdge The pre-order edge which drives start node access the path.
     * @param edgeOwner The edge owner name of all the nodes in the path. In our case, this is user name.
     * @return The stream can fetch proper edges out to evaluate.
     */
    protected abstract Stream<Edge> getNextEdges(final Node node, final Edge preEdge, final String edgeOwner);

    /**
     * Get next node to evaluate.
     * @param edge The edge to follow.
     * @return The next node.
     */
    protected abstract Node getNextNode(final Edge edge);
}
