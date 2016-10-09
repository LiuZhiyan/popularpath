package io.lzy.popular_path;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import io.lzy.popular_path.model.*;

/**
 * @author zhiyan
 */
@Slf4j
public class LogParser<G extends Graph> {

    @Getter
    private final G graph;

    /**
     * Parent node of the owner.
     * The key is edge owner name.
     * The value is last node generated of the owner, use to parse next record of the same owner.
     */
    private final Map<String, Map.Entry<Node, Edge>> nodeParentCache;

    /**
     * Parse log and generate graph.
     * @param stream The stream as input.
     * @param graph The graph as output of parser.
     * @param <G> Real graph type {@link GraphRandom}, {@link GraphSequence}.
     * @return The amount of handled access record.
     * @throws IOException The exception about reading access log from input failed.
     */
    public static <G extends Graph> int parseLog(final InputStream stream, final G graph) throws IOException {
        return  new LogParser<>(graph).parseLog(stream);
    }

    /**
     * Create new access log parser.
     * @param graph The graph as output of parser.
     */
    private LogParser(final G graph) {
        Preconditions.checkNotNull(graph, "Graph should not be null");

        this.graph = graph;
        this.nodeParentCache = new LinkedHashMap<>();
    }

    /**
     * Parse access log input stream.
     * @param stream The stream as input.
     * @return The amount of handled access record.
     * @throws IOException The exception about reading access log from input failed.
     */
    private int parseLog(final InputStream stream) throws IOException {
        Preconditions.checkNotNull(stream, "Input stream should not be null");

        int parsedLines = 0;

        try (Reader reader = new InputStreamReader(stream)) {
            try (BufferedReader breader = new BufferedReader(reader)) {
                String line;
                while ((line = breader.readLine()) != null) {
                    parseRecord(line);
                    parsedLines++;
                }
            }
        }

        return parsedLines;
    }

    /**
     * Parse a single node access record in log line.
     * The format of access record in the line is: USER_NAME[\t\x0B\f\r]NODE_NAME(\n|\r\n)
     * @param logLine A single line in the log
     * @return The node added to the graph. Value null will be returned if access record in the line invalid.
     */
    private Node parseRecord(final String logLine) {
        Preconditions.checkNotNull(logLine, "Access record log line should not be null");
        String[] ret = logLine.split("\\s+");
        Node node = null;
        Edge edge = null;
        String owner, name;

        if ((ret.length != 2) ||
                ((owner = ret[0].trim()).length() == 0) ||
                ((name = ret[1].trim()).length() == 0)) {
            log.warn(String.format(
                    "Invalid access record in log: %s\nValid format: USER_NAME[\\t\\x0B\\f\\r]NODE_NAME(\\n|\\r\\n)",
                    logLine));
        } else {
            Map.Entry<Node, Edge> item = this.nodeParentCache.get(owner);
            if (item != null) {
                node = item.getKey();
                edge = item.getValue();
            }
            item = graph.touchNode(name, node, edge, owner);
            if (item.getKey() != null) {    // Skip duplicated access record, e.g. user refresh node accessing.
                this.nodeParentCache.put(ret[0], item);
            }
        }

        return node;
    }
}
