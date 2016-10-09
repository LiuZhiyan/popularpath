package io.lzy.popular_path;

import java.util.Comparator;
import java.util.Map;

import io.lzy.popular_path.model.Graph;

/**
 * @author zhiyan
 */
public abstract class TestBase {

    public final static String TEST_USER_1 = "U1";
    public final static String TEST_USER_2 = "U2";
    public final static String TEST_USER_3 = "U3";

    public final static String TEST_NODE_ROOT = Graph.ROOT_NODE_NAME;
    public final static String TEST_NODE_CHILD_1 = "N1";
    public final static String TEST_NODE_CHILD_2 = "N2";
    public final static String TEST_NODE_CHILD_3 = "N3";
    public final static String TEST_NODE_CHILD_4 = "N4";
    public final static String TEST_NODE_CHILD_5 = "N5";

    /**
     * Helper to help code reader understand the number meaning in graph interface call parameter list.
     */
    protected int DEPTH(final int depth) {
        return depth;
    }

    /**
     * Helper to help code reader understand the number meaning in graph interface call parameter list.
     */
    protected int TOP(final int topN) {
        return topN;
    }

    /**
     * Parallel stream is used in code, we need predictable order for the assertion in test cases.
     */
    protected final static Comparator<Map.Entry<String, Integer>> comparator = (o1, o2) -> {
        int ret = o1.getValue().compareTo(o2.getValue());
        if (ret == 0) {
            ret = o1.getKey().compareTo(o2.getKey());
        }
        return ret;
    };
}
