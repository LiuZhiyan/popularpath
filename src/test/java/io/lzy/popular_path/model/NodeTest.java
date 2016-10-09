package io.lzy.popular_path.model;

import org.testng.annotations.Test;

import java.util.Map;

import io.lzy.popular_path.TestBase;

import static org.testng.Assert.*;

/**
 * @author zhiyan
 */
public class NodeTest extends TestBase {

    @Test
    public void testRootNode() {
        Map.Entry<Node, Edge> rootItem = Node.createRootNode(TEST_USER_1);

        assertEquals(rootItem.getKey().getName(), TEST_NODE_ROOT);

        assertEquals(rootItem.getKey().getInEdges().size(), 1);
        assertEquals(rootItem.getKey().getInEdges().get(0).getOwner(), TEST_USER_1);
        assertNull(rootItem.getKey().getInEdges().get(0).getInNode());
        assertEquals(rootItem.getKey().getInEdges().get(0).getOutNode(), rootItem.getKey());

        assertEquals(rootItem.getKey().getOutEdges().size(), 0);

        assertEquals(rootItem.getKey().getRefCount(TEST_USER_1), 1);
        assertEquals(rootItem.getKey().addRef(TEST_USER_1), 2);
        rootItem.getKey().addRef(TEST_USER_1);
        assertEquals(rootItem.getKey().getRefCount(TEST_USER_1), 3);

        assertEquals(rootItem.getKey().getRefCount(TEST_USER_2), 0);
    }

    @Test
    public void testChildNode() {
        Map.Entry<Node, Edge> rootItem = Node.createRootNode(TEST_USER_1);
        Map.Entry<Node, Edge> childItem1 =
                Node.createNode(TEST_NODE_CHILD_1, rootItem.getKey(), rootItem.getValue(), TEST_USER_1);
        Map.Entry<Node, Edge> childItem2 =
                Node.createNode(TEST_NODE_CHILD_2, childItem1.getKey(), childItem1.getValue(), TEST_USER_1);
        Map.Entry<Node, Edge> childItem3 =
                Node.createNode(TEST_NODE_CHILD_3, childItem2.getKey(), childItem2.getValue(), TEST_USER_1);
        Map.Entry<Node, Edge> childItem4 =
                Node.createNode(TEST_NODE_CHILD_4, childItem3.getKey(), childItem3.getValue(), TEST_USER_1);
        Map.Entry<Node, Edge> childItem5 =
                Node.createNode(TEST_NODE_CHILD_5, childItem4.getKey(), childItem4.getValue(), TEST_USER_1);

        assertEquals(rootItem.getKey().getName(), TEST_NODE_ROOT);
        assertEquals(childItem1.getKey().getName(), TEST_NODE_CHILD_1);
        assertEquals(childItem2.getKey().getName(), TEST_NODE_CHILD_2);
        assertEquals(childItem3.getKey().getName(), TEST_NODE_CHILD_3);
        assertEquals(childItem4.getKey().getName(), TEST_NODE_CHILD_4);
        assertEquals(childItem5.getKey().getName(), TEST_NODE_CHILD_5);

        assertEquals(rootItem.getKey().getInEdges().size(), 1);
        assertEquals(rootItem.getKey().getInEdges().get(0).getOwner(), TEST_USER_1);
        assertNull(rootItem.getKey().getInEdges().get(0).getInNode());
        assertEquals(rootItem.getKey().getInEdges().get(0).getOutNode(), rootItem.getKey());

        assertEquals(rootItem.getKey().getOutEdges().size(), 1);
        assertEquals(rootItem.getKey().getOutEdges().get(0).getOwner(), TEST_USER_1);
        assertEquals(rootItem.getKey().getOutEdges().get(0).getInNode(), rootItem.getKey());
        assertEquals(rootItem.getKey().getOutEdges().get(0).getOutNode(), childItem1.getKey());

        assertEquals(childItem1.getKey().getInEdges().size(), 1);
        assertEquals(childItem1.getKey().getInEdges().get(0).getOwner(), TEST_USER_1);
        assertEquals(childItem1.getKey().getInEdges().get(0).getInNode(), rootItem.getKey());
        assertEquals(childItem1.getKey().getInEdges().get(0).getOutNode(), childItem1.getKey());

        assertEquals(childItem1.getKey().getOutEdges().size(), 1);
        assertEquals(childItem1.getKey().getOutEdges().get(0).getOwner(), TEST_USER_1);
        assertEquals(childItem1.getKey().getOutEdges().get(0).getInNode(), childItem1.getKey());
        assertEquals(childItem1.getKey().getOutEdges().get(0).getOutNode(), childItem2.getKey());

        assertEquals(childItem5.getKey().getInEdges().size(), 1);
        assertEquals(childItem5.getKey().getInEdges().get(0).getOwner(), TEST_USER_1);
        assertEquals(childItem5.getKey().getInEdges().get(0).getInNode(), childItem4.getKey());
        assertEquals(childItem5.getKey().getInEdges().get(0).getOutNode(), childItem5.getKey());
        assertEquals(childItem5.getKey().getInEdges().get(0).getPreEdges().size(), 1);
        assertEquals(childItem5.getKey().getInEdges().get(0).getPreEdges().get(0),
                childItem4.getKey().getInEdges().get(0));

        assertEquals(childItem5.getKey().getOutEdges().size(), 0);

        assertTrue(childItem4.getKey().isParent(childItem3.getKey(), TEST_USER_1));
        assertFalse(childItem4.getKey().isParent(childItem2.getKey(), TEST_USER_1));

        assertTrue(childItem4.getKey().hasPreEdge(childItem3.getKey(), TEST_USER_1,
                childItem3.getKey().getInEdges().get(0)));
        assertFalse(childItem4.getKey().hasPreEdge(childItem2.getKey(), TEST_USER_1,
                childItem3.getKey().getInEdges().get(0)));
        assertTrue(childItem5.getKey().hasPreEdge(childItem4.getKey(), TEST_USER_1,
                childItem4.getValue()));

        rootItem.getKey().linkParent(childItem5.getKey(), childItem5.getValue(), TEST_USER_1);
        assertEquals(childItem5.getKey().getOutEdges().size(), 1);
        assertEquals(rootItem.getKey().getInEdges().size(), 2);
        assertEquals(rootItem.getKey().getOutEdges().size(), 1);
        assertTrue(rootItem.getKey().isParent(childItem5.getKey(), TEST_USER_1));
        assertFalse(childItem5.getKey().isParent(rootItem.getKey(), TEST_USER_2));
        assertTrue(rootItem.getKey().hasPreEdge(childItem5.getKey(), TEST_USER_1, childItem5.getValue()));

    }
}
