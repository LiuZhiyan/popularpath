package io.lzy.popular_path.model;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.testng.annotations.Test;

import io.lzy.popular_path.LogParser;
import io.lzy.popular_path.TestBase;

import static org.testng.Assert.*;

/**
 * @author zhiyan
 */
public class GraphRandomTest extends TestBase {

    @Test
    public void testGraphRandomWithMultipleUsersAndCrossAccess() {
        StringBuffer testLog = new StringBuffer();
        testLog.append("U1\t/\n");
        testLog.append("U1\tN1\n");
        testLog.append("U2\t/\n");
        testLog.append("U2\tN1\n");
        testLog.append("U1\tN2\n");
        testLog.append("U1\tN3\n");
        testLog.append("U2\tN2\n");
        testLog.append("U2\tN3\n");
        testLog.append("U3\t/\n");
        testLog.append("U3\tN4\n");
        testLog.append("U3\tN5\n");

        InputStream stream = new ByteArrayInputStream(testLog.toString().getBytes());
        GraphRandom graph = new GraphRandom();
        try {
            LogParser.parseLog(stream, graph);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        Map<String, List<Map.Entry<String, Integer>>> ret1;
        ret1 = graph.getAllPopularPath(TOP(5));
        // {U1=[/N1/N2=3, N1/N2/N3=3], U2=[N1/N2/N3=3, /N1/N2=3], U3=[/N4/N5=3]}
        assertEquals(ret1.keySet().size(), 3);
        List<String> edgeOwners = new ArrayList<>(ret1.keySet());
        Collections.sort(edgeOwners);
        assertEquals(edgeOwners.size(), 3);
        assertEquals(edgeOwners.get(0), TEST_USER_1);
        assertEquals(edgeOwners.get(1), TEST_USER_2);
        assertEquals(edgeOwners.get(2), TEST_USER_3);
        ret1.get(TEST_USER_1).sort(comparator);
        assertEquals(ret1.get(TEST_USER_1).size(), 2);
        assertEquals(ret1.get(TEST_USER_1).get(0).getKey(), "/N1/N2");
        assertEquals(ret1.get(TEST_USER_1).get(0).getValue(), new Integer(3));
        assertEquals(ret1.get(TEST_USER_1).get(1).getKey(), "N1/N2/N3");
        assertEquals(ret1.get(TEST_USER_1).get(1).getValue(), new Integer(3));
        ret1.get(TEST_USER_2).sort(comparator);
        assertEquals(ret1.get(TEST_USER_2).size(), 2);
        assertEquals(ret1.get(TEST_USER_2).get(0).getKey(), "/N1/N2");
        assertEquals(ret1.get(TEST_USER_2).get(0).getValue(), new Integer(3));
        assertEquals(ret1.get(TEST_USER_2).get(1).getKey(), "N1/N2/N3");
        assertEquals(ret1.get(TEST_USER_2).get(1).getValue(), new Integer(3));
        ret1.get(TEST_USER_3).sort(comparator);
        assertEquals(ret1.get(TEST_USER_3).size(), 1);
        assertEquals(ret1.get(TEST_USER_3).get(0).getKey(), "/N4/N5");
        assertEquals(ret1.get(TEST_USER_3).get(0).getValue(), new Integer(3));

        ret1 = graph.getAllPopularPath(DEPTH(2), TOP(3));
        // {U1=[/N1=2, N1/N2=2, N2/N3=2], U2=[/N1=2, N2/N3=2, N1/N2=2], U3=[/N4=2, N4/N5=2]}
        assertEquals(ret1.keySet().size(), 3);
        edgeOwners = new ArrayList<>(ret1.keySet());
        Collections.sort(edgeOwners);
        assertEquals(edgeOwners.size(), 3);
        assertEquals(edgeOwners.get(0), TEST_USER_1);
        assertEquals(edgeOwners.get(1), TEST_USER_2);
        assertEquals(edgeOwners.get(2), TEST_USER_3);
        ret1.get(TEST_USER_1).sort(comparator);
        assertEquals(ret1.get(TEST_USER_1).size(), 3);
        assertEquals(ret1.get(TEST_USER_1).get(0).getKey(), "/N1");
        assertEquals(ret1.get(TEST_USER_1).get(0).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_1).get(1).getKey(), "N1/N2");
        assertEquals(ret1.get(TEST_USER_1).get(1).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_1).get(2).getKey(), "N2/N3");
        assertEquals(ret1.get(TEST_USER_1).get(2).getValue(), new Integer(2));
        ret1.get(TEST_USER_2).sort(comparator);
        assertEquals(ret1.get(TEST_USER_2).size(), 3);
        assertEquals(ret1.get(TEST_USER_2).get(0).getKey(), "/N1");
        assertEquals(ret1.get(TEST_USER_2).get(0).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_2).get(1).getKey(), "N1/N2");
        assertEquals(ret1.get(TEST_USER_2).get(1).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_2).get(2).getKey(), "N2/N3");
        assertEquals(ret1.get(TEST_USER_2).get(2).getValue(), new Integer(2));
        ret1.get(TEST_USER_3).sort(comparator);
        assertEquals(ret1.get(TEST_USER_3).size(), 2);
        assertEquals(ret1.get(TEST_USER_3).get(0).getKey(), "/N4");
        assertEquals(ret1.get(TEST_USER_3).get(0).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_3).get(1).getKey(), "N4/N5");
        assertEquals(ret1.get(TEST_USER_3).get(1).getValue(), new Integer(2));

        List<Map.Entry<String, Integer>> ret2;
        ret2 = graph.getPopularPath(DEPTH(4), TOP(3), TEST_USER_1); //[/N1/N2/N3=4]
        assertEquals(ret2.size(), 1);
        assertEquals(ret2.get(0).getKey(), "/N1/N2/N3");
        assertEquals(ret2.get(0).getValue(), new Integer(4));

        ret2 = graph.getPopularPath(TOP(3), TEST_USER_2);   // [/N1/N2=3, N1/N2/N3=3]
        assertEquals(ret2.size(), 2);
        ret2.sort(comparator);
        assertEquals(ret2.get(0).getKey(), "/N1/N2");
        assertEquals(ret2.get(0).getValue(), new Integer(3));
        assertEquals(ret2.get(1).getKey(), "N1/N2/N3");
        assertEquals(ret2.get(1).getValue(), new Integer(3));

        ret2 = graph.getPopularPath(TOP(3), TEST_USER_3);   // [/N4/N5=3]
        assertEquals(ret2.size(), 1);
        ret2.sort(comparator);
        assertEquals(ret2.get(0).getKey(), "/N4/N5");
        assertEquals(ret2.get(0).getValue(), new Integer(3));

        ret2 = graph.getPopularPath(DEPTH(4), TOP(1), TEST_USER_3);   // []
        assertEquals(ret2.size(), 0);
    }

    @Test
    public void testGraphRandomWithSingleUserAndMultipleAccess() {
        StringBuffer testLog = new StringBuffer();
        testLog.append("U1\t/\n");
        testLog.append("U1\tN1\n");
        testLog.append("U1\tN2\n");
        testLog.append("U1\tN3\n");
        testLog.append("U1\tN3\n");     // refresh node, will be skipped
        testLog.append("U1\tN2\n");     // access visited node, add frequency
        testLog.append("U1\tN1\n");     // access visited node, add frequency

        InputStream stream = new ByteArrayInputStream(testLog.toString().getBytes());
        GraphRandom graph = new GraphRandom();
        try {
            LogParser.parseLog(stream, graph);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        Map<String, List<Map.Entry<String, Integer>>> ret1;
        ret1 = graph.getAllPopularPath(TOP(5));
        // {U1=[/N1/N2=5, N3/N2/N1=5,
        //      N1/N2/N3=5,
        //      N2/N3/N2=5]}
        assertEquals(ret1.keySet().size(), 1);
        assertEquals(ret1.keySet().toArray()[0], TEST_USER_1);
        ret1.get(TEST_USER_1).sort(comparator);
        assertEquals(ret1.get(TEST_USER_1).size(), 4);
        assertEquals(ret1.get(TEST_USER_1).get(0).getKey(), "/N1/N2");
        assertEquals(ret1.get(TEST_USER_1).get(0).getValue(), new Integer(5));
        assertEquals(ret1.get(TEST_USER_1).get(1).getKey(), "N1/N2/N3");
        assertEquals(ret1.get(TEST_USER_1).get(1).getValue(), new Integer(5));
        assertEquals(ret1.get(TEST_USER_1).get(2).getKey(), "N2/N3/N2");
        assertEquals(ret1.get(TEST_USER_1).get(2).getValue(), new Integer(5));
        assertEquals(ret1.get(TEST_USER_1).get(3).getKey(), "N3/N2/N1");
        assertEquals(ret1.get(TEST_USER_1).get(3).getValue(), new Integer(5));

        ret1 = graph.getAllPopularPath(DEPTH(2), TOP(2));
        // {U1=[N1/N2=4,
        //      N2/N1=4]}
        assertEquals(ret1.keySet().size(), 1);
        assertEquals(ret1.keySet().toArray()[0], TEST_USER_1);
        ret1.get(TEST_USER_1).sort(comparator);
        assertEquals(ret1.get(TEST_USER_1).size(), 2);
        assertEquals(ret1.get(TEST_USER_1).get(0).getKey(), "N1/N2");
        assertEquals(ret1.get(TEST_USER_1).get(0).getValue(), new Integer(4));
        assertEquals(ret1.get(TEST_USER_1).get(1).getKey(), "N2/N1");
        assertEquals(ret1.get(TEST_USER_1).get(1).getValue(), new Integer(4));

        List<Map.Entry<String, Integer>> ret2;
        ret2 = graph.getPopularPath(DEPTH(4), TOP(3), TEST_USER_1);
        // [N1/N2/N3/N2=7, N2/N3/N2/N1=7, /N1/N2/N3=6]
        assertEquals(ret2.size(), 3);
        ret2.sort(comparator);
        assertEquals(ret2.get(0).getKey(), "/N1/N2/N3");
        assertEquals(ret2.get(0).getValue(), new Integer(6));
        assertEquals(ret2.get(1).getKey(), "N1/N2/N3/N2");
        assertEquals(ret2.get(1).getValue(), new Integer(7));
        assertEquals(ret2.get(2).getKey(), "N2/N3/N2/N1");
        assertEquals(ret2.get(2).getValue(), new Integer(7));

        ret2 = graph.getPopularPath(TOP(10), TEST_USER_1);
        // [N1/N2/N3=5, N2/N3/N2=5, /N1/N2=5, N3/N2/N1=5]
        assertEquals(ret2.size(), 4);
        ret2.sort(comparator);
        assertEquals(ret2.get(0).getKey(), "/N1/N2");
        assertEquals(ret2.get(0).getValue(), new Integer(5));
        assertEquals(ret2.get(1).getKey(), "N1/N2/N3");
        assertEquals(ret2.get(1).getValue(), new Integer(5));
        assertEquals(ret2.get(2).getKey(), "N2/N3/N2");
        assertEquals(ret2.get(2).getValue(), new Integer(5));
        assertEquals(ret2.get(3).getKey(), "N3/N2/N1");
        assertEquals(ret2.get(3).getValue(), new Integer(5));
    }

    @Test
    public void testGraphRandomWithMultipleUserAndMultipleAccess() {
        StringBuffer testLog = new StringBuffer();
        testLog.append("U1\t/\n");
        testLog.append("U1\tN1\n");
        testLog.append("U1\tN2\n");
        testLog.append("U1\tN3\n");
        testLog.append("U1\tN3\n");     // refresh node, will be skipped
        testLog.append("U2\tN2\n");     // access visited node by other user
        testLog.append("U2\tN1\n");     // access visited node by other user

        InputStream stream = new ByteArrayInputStream(testLog.toString().getBytes());
        GraphRandom graph = new GraphRandom();
        try {
            LogParser.parseLog(stream, graph);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        Map<String, List<Map.Entry<String, Integer>>> ret1;
        ret1 = graph.getAllPopularPath(DEPTH(2), TOP(5));
        // {U1=[N1/N2=2, /N1=2, N2/N3=2], U2=[N2/N1=2]}
        assertEquals(ret1.keySet().size(), 2);
        assertEquals(ret1.keySet().toArray()[0], TEST_USER_1);
        assertEquals(ret1.keySet().toArray()[1], TEST_USER_2);
        ret1.get(TEST_USER_1).sort(comparator);
        assertEquals(ret1.get(TEST_USER_1).size(), 3);
        assertEquals(ret1.get(TEST_USER_1).get(0).getKey(), "/N1");
        assertEquals(ret1.get(TEST_USER_1).get(0).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_1).get(1).getKey(), "N1/N2");
        assertEquals(ret1.get(TEST_USER_1).get(1).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_1).get(2).getKey(), "N2/N3");
        assertEquals(ret1.get(TEST_USER_1).get(2).getValue(), new Integer(2));
        assertEquals(ret1.get(TEST_USER_2).get(0).getKey(), "N2/N1");
        assertEquals(ret1.get(TEST_USER_2).get(0).getValue(), new Integer(2));
    }
}
