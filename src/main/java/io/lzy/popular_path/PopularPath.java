package io.lzy.popular_path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import io.lzy.popular_path.model.GraphRandom;
import io.lzy.popular_path.model.GraphSequence;

/**
 * @author zhiyan
 */
@Slf4j
public class PopularPath {

    private final static int TOP_N_POPULAR_PATH = 3;

    private static Integer handlePvtTimes(final String arg) {
        try {
            return Integer.parseInt(arg);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static InputStream handleAccessLogFilePathArg(final String arg) {
        Preconditions.checkNotNull(arg);

        InputStream is = null;
        String accessLog = arg.trim();

        try {
            if (accessLog.equals("-")) {    // be friendly to unix pipeline
                is = System.in;
            } else {
                is = new FileInputStream(accessLog);
            }
        } catch (Exception e) {
            log.error(String.format("Open access log file %s failed: %s",
                    accessLog.equals("-") ? "stdin" : accessLog, e.getMessage()));
        }

        return is;
    }

    private static Boolean handleAOEKindFlagArg(final String arg) {
        if (arg == null || arg.trim().isEmpty() || arg.trim().equals("0") ||
                arg.trim().equals("false") || arg.trim().equals("n") || arg.trim().equals("no")) {
            return false;
        } else if (arg.trim().equals("1") || arg.trim().equals("true") || arg.trim().equals("y") ||
                arg.trim().equals("yes"))
            return true;
        else {
            return null;
        }
    }

    private static void printEvaluationResult(final Map<String, List<Map.Entry<String, Integer>>> result) {
        System.out.println(String.format("Access log evaluation result: (total user(s) = %d)", result.size()));

        result.entrySet().forEach(user -> {
            System.out.println(String.format("Visitor: %s", user.getKey()));
            System.out.println(String.format(
                    "== Paths (Order by total node access frequencies, total path(s) = %d) ==",
                    user.getValue().size()));

            user.getValue().forEach(path ->
                System.out.println(String.format("%-40s(total node frequencies: %d)", path.getKey(), path.getValue()))
            );

            System.out.println();
        });
    }

    private static void printEvaluationResult(final String user, final List<Map.Entry<String, Integer>> result) {
        Map<String, List<Map.Entry<String, Integer>>> r = new HashMap<>();
        r.put(user, result);
        printEvaluationResult(r);
    }

    public static void main(String[] args) {
        Integer pvtLogParseTimes = null, pvtPathEvalTimes = null;
        Boolean isAOEKind = null;
        InputStream stream = null;
        String user = "";
        boolean failed = false;
        long begin, end;

        log.warn("The program is used to process sample access log for performance or function test only, " +
                "the supported arguments are very limited. " +
                "Please check the source code or the unit tests to explore complete and more powerful interfaces, " +
                "and using them as a library from your program are encouraged.");

        log.debug("Process started. " +
                "(debug level log output are enabled by default in logback to show internal details)");

        if (args.length == 4) {     // Input access log file path
            pvtLogParseTimes = handlePvtTimes(args[0]);
            pvtPathEvalTimes = handlePvtTimes(args[1]);
            isAOEKind = handleAOEKindFlagArg(args[2]);
            stream = handleAccessLogFilePathArg(args[3]);
        } else if (args.length == 5) { // Input access log and user name
            pvtLogParseTimes = handlePvtTimes(args[0]);
            pvtPathEvalTimes = handlePvtTimes(args[1]);
            isAOEKind = handleAOEKindFlagArg(args[2]);
            stream = handleAccessLogFilePathArg(args[3]);
            user = args[4].trim();
        } else {
            log.info("Wrong input arguments. Usage: " +
                    "PopularPath <PVT-log-parse-times> <PVT-path-eval-times> <AOE-kind-graph-flag> " +
                    "<-|access-log-file-path> [user-name-to-display]");
            failed = true;
        }

        if (!(failed || pvtLogParseTimes == null || pvtPathEvalTimes == null || isAOEKind == null || stream == null)) {
            try {
                if (isAOEKind) {
                    log.debug("GraphSequence is used.");

                    GraphSequence graph = null;

                    begin = System.currentTimeMillis();

                    for (int times = 0; times < pvtLogParseTimes; times ++) {  // loop for performance test
                        graph = new GraphSequence();
                        LogParser.parseLog(stream, graph);
                        stream.close();
                        stream = handleAccessLogFilePathArg(args[3]);
                    }

                    end = System.currentTimeMillis();

                    log.info(String.format("Time expended by parsing log to graph %d times: %dms",
                            pvtLogParseTimes, end - begin));

                    begin = System.currentTimeMillis();

                    for (int times = 0; times < pvtPathEvalTimes; times ++) {   // loop for performance test
                        if (times == 0) {
                            if (user.isEmpty()) {
                                printEvaluationResult(graph.getPopularPath(TOP_N_POPULAR_PATH));
                            } else {
                                printEvaluationResult(user, graph.getPopularPath(TOP_N_POPULAR_PATH, user));
                            }
                        } else {
                            System.out.println("Skip same output.");
                        }
                    }

                    end = System.currentTimeMillis();

                    log.info(String.format("Time expended by evaluating popular path %d times: %dms",
                            pvtPathEvalTimes, end - begin));
                } else {
                    log.debug("GraphRandom is used.");

                    GraphRandom graph = null;

                    begin = System.currentTimeMillis();

                    for (int times = 0; times < pvtLogParseTimes; times ++) {  // loop for performance test
                        graph = new GraphRandom();
                        LogParser.parseLog(stream, graph);
                        stream.close();
                        stream = handleAccessLogFilePathArg(args[3]);
                    }

                    end = System.currentTimeMillis();

                    log.info(String.format("Time expended by parsing log to graph %d times: %dms",
                            pvtLogParseTimes, end - begin));

                    begin = System.currentTimeMillis();

                    for (int times = 0; times < pvtPathEvalTimes; times ++) {  // loop for performance test
                        if (times == 0) {
                            if (user.isEmpty()) {
                                printEvaluationResult(graph.getAllPopularPath(TOP_N_POPULAR_PATH));
                            } else {
                                printEvaluationResult(user, graph.getPopularPath(TOP_N_POPULAR_PATH, user));
                            }
                        } else {
                            System.out.println("Skip same output.");
                        }
                    }

                    end = System.currentTimeMillis();

                    log.info(String.format("Time expended by evaluating popular path %d times: %dms",
                            pvtPathEvalTimes, end - begin));
                }
            } catch (IOException e) {
                log.error(e.getMessage());
                failed = true;
            } finally {
                try {
                    stream.close();
                } catch (Exception e) {
                    // nothing to do
                }
            }
        }

        if (failed) {
            log.error("Process failed, exit abnormally.");
            System.exit(1);
        } else {
            log.debug("Process finished, exit normally.");
            System.exit(0);
        }
    }
}
