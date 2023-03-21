package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingPerfOptimizationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingPerfOptimizationUtil.class);

    public static boolean isEligibleForNullEquijoinOptimization(
        boolean isKeyAnyNulls,
        boolean isEquijoin,
        boolean isBatchMode,
        boolean statelessNullKeysEnabled,
        boolean isOuterJoin
    ){
        if (statelessNullKeysEnabled && isEquijoin && !isBatchMode && isOuterJoin && isKeyAnyNulls) {
            return true;
        }
        return false;
    }

    public static boolean isKeyAnyNulls(RowData key) {
        for (int i = 0; i < key.getArity(); i++) {
            if (key.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }
    
}