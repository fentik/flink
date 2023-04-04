package com.fentik;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.runtime.SavepointLoader;

public class RescaleSavepoint {

  static private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    if (args.length == 0) {
      System.err.println("Usage: java RescaleSavepoint <savepoint-path> [rescale-cuttoff-mb]");
      System.exit(1);
    }

    String path = args[0];
    long rescaleCutoff = 1073741824;
    if (args.length > 1) {
      rescaleCutoff = Long.parseLong(args[1]) * 1048576;
    }

    long totalSize = 0;
    int numOperators = 0;
    int parallelism = 0;
    boolean alreadyRescaled = false;
    ArrayList<String> noRescaleOperators = new ArrayList<String>();

    String[] histogramLabels = {
        "=<    256MB",
        "=<    1GB  ",
        "=<    4GB  ",
        "=<    8GB  ",
        "=<   16GB  ",
        "=<   32GB  ",
        "=<   64GB  ",
        "=<  128GB  ",
        "=<  256GB  ",
        ">          "
    };

    long[] historgramThesholds = {
        256 * (1L << 20),
        1L << 30,
        4 * (1L << 30),
        8 * (1L << 30),
        16 * (1L << 30),
        32 * (1L << 30),
        64 * (1L << 30),
        128 * (1L << 30),
        256 * (1L << 30),
        Long.MAX_VALUE
    };
    long[] histogramCounts = new long[histogramLabels.length];

    try {
      CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);

      for (OperatorState os : metadata.getOperatorStates()) {
          if (parallelism == 0) {
              parallelism = os.getParallelism();
              continue;
          }
          if (os.getParallelism() != parallelism) {
              if (!alreadyRescaled) {
                  System.err.println("Checkpoint already rescaled, will preserve the large operators");
                  alreadyRescaled = true;
              }
              parallelism = Math.max(parallelism, os.getParallelism());
          }
      }

      for (OperatorState os : metadata.getOperatorStates()) {
        long size = os.getStateSize();
        if (size == 0) {
          // not a stateful operator
          continue;
        }

        if (alreadyRescaled) {
            // Savepoint was previously rescaled, preserve the scale factors by ignoring size and
            // returning all the operators at max parallelism.
            if (os.getParallelism() == parallelism) {
                noRescaleOperators.add(os.getOperatorID() + "," + os.getParallelism());
            }
        } else if (size > rescaleCutoff) {
          noRescaleOperators.add(os.getOperatorID() + "," + os.getParallelism());
        }

        totalSize += size;
        numOperators += 1;
        for (int i = 0; i < historgramThesholds.length; i++) {
          if (size <= historgramThesholds[i]) {
            histogramCounts[i]++;
            break;
          }
        }
        // System.out.println(" " + os.toString() + "(total size " +
        // humanReadableBytes(os.getStateSize()) + ")");
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    // Use stderr for stats, to keep stdout machine-readable.
    System.err.println("Total size: " + humanReadableBytes(totalSize));
    System.err.println("Parallelism: " + parallelism);
    System.err.println("Total number of stateful operators: " + numOperators);
    System.err.println("Operator distribution by size:");
    for (int i = 0; i < histogramCounts.length; i++) {
      System.err.println("  " + histogramLabels[i] + ": " + histogramCounts[i]);
    }
    System.err.println("Using per operator cutoff " + humanReadableBytes(rescaleCutoff));
    System.err.println("Configure execution.checkpointing.per-operator-parallelism to:");

    System.out.println(String.join(",", noRescaleOperators));
  }

  private static String humanReadableBytes(long bytes) {
    if (-1000 < bytes && bytes < 1000) {
      return bytes + " B";
    }
    CharacterIterator ci = new StringCharacterIterator("kMGTPE");
    while (bytes <= -999_950 || bytes >= 999_950) {
      bytes /= 1000;
      ci.next();
    }
    return String.format("%.1f %cB", bytes / 1000.0, ci.current());
  }
}
