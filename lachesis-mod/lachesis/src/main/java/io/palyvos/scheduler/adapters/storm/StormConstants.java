package io.palyvos.scheduler.adapters.storm;

import java.util.regex.Pattern;

public class StormConstants {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 8080;
  public static final String STORM_WORKER_CLASS = "org.apache.storm.daemon.worker";
  public static final Pattern EXECUTOR_THREAD_PATTERN = Pattern
      .compile("Thread-\\d+-(.+)-executor\\[(\\d+) \\d+\\]");
  public static final Pattern DISRUPTOR_THREAD_PATTERN = Pattern
      .compile("Thread-\\d+-disruptor-executor\\[(\\d+) \\d+\\]-send-queue");
  public static final String ACKER_NAME = "_acker";
  public static final String METRIC_REPORTER_NAME = "MetricReporter";
  public static final Pattern METRIC_REPORTER_THREAD_PATTERN = Pattern
      .compile("Thread-\\d+-.+storm\\.metrics\\.MetricReporter-executor\\[\\d+ \\d+\\]");

  private StormConstants() {
  }
}
