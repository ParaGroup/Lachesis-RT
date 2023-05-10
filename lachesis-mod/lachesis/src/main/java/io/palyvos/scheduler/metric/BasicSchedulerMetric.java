package io.palyvos.scheduler.metric;

public enum BasicSchedulerMetric implements SchedulerMetric {
  /**
   * Relatively long time compared to query runtime
   */
  SUBTASK_TUPLES_IN_TOTAL,
  SUBTASK_TUPLES_OUT_TOTAL,
  /**
   * Recent compared to query runtime
   */
  SUBTASK_TUPLES_IN_RECENT,
  SUBTASK_TUPLES_OUT_RECENT,
  INPUT_OUTPUT_QUEUE_SIZE,
  INPUT_OUTPUT_EXTERNAL_QUEUE_SIZE,
  INPUT_OUTPUT_KAFKA_QUEUE_SIZE,
  INPUT_EXTERNAL_QUEUE_SIZE,
  INPUT_KAFKA_QUEUE_SIZE,
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA,
  TASK_OUTPUT_QUEUE_SIZE_FROM_SUBTASK_DATA,
  TASK_QUEUE_SIZE_RECENT_FROM_SUBTASK_DATA,
  SUBTASK_SELECTIVITY,
  THREAD_CPU_UTILIZATION,
  SUBTASK_CPU_UTILIZATION,
  SUBTASK_COST,
  /**
   * output_tuples/input_tuple
   */
  SUBTASK_GLOBAL_SELECTIVITY,
  /**
   * cost/input_tuple
   */
  SUBTASK_GLOBAL_AVERAGE_COST,
  /**
   * output_tuples/cost
   */
  SUBTASK_GLOBAL_RATE,
  TASK_LATENCY;

}
