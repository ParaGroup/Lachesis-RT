package io.palyvos.scheduler.policy.single_priority;

import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.HelperTask;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SinglePriorityPolicy} that assigns a constant priority to all {@link Task}s.
 */
public class ConstantSinglePriorityPolicy implements SinglePriorityPolicy {

  private final double normalizedPriority;
  private final boolean scheduleHelpers;

  public ConstantSinglePriorityPolicy(long normalizedPriority, boolean scheduleHelpers) {
    this.normalizedPriority = normalizedPriority;
    this.scheduleHelpers = scheduleHelpers;
  }

  @Override
  public void init(SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {
  }

  @Override
  public void apply(Collection<Task> tasks,
      SpeRuntimeInfo speRuntimeInfo, SinglePriorityTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.apply(computeSchedule(tasks, speRuntimeInfo, metricProvider));
  }

  @Override
  public Map<ExternalThread, Double> computeSchedule(
      Collection<Task> tasks, SpeRuntimeInfo speRuntimeInfo,
      SchedulerMetricProvider metricProvider) {
    final Map<ExternalThread, Double> normalizedSchedule = new HashMap<>();
    for (Task task : tasks) {
      for (Subtask subtask : task.subtasks()) {
        normalizedSchedule.put(subtask.thread(), normalizedPriority);
        if (scheduleHelpers) {
          for (HelperTask helper : task.helpers()) {
            normalizedSchedule.put(helper.thread(), normalizedPriority);
          }
        }
      }
    }
    return normalizedSchedule;
  }

}
