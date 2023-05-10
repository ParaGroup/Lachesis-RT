package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * {@link io.palyvos.scheduler.policy.cgroup.CGroupActionExecutor} that executes cgroup-management
 * commands in parallel using multiple threads
 */
class BasicCGroupActionExecutor implements CGroupActionExecutor {

  private static final Logger LOG = LogManager.getLogger();

  @Override
  public void create(Collection<CGroup> cgroups) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : cgroups) {
      futures.add(executor.submit(() -> cgroup.create()));
    }
    wait(futures, true);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  @Override
  public void delete(Collection<CGroup> cgroups) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : cgroups) {
      futures.add(executor.submit(() -> cgroup.delete()));
    }
    wait(futures, false);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  @Override
  public void updateParameters(Map<CGroup, Collection<CGroupParameterContainer>> rawSchedule) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : rawSchedule.keySet()) {
      Collection<CGroupParameterContainer> parameters = rawSchedule.get(cgroup);
      parameters.forEach(parameter ->
          futures.add(executor.submit(() -> cgroup.set(parameter.key(), parameter.value())))
      );
    }
    wait(futures, true);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  @Override
  public void updateAssignment(Map<CGroup, Collection<ExternalThread>> assignment) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : assignment.keySet()) {
      futures.add(executor.submit(() -> cgroup.classify(assignment.get(cgroup))));
    }
    wait(futures, true);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  private ExecutorService newExecutor() {
    return Executors.newFixedThreadPool(SchedulerContext.CGROUP_ENFORCER_THREADS);
  }

  private void wait(List<Future<Boolean>> futures, boolean checkSuccess) {
    for (Future<Boolean> future : futures) {
      try {
        final boolean success = future.get();
        Validate.validState(!checkSuccess || success, "Execution failure!");
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for policy to be applied");
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOG.error("Policy application failed!");
        throw new RuntimeException(e);
      }
    }
  }

}
