package io.palyvos.scheduler.adapters.liebre;

import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.SpeAdapter;
import io.palyvos.scheduler.adapters.SpeRuntimeInfo;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskIndex;
import io.palyvos.scheduler.util.QueryGraphFileParser;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;

public class LiebreAdapter implements SpeAdapter {

  public static final Function<String, String> THREAD_NAME_GRAPHITE_CONVERTER =
      s -> s.replace(".", "-");
  public static final String SPE_NAME = "liebre";

  private final QueryGraphFileParser queryGraphFileParser = new QueryGraphFileParser();
  private final OsAdapter osAdapter;
  private final String queryGraphPath;
  private final int pid;

  private SpeRuntimeInfo speRuntimeInfo;
  private TaskIndex taskIndex;

  public LiebreAdapter(int pid, String queryGraphPath) {
    this(pid, new LinuxAdapter(), queryGraphPath);
  }

  public LiebreAdapter(int pid, OsAdapter osAdapter, String queryGraphPath) {
    Validate.isTrue(pid > 1, "invalid pid");
    Validate.notEmpty(queryGraphPath, "Path to query graph is empty!");
    Validate.notNull(osAdapter, "osAdapter");
    this.pid = pid;
    this.osAdapter = osAdapter;
    this.queryGraphPath = queryGraphPath;
  }

  @Override
  public void updateState() {
    Collection<Task> tasks = queryGraphFileParser.loadTasks(queryGraphPath,
        id -> Task.ofSingleSubtask(id, SPE_NAME));
    List<ExternalThread> threads = osAdapter.retrieveThreads(pid);
    LiebreThreadAssigner.assign(tasks, threads);
    this.taskIndex = new TaskIndex(tasks);
    this.speRuntimeInfo = new SpeRuntimeInfo(Collections.singletonList(pid), threads, SPE_NAME);
  }

  @Override
  public SpeRuntimeInfo runtimeInfo() {
    return speRuntimeInfo;
  }

  @Override
  public TaskIndex taskIndex() {
    return taskIndex;
  }

}
