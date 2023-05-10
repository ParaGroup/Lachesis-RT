package io.palyvos.scheduler.metric.graphite;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.regex.Pattern;

public class SimpleGraphiteReporter {

  private static final Pattern GRAPHITE_REJECT_PATTERN = Pattern.compile("[^A-Za-z0-9\\-_>]");
  private final int graphitePort;
  private final String graphiteHost;
  private Socket socket;
  private DataOutputStream output;

  public SimpleGraphiteReporter(String graphiteHost, int graphitePort) {
    this.graphiteHost = graphiteHost;
    this.graphitePort = graphitePort;
  }

  public void open() {
    try {
      socket = new Socket(graphiteHost, graphitePort);
      output = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void report(long timestampSeconds, String key, Object value) throws IOException {
    String message = key + " " + value + " " + timestampSeconds + "\n";
    output.writeBytes(message);
  }

  public void close() {
    try {
      output.flush();
      output.close();
      socket.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }


  public static String cleanGraphiteId(String thread) {
    return GRAPHITE_REJECT_PATTERN.matcher(thread).replaceAll("");
  }
}
