Embulk::JavaPlugin.register_executor(
  "remoteserver", "org.embulk.executor.remoteserver.RemoteServerExecutor",
  File.expand_path('../../../../classpath', __FILE__))
