Embulk::JavaPlugin.register_executor(
  "docker", "org.embulk.executor.docker.DockerExecutor",
  File.expand_path('../../../../classpath', __FILE__))
