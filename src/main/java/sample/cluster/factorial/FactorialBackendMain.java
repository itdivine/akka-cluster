package sample.cluster.factorial;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class FactorialBackendMain {

  public static void main(String[] args) {
    // Override the configuration of the port when specified as program argument
    final String port = args.length > 0 ? args[0] : "0";
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port).
      withFallback(ConfigFactory.parseString("akka.sample.cluster.roles = [backend]")).
      withFallback(ConfigFactory.load("factorial"));

    ActorSystem system = ActorSystem.create("ClusterSystem", config);

    system.actorOf(Props.create(FactorialBackend.class), "factorialBackend");

    system.actorOf(Props.create(MetricsListener.class), "metricsListener");

  }

}
