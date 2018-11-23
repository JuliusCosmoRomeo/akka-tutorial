package de.hpi.octopus;

import java.util.Scanner;
import java.util.*;
import java.io.*;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import de.hpi.octopus.actors.Profiler;
import de.hpi.octopus.actors.Worker;
import de.hpi.octopus.actors.listeners.ClusterListener;

public class OctopusMaster extends OctopusSystem {
	
	public static final String MASTER_ROLE = "master";

	public static void start(String actorSystemName, int workers, String host, int port) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		
		final ActorSystem system = createSystem(actorSystemName, config);
		
		Cluster.get(system).registerOnMemberUp(new Runnable() {
			@Override
			public void run() {
				system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
			//	system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);

				system.actorOf(Profiler.props(), Profiler.DEFAULT_NAME);
				
				for (int i = 0; i < workers; i++)
					system.actorOf(Worker.props(), Worker.DEFAULT_NAME + i);
				
			//	int maxInstancesPerNode = workers; // TODO: Every node gets the same number of workers, so it cannot be a parameter for the slave nodes
			//	Set<String> useRoles = new HashSet<>(Arrays.asList("master", "slave"));
			//	ActorRef router = system.actorOf(
			//		new ClusterRouterPool(
			//			new AdaptiveLoadBalancingPool(SystemLoadAverageMetricsSelector.getInstance(), 0),
			//			new ClusterRouterPoolSettings(10000, workers, true, new HashSet<>(Arrays.asList("master", "slave"))))
			//		.props(Props.create(Worker.class)), "router");
			}
		});

        ArrayList<Integer> ids = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        ArrayList<String> pwhashes = new ArrayList<>();
        ArrayList<String> genes = new ArrayList<>();


        //parse input csv file
        try {
            final Scanner scanner = new Scanner(new File("../data/students.csv"));
            scanner.useDelimiter(";|\n");

            //skip header
            scanner.nextLine();

            int argCount = 0;
            while(scanner.hasNext()){
                String next = scanner.next();
                System.out.println("Next " + next);
                switch(argCount%4){
                    case 0:
                        ids.add(Integer.parseInt(next));
                        break;
                    case 1:
                        names.add(next);
                        break;
                    case 2:
                        pwhashes.add(next);
                        break;
                    case 3:
                        genes.add(next);
                        break;
                }

                argCount++;
            }
            scanner.close();
            //int attributes = Integer.parseInt(line);

            system.actorSelection("/user/" + Profiler.DEFAULT_NAME).tell(new Profiler.PWCrackingTaskMessage(ids), ActorRef.noSender());
            system.actorSelection("/user/" + Profiler.DEFAULT_NAME).tell(new Profiler.TaskMessage(300), ActorRef.noSender());
        } catch (FileNotFoundException e){
            System.out.println("File not found exception");
        }

	}
}
