package de.hpi.octopus.actors;

import java.io.Serializable;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Profiler.CompletionMessage;
import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessage implements Serializable {
		private static final long serialVersionUID = -7643194361868862395L;
		//private WorkMessage() {}
/*		private int[] x;
		private int[] y;*/
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class PWCrackingWorkMessage extends WorkMessage  implements Serializable {
		private static final long serialVersionUID = -6643194361868862395L;
		private PWCrackingWorkMessage() {}
		private String pw;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class LinCombWorkMessage extends WorkMessage  implements Serializable {
		private static final long serialVersionUID = -5643194361868862395L;
		private LinCombWorkMessage() {}
		private String pw;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class HashingWorkMessage extends WorkMessage  implements Serializable {
		private static final long serialVersionUID = -4643194361868862395L;
		private HashingWorkMessage() {}
		private String prefix;
		private String partner;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class GeneWorkMessage extends WorkMessage implements Serializable {
		private static final long serialVersionUID = -3643194361868862395L;
		private GeneWorkMessage() {}
		private String gene;
	}

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(WorkMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if (member.hasRole(OctopusMaster.MASTER_ROLE))
			this.getContext()
				.actorSelection(member.address() + "/user/" + Profiler.DEFAULT_NAME)
				.tell(new RegistrationMessage(), this.self());
	}

	private void handle(WorkMessage message) {
		String result = "";
		if(message instanceof PWCrackingWorkMessage){
			PWCrackingWorkMessage msg = (PWCrackingWorkMessage) message;
			result = crackPW(msg.pw);
		} else if(message instanceof GeneWorkMessage){
			GeneWorkMessage msg = (GeneWorkMessage) message;
			result = geneTask(msg.gene);
		} else if(message instanceof HashingWorkMessage){
			HashingWorkMessage msg = (HashingWorkMessage) message;
			result = hashTask(msg.partner, msg.prefix);
		}
        this.log.info("done: " + message.getClass().getName());
		this.sender().tell(new CompletionMessage(result), this.self());

		/*this.log.info("message: " + message.x + " " + message.y);
		long y = 0;
		for (int i = 0; i < 1000000; i++)
			if (this.isPrime(i))
				y = y + i;
		
		this.log.info("done: " + y);
		*/
	}

	private String crackPW(String hash){
		//TODO: pass
        return "";
	}

	private String geneTask(String gene){
        return "";
	}

	private String hashTask(String prefix, String partner){
        return "";
	}
}