package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Worker.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Profiler extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "profiler";

	public static Props props() {
		return Props.create(Profiler.class);
	}

	ArrayList<String> crackedPws = new ArrayList<>();
	ArrayList<String> doneLinCombs = new ArrayList<>();
	ArrayList<String> doneGeneTasks = new ArrayList<>();
	ArrayList<String> doneHashingTasks = new ArrayList<>();
	int amountOfDataPts = 42;

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @AllArgsConstructor
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 4545299661052078209L;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = -8330958742629706627L;
		private TaskMessage() {}
		private int attributes;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class PWCrackingTaskMessage extends TaskMessage implements Serializable {
		private static final long serialVersionUID = -7330958742629706627L;
		public PWCrackingTaskMessage() {}
		private ArrayList<String> pws;
	}


    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class GeneTaskMessage extends TaskMessage implements Serializable {
        private static final long serialVersionUID = -6330958742629706627L;
        public GeneTaskMessage() {}
        private ArrayList<String> genes;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class LinCombTaskMessage extends TaskMessage implements Serializable {
        private static final long serialVersionUID = -5330958742629706627L;
        public LinCombTaskMessage() {}
        private ArrayList<String> pws;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class HashingTaskMessage extends TaskMessage implements Serializable {
        private static final long serialVersionUID = -4330958742629706627L;
        public HashingTaskMessage() {}
        private ArrayList<String> prefixes;
        private ArrayList<String> partners;
    }


    @Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		private CompletionMessage() {}
		private String result;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private final Queue<WorkMessage> unassignedWork = new LinkedList<>();
	private final Queue<ActorRef> idleWorkers = new LinkedList<>();
	private final Map<ActorRef, WorkMessage> busyWorkers = new HashMap<>();

	private TaskMessage task1;
    private TaskMessage task2;


	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RegistrationMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(TaskMessage.class, this::handle)
				.match(CompletionMessage.class, this::handle)
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		
		this.assign(this.sender());
		this.log.info("Registered {}", this.sender());
	}
	
	private void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		
		if (!this.idleWorkers.remove(message.getActor())) {
			WorkMessage work = this.busyWorkers.remove(message.getActor());
			if (work != null) {
				this.assign(work);
			}
		}
		this.log.info("Unregistered {}", message.getActor());
	}
	
	private void handle(TaskMessage message) {
		if (this.task1==null){
            this.task1 = message;
			PWCrackingTaskMessage task = (PWCrackingTaskMessage) message;
            for (String pw : task.pws){
				this.assign(new Worker.PWCrackingWorkMessage(pw));
			}
        } else if (this.task2==null){
            this.task2 = message;
			GeneTaskMessage task = (GeneTaskMessage) message;
			for (String gene : task.genes){
				this.assign(new Worker.GeneWorkMessage(gene));
			}
        }
	    if (this.task1 != null && this.task2 != null)
			this.log.error("The profiler actor can process only two tasks in its current implementation!");

	}
	
	private void handle(CompletionMessage message) {
		ActorRef worker = this.sender();
		WorkMessage work = this.busyWorkers.remove(worker);

		//if task done => next task
		if(work instanceof Worker.PWCrackingWorkMessage){
			//count up amount of done pw cracks
			crackedPws.add(message.result);
			if (amountOfDataPts==crackedPws.size()){
				log.info("finished pw cracking");
				log.info("starting lin comb");
				this.task1 = new LinCombTaskMessage(crackedPws);
			}

		} else if(work instanceof Worker.GeneWorkMessage){
			//count up amount of done gene
			doneGeneTasks.add(message.result);
			if (amountOfDataPts==doneGeneTasks.size()){
				log.info("finished gene task");
				if (amountOfDataPts == doneLinCombs.size()){
					log.info("starting hashing task");
					this.task1 = new HashingTaskMessage(doneLinCombs, doneGeneTasks);
				}
			}

		} else if(work instanceof Worker.LinCombWorkMessage){
			//count up amount of done lin combs
			doneLinCombs.add(message.result);
			if (amountOfDataPts==doneLinCombs.size()){
				log.info("finished lin combs");
				if (amountOfDataPts == doneGeneTasks.size()){
					log.info("starting hashing task");
					this.task1 = new HashingTaskMessage(doneLinCombs, doneGeneTasks);
				}
			}

		} else if(work instanceof Worker.HashingWorkMessage){
			//count up amount of done hashings
			doneHashingTasks.add(message.result);
			if (amountOfDataPts==doneHashingTasks.size()){
				log.info("finished hashing - srrf - done with all tasks :)");
				//this.task1 = new LinCombTaskMessage(doneHashingTasks);
			}
		}

		this.log.info("Completed: ", message.result);

		this.assign(worker);
	}
	
	private void assign(WorkMessage work) {
		ActorRef worker = this.idleWorkers.poll();
		
		if (worker == null) {
			this.unassignedWork.add(work);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}
	
	private void assign(ActorRef worker) {
		WorkMessage work = this.unassignedWork.poll();
		
		if (work == null) {
			this.idleWorkers.add(worker);
			return;
		}
		
		this.busyWorkers.put(worker, work);
		worker.tell(work, this.self());
	}

}