package de.hpi.octopus.actors;

import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Collections;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Worker.WorkMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import akka.actor.PoisonPill;


public class Profiler extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "profiler";

	public static Props props() {
		return Props.create(Profiler.class);
	}

	int amountOfDataPts = 42;

	ArrayList<String> crackedPws = new ArrayList<>();
	int[] pws = new int[42];
	ArrayList<Integer> prefixes = new ArrayList<>();
	ArrayList<String> analyzedGenes = new ArrayList<>();
	ArrayList<String> doneHashingTasks = new ArrayList<>();

    long startTime = System.nanoTime();
    long workTime = 0;

    long maxLinCombValue = Long.MAX_VALUE;
    long currentLinCombValue = 500000000;
    int batchSize = 100000;


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
        private int[] pws;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class HashingTaskMessage extends TaskMessage implements Serializable {
        private static final long serialVersionUID = -4330958742629706627L;
        public HashingTaskMessage() {}
        private ArrayList<Integer> prefixes;
        private ArrayList<String> partners;
    }


    @Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class CompletionMessage implements Serializable {
		private static final long serialVersionUID = -6823011111281387872L;
		//private CompletionMessage() {}
		private String result;
		private int[] prefixes;
		private long duration;
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
    public void preStart() throws Exception {
        super.preStart();
        //reaper register
        Reaper.watchWithDefaultReaper(this);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        log.info("Stopped {}.", this.self());
        //forward pp

        for (ActorRef worker : idleWorkers){
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        for (ActorRef worker : busyWorkers.keySet()){
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
    }

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
	    log.info("New task message " + message.getClass().getSimpleName());
	    if (this.task1 != null && this.task2 != null){
			this.log.error("The profiler actor can process only two tasks in its current implementation!");
	        return;
        }
		if (this.task1==null){
            this.task1 = message;
            if (message instanceof PWCrackingTaskMessage){
                startTime = System.nanoTime();
                PWCrackingTaskMessage task = (PWCrackingTaskMessage) message;
                for (String pw : task.pws){
                    this.assign(new Worker.PWCrackingWorkMessage(pw));
                }
            } else if (message instanceof LinCombTaskMessage){
                LinCombTaskMessage task = (LinCombTaskMessage) message;

                for (int i=0;i<50;i++){
                    this.assign(new Worker.LinCombWorkMessage(pws, currentLinCombValue, currentLinCombValue+batchSize));
                    currentLinCombValue += batchSize;
                }
            } else if (message instanceof HashingTaskMessage){
                HashingTaskMessage task = (HashingTaskMessage) message;
                log.info("task partners/prefixes size " + task.partners.size() + " " + task.prefixes.size());
                for (int i=0;i<amountOfDataPts;i++){
                    this.assign(new Worker.HashingWorkMessage(task.partners.get(i), task.prefixes.get(i)));
                }
            }
        } else if (this.task2==null){
            this.task2 = message;
			GeneTaskMessage task = (GeneTaskMessage) message;
			for (int i=0;i<task.genes.size();i++){
				this.assign(new Worker.GeneWorkMessage(i, task.genes));
			}
        }
	}
	
	private void handle(CompletionMessage message) {
		ActorRef worker = this.sender();
		WorkMessage work = this.busyWorkers.remove(worker);
		//this.log.info("Completed: " + message.result);
        this.workTime += message.duration;

		//if task done => next task
		if(work instanceof Worker.PWCrackingWorkMessage){
			//count up amount of done pw cracks
			crackedPws.add(message.result);
			if (amountOfDataPts==crackedPws.size()){
				log.info("finished pw cracking");
				log.info("starting lin comb");
				this.task1 = null;
				for (int i=0;i<crackedPws.size();i++){
				    pws[i] = Integer.parseInt(crackedPws.get(i));
                }
                this.getSelf().tell(new LinCombTaskMessage(pws), this.getSelf());

			}

		} else if(work instanceof Worker.GeneWorkMessage){
			//count up amount of done gene
			analyzedGenes.add(message.result);
			if (amountOfDataPts == analyzedGenes.size()){
				log.info("finished gene task");
				if (amountOfDataPts == prefixes.size()){
					log.info("starting hashing task");
					//this.task1 = new HashingTaskMessage(prefixes, analyzedGenes);
					this.task1 = null;
					this.task2 = null;
                    this.getSelf().tell(new HashingTaskMessage(prefixes, analyzedGenes), this.getSelf());
				}
			}
		} else if(work instanceof Worker.LinCombWorkMessage){
			//count up amount of done lin combs
			if(message.prefixes!=null){
			    prefixes = new ArrayList<>();
				for (int prefix : message.prefixes){
					prefixes.add(prefix);
				}
                log.info("finished lin combs");
                if (amountOfDataPts == analyzedGenes.size()){
                    log.info("starting hashing task");
                    this.task1 = null;
                    this.task2 = null;
                    this.getSelf().tell(new HashingTaskMessage(prefixes, analyzedGenes), this.getSelf());
                }
            } else {
                if (this.task1 instanceof LinCombTaskMessage || this.task2 instanceof LinCombTaskMessage ){
					this.assign(new Worker.LinCombWorkMessage(pws, currentLinCombValue, currentLinCombValue+batchSize));
					currentLinCombValue += batchSize;
				}
            }

		} else if(work instanceof Worker.HashingWorkMessage){
			//count up amount of done hashings
			doneHashingTasks.add(message.result);
			if (amountOfDataPts==doneHashingTasks.size()){
				log.info("finished hashing - nice - done with all tasks :)");
                log.info("Time since Profiler started " + (System.nanoTime() - startTime)/1000000000 + " s");
                log.info("Accumulated work time " + workTime/1000000000 + " s");

                this.getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}


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