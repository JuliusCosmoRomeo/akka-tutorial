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
import java.util.ArrayList;
import java.util.List;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Profiler.CompletionMessage;
import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import com.google.common.hash.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;

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
        private ArrayList<String> pws;
		private String pw;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class HashingWorkMessage extends WorkMessage  implements Serializable {
		private static final long serialVersionUID = -4643194361868862395L;
		private HashingWorkMessage() {}
		private String partner;
		private int prefix;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class GeneWorkMessage extends WorkMessage implements Serializable {
		private static final long serialVersionUID = -3643194361868862395L;
		private GeneWorkMessage() {}
		private int geneIndex;
		private ArrayList<String> genes;
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
			result = longestOverlapPartner(msg.geneIndex, msg.genes);
		} else if(message instanceof HashingWorkMessage){
			HashingWorkMessage msg = (HashingWorkMessage) message;
			result = hashTask(msg.partner, msg.prefix);
		} else if(message instanceof LinCombWorkMessage){
            LinCombWorkMessage msg = (LinCombWorkMessage) message;
            result = findLinComb(msg.pws, msg.pw);
        }
        this.log.info("done: " + message.getClass().getSimpleName() + " " + result);
		this.sender().tell(new CompletionMessage(result), this.self());

	}

	private String crackPW(String hash){
        String result = "";

        for (int i=0;i<1000000;i++){
            String sha256hex = this.hash((i+""));
            if (sha256hex.equals(hash)){
                result = i + "";
                break;
            }
        }
        return result;
	}

	//gene analysis - taken from the slides
    private String longestOverlapPartner(int thisIndex, List<String> sequences) {
        int bestOtherIndex = -1;
        String bestOverlap = "";
        for (int otherIndex = 0; otherIndex < sequences.size(); otherIndex++) {
            if (otherIndex == thisIndex)
                continue;

            String longestOverlap = this.longestOverlap(sequences.get(thisIndex), sequences.get(otherIndex));

            if (bestOverlap.length() < longestOverlap.length()) {
                bestOverlap = longestOverlap;
                bestOtherIndex = otherIndex;
            }
        }
        return bestOtherIndex + "";
    }

    private String longestOverlap(String str1, String str2) {
        if (str1.isEmpty() || str2.isEmpty())
            return "";

        if (str1.length() > str2.length()) {
            String temp = str1;
            str1 = str2;
            str2 = temp;
        }

        int[] currentRow = new int[str1.length()];
        int[] lastRow = str2.length() > 1 ? new int[str1.length()] : null;
        int longestSubstringLength = 0;
        int longestSubstringStart = 0;

        for (int str2Index = 0; str2Index < str2.length(); str2Index++) {
            char str2Char = str2.charAt(str2Index);
            for (int str1Index = 0; str1Index < str1.length(); str1Index++) {
                int newLength;
                if (str1.charAt(str1Index) == str2Char) {
                    newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;

                    if (newLength > longestSubstringLength) {
                        longestSubstringLength = newLength;
                        longestSubstringStart = str1Index - (newLength - 1);
                    }
                } else {
                    newLength = 0;
                }
                currentRow[str1Index] = newLength;
            }
            int[] temp = currentRow;
            currentRow = lastRow;
            lastRow = temp;
        }
        return str1.substring(longestSubstringStart, longestSubstringStart + longestSubstringLength);
    }


    private String geneTask(String gene){
        return "b";
	}

	private String hash(String input){
	    return Hashing.sha256()
                .hashString(input, StandardCharsets.UTF_8)
                .toString();
    }

    private String findLinComb(ArrayList<String> pws, String pw){
		if (Integer.parseInt(pw)<500000){
	        return -1 + "";
        } else {
	        return 1 + "";

        }
    }


	private String hashTask(String partner, int prefix){

        String fullPrefix = "11111";
        if (prefix==-1){
            fullPrefix = "00000";

        }

        Random rand = new Random();

        String hash = this.hash(partner);
        while (!hash.startsWith(fullPrefix)){
            int nonce = rand.nextInt();
            hash = this.hash(partner + nonce);
        }
		return hash;

	}
}