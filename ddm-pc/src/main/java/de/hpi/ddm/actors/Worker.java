package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;

import de.hpi.ddm.actors.Master.WorkMessage;
import de.hpi.ddm.actors.Master.PassWordCrackMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";
	private String passwordSoluted = "";
	private boolean returnImmediately = false;

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ResultMessage implements Serializable {
		private static final long serialVersionUID = 3303081603964723997L;

		private SortedMap<String,String> results = new TreeMap<>();
		private ActorRef sender;

		public ResultMessage(ActorRef self) {
			this.sender = self;
		}

		public SortedMap<String,String> getResults(){
			return results;
		}

		public ActorRef getSender(){
			return sender;
		}

		public void addKeyValuePair(String key, String value){
			results.put(key,value);
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	protected class CrackedPassword {
		private String solution;
		private int lineNumber;
		private ActorRef sender;

		public String getSolution(){
			return solution;
		}

		public int getLineNumber() { return lineNumber; }

		public ActorRef getSender(){
			return sender;
		}
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
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
				.match(MemberRemoved.class, this::handle)
                .match(WorkMessage.class, this::handle)
				.match(PassWordCrackMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(PassWordCrackMessage message){
		ArrayList<Character> alphabet = message.getPasswordAlphabet();
		char[] alphabetChars = new char[alphabet.size()];
		for(int i=0; i<alphabet.size();i++){
			alphabetChars[i] = alphabet.get(i);
		}

		printAllKLengthRec(alphabetChars, "", alphabetChars.length, message.getPasswordLength(), message.getPassword());
		message.getSender().tell(new CrackedPassword(passwordSoluted, message.getLine(), this.self()), this.self());
		passwordSoluted = "";
		returnImmediately = false;
	}

    private void handle(WorkMessage message) {
        //System.out.println("Someone told me to work I don't");
        ResultMessage result = new ResultMessage(this.self());
        for (int i=0; i<message.getTexts().size(); i++){
        	String text = message.getTexts().get(i);
        	String hashedText = hash(text);
			result.addKeyValuePair(hashedText, text);
		}
		message.getSender().tell(result, this.self());

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
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

	//This takes way tooooooo long
	public void printAllKLengthRec(char[] set,
								   String prefix,
								   int n, int k, String solution)
	{

		if(returnImmediately){
			return;
		}
		// Base case: k is 0,
		// print prefix
		if (k == 0)
		{
			if(hash(prefix).equals(solution)){
				passwordSoluted = prefix;
				returnImmediately = true;
			}
			return;
		}

		// One by one add all characters
		// from set and recursively
		// call for k equals to k-1
		for (int i = 0; i < n; ++i)
		{

			// Next character of input added
			String newPrefix = prefix + set[i];

			// k is decreased, because
			// we have added a new character
			printAllKLengthRec(set, newPrefix,
					n, k - 1, solution);
		}
	}
}