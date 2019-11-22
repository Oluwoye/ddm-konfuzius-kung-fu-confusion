package de.hpi.ddm.actors;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.dsl.Creators;
import de.hpi.ddm.structures.BloomFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import de.hpi.ddm.actors.Worker.ResultMessage;
import de.hpi.ddm.actors.Worker.CrackedPassword;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";
	public ArrayList<String> texts = new ArrayList<>();
	private char[] alphabet = null;
	private ArrayList<char[]> subAlphabets = new ArrayList<>();
	private int currentID = 1;
	private ArrayList<String> passwordsToCrack = new ArrayList<>();
	private int passwordLength;
	private boolean finished = false;

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.readyWorkers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;

		public List<String[]> getLines(){
			return lines;
		}
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
    
    @Data @NoArgsConstructor @AllArgsConstructor
    public class WorkMessage implements Serializable {
		private static final long serialVersionUID = 3303081603964723997L;
        
        private ArrayList<String> texts;
        private ActorRef sender;

        public ArrayList<String> getTexts(){
        	return texts;
		}

		public ActorRef getSender(){
        	return sender;
		}
    }

	@Data @NoArgsConstructor @AllArgsConstructor
	protected class PassWordCrackMessage implements Serializable {
		private static final long serialVersionUID = 3303081603961111997L;

		private ArrayList<Character> passwordAlphabet;
		private String password;
		private int passwordLength;
		private ActorRef sender;
		private int line;

		public ArrayList<Character> getPasswordAlphabet(){
			return passwordAlphabet;
		}

		public String getPassword(){
			return password;
		}

		public int getPasswordLength(){
			return passwordLength;
		}

		public ActorRef getSender(){
			return sender;
		}

		public int getLine() {
			return line;
		}
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> readyWorkers;
	private final List<ActorRef> workingWorkers = new ArrayList<>();
    
    private long permutationIndex = 0;

    boolean workBeeingDone = false;
    
	private long startTime;
    
     @AllArgsConstructor
    class HashToCrack {
        String hash;
        int row, hintIndex;
        String solution;

        public HashToCrack(String hash, int row, int hintIndex){
        	this.hash = hash;
        	this.row = row;
        	this.hintIndex = hintIndex;
		}

        public String getHash(){
        	return hash;
		}

		public void setSolution(String solution){
        	this.solution = solution;
		}

		 public String getSolution() {
        	return solution;
		 }

		 public int getRow() {
        	return row;
		 }
	 }
    
    private ArrayList<HashToCrack> hashsToCrack = new ArrayList<>();
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

    
    
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(ResultMessage.class, this::handle)
				.match(CrackedPassword.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(CrackedPassword message){
		for(int i=0; i<workingWorkers.size(); i++){
			if(workingWorkers.get(i) == message.getSender()){
				readyWorkers.add(workingWorkers.get(i));
				workingWorkers.remove(i);
				break;
			}
		}

		// System.out.println("Cracked a password " + message.getSolution());
		collector.tell(new Collector.CollectMessage("Password for line " + message.getLineNumber() + " is " +
				message.getSolution()), this.self());

		if(!finished){
			crackPasswords();
		} else if (workingWorkers.isEmpty()){
			collector.tell(new Collector.PrintMessage(), this.self());
		}
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
    
    private void distributeWork() {
		for(int i=0; i<readyWorkers.size(); i++) {
			ArrayList<String> job = new ArrayList<>();
			for(int j=0; j<100; j++){
				job.add(String.valueOf(subAlphabets.get(0)));
				char[] currentPermutation = subAlphabets.get(0);
				int[] currentPermutationAsInt = new int[currentPermutation.length];
				for(int k=0; k<currentPermutation.length; k++){
					currentPermutationAsInt[k] = currentPermutation[k];
				}
				if(findNextPermutation(currentPermutationAsInt)){
					for(int k=0; k<currentPermutation.length; k++){
						currentPermutation[k] = (char) (currentPermutationAsInt[k]);
					}
					subAlphabets.set(0, currentPermutation);
				} else {
					subAlphabets.remove(0);
					if(subAlphabets.isEmpty()){
						break;
					}
				}
			}
			ActorRef worker = readyWorkers.get(0);
			workingWorkers.add(worker);
			readyWorkers.remove(0);
			worker.tell(new WorkMessage(job, this.self()), this.self());
		}

    }

    protected void handle (ResultMessage message){
		for(int i=0; i<workingWorkers.size(); i++){
			if(workingWorkers.get(i) == message.getSender()){
				readyWorkers.add(workingWorkers.get(i));
				workingWorkers.remove(i);
				break;
			}
		}

		Set<String> keys = message.getResults().keySet();
		for (int i=0; i < hashsToCrack.size(); i++){
			for (String key : keys) {
				if(key.equals(hashsToCrack.get(i).getHash())){
					hashsToCrack.get(i).setSolution(message.getResults().get(key));
					System.out.println(hashsToCrack.get(i).getSolution());
				}
			}
		}

		if(subAlphabets.isEmpty()){
			crackPasswords();
		} else {
			distributeWork();
		}
	}

	protected void handle(BatchMessage message) {
		if (message.getLines().isEmpty()) {
			//printAllKLengthRec(alphabet, "", alphabet.length, alphabet.length-1);
			distributeWork();
			//crackPasswords(message.getLines());
		} else {
            for (String[] line : message.getLines()) {
            	if(alphabet == null){
					alphabet = line[2].toCharArray();
					createSubAlphabet(alphabet);
				}
                int numHints = line.length - 5;
            	passwordLength = Integer.parseInt(line[3]);
				passwordsToCrack.add(line[4]);
                for (int i = 0; i < numHints; i++) {
                    hashsToCrack.add(new HashToCrack(line[i+5], Integer.parseInt(line[0]), i));
                }
            }

           // this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
            this.reader.tell(new Reader.ReadMessage(), this.self());
        }
	}

	private void createSubAlphabet(char[] alphabet) {
		Arrays.sort(alphabet);
		subAlphabets.add(Arrays.copyOfRange(alphabet, 0, alphabet.length - 1));
		for(int i=0; i<alphabet.length-1; i++){
			char[] alphabet_tmp = new char[alphabet.length];
			for(int j=0; j<alphabet.length; j++){
				alphabet_tmp[j] = alphabet[j];
			}
			alphabet_tmp[i] = alphabet[alphabet.length - 1];
			Arrays.sort(alphabet_tmp);
			subAlphabets.add(Arrays.copyOfRange(alphabet_tmp, 0, alphabet.length - 1));
		}
	}

	private void crackPasswords() {
		for(int i=0; i<readyWorkers.size(); i++) {
			ArrayList<Character> passwordAlphabet = new ArrayList<>();
			for (char c : alphabet) {
				passwordAlphabet.add(c);
			}

			for (HashToCrack hashToCrack : hashsToCrack) {
				if (hashToCrack.getRow() == currentID) {
					String solution = hashToCrack.getSolution();
					for (char c : alphabet) {
						if (solution.indexOf(c) < 0 && passwordAlphabet.indexOf(c) >= 0) {
							passwordAlphabet.remove(passwordAlphabet.indexOf(c));
						}
					}
				}
			}

			if(passwordAlphabet.size() != alphabet.length){
				ActorRef worker = readyWorkers.get(0);
				workingWorkers.add(worker);
				readyWorkers.remove(0);
				worker.tell(new PassWordCrackMessage(passwordAlphabet, passwordsToCrack.get(currentID - 1), passwordLength,
						this.self(), currentID), this.self());
				currentID++;
			} else {
				finished = true;
			}
		}
	}

	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.readyWorkers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.readyWorkers.add(this.sender());
		// distributeWork();
        
        /*if (workBeeingDone) {
            distributeWork(this.sender());
        }*/
        this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		if(this.readyWorkers.contains(message.getActor())){
			this.readyWorkers.remove(message.getActor());
		} else if (this.workingWorkers.contains(message.getActor())){
			this.workingWorkers.remove(message.getActor());
		}
		this.log().info("Unregistered {}", message.getActor());
	}

	// Function to swap the data
	// present in the left and right indices
	public static int[] swap(int data[], int left, int right)
	{

		// Swap the data
		int temp = data[left];
		data[left] = data[right];
		data[right] = temp;

		// Return the updated array
		return data;
	}

	// Function to reverse the sub-array
	// starting from left to the right
	// both inclusive
	public static int[] reverse(int data[], int left, int right)
	{

		// Reverse the sub-array
		while (left < right) {
			int temp = data[left];
			data[left++] = data[right];
			data[right--] = temp;
		}

		// Return the updated array
		return data;
	}

	// Function to find the next permutation
	// of the given integer array
	public static boolean findNextPermutation(int data[])
	{

		// If the given dataset is empty
		// or contains only one element
		// next_permutation is not possible
		if (data.length <= 1)
			return false;

		int last = data.length - 2;

		// find the longest non-increasing suffix
		// and find the pivot
		while (last >= 0) {
			if (data[last] < data[last + 1]) {
				break;
			}
			last--;
		}

		// If there is no increasing pair
		// there is no higher order permutation
		if (last < 0)
			return false;

		int nextGreater = data.length - 1;

		// Find the rightmost successor to the pivot
		for (int i = data.length - 1; i > last; i--) {
			if (data[i] > data[last]) {
				nextGreater = i;
				break;
			}
		}

		// Swap the successor and the pivot
		data = swap(data, nextGreater, last);

		// Reverse the suffix
		data = reverse(data, last + 1, data.length - 1);

		// Return true as the next_permutation is done
		return true;
	}
}
