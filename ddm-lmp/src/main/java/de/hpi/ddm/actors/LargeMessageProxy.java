package de.hpi.ddm.actors;

import java.io.*;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	private int messageSession = (new Random()).nextInt();
	private int currentSession = -1;
	private SortedMap<Integer,BytesMessage<?>> messageMap = new TreeMap<Integer, BytesMessage<?>>();

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;

		/*public LargeMessage(T data, T sender) {
			this.message = data;
			this.receiver = (ActorRef) sender;
		}*/


		public ActorRef getReceiver() {
			return receiver;
		}

		public T getMessage() {
			return message;
		}
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
		private int sessionID = -1;
		private int messageID = -1;
		private boolean hasNext = false;

		/*public BytesMessage(T message, T sender, T receiver, int i, int j, boolean b) {
            bytes = message;
            this.sender = (ActorRef) sender;
            this.receiver = (ActorRef) receiver;
            this.sessionID = i;
            this.messageID = j;
            this.hasNext = b;
		}*/

		public T getBytes(){
			return bytes;
		}

		public ActorRef getSender(){
			return sender;
		}

		public ActorRef getReceiver(){
			return receiver;
		}

		public boolean hasNext(){
			return hasNext;
		}

		public int getSessionID() {
			return sessionID;
		}

		public int getMessageID() {
			return messageID;
		}
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		int batchSize = 10000;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		//BytesMessage<?> byteMessage = new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver());
		//System.out.println(byteMessage.getBytes());
		byte[] yourBytes = null;

		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(message.getMessage());
			out.flush();
			yourBytes = bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}

		for(int i=0; i<yourBytes.length; i += batchSize){
			byte[] byteBatch = Arrays.copyOfRange(yourBytes, i, Math.min(i+batchSize, yourBytes.length));
            
			if(i+batchSize < yourBytes.length){
                try {
                    /* In oreder to be reliable over weak Wifi so the channel will not be overloaded */
                    TimeUnit.MILLISECONDS.sleep(5);
                } catch (InterruptedException ex) {
                    Logger.getLogger(LargeMessageProxy.class.getName()).log(Level.SEVERE, null, ex);
                }
				receiverProxy.tell(new BytesMessage<>(byteBatch, this.sender(), message.getReceiver(), i/batchSize,
						messageSession, true), this.self());
			} else {
                try {
                    /* In oreder to be reliable over weak Wifi so the channel will not be overloaded */
                    TimeUnit.MILLISECONDS.sleep(5);
                } catch (InterruptedException ex) {
                    Logger.getLogger(LargeMessageProxy.class.getName()).log(Level.SEVERE, null, ex);
                }
				receiverProxy.tell(new BytesMessage<>(byteBatch, this.sender(), message.getReceiver(), i/batchSize,
						messageSession, false), this.self());
			}

		}

		messageSession++;
	}

	private void handle(BytesMessage<?> message) {
		//TODO: Größere Messages, check if complete, ordering
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.

		if(currentSession == -1){
			currentSession = message.getMessageID();
		}
		if(message.getMessageID() != currentSession){
			return;
		} else {
			messageMap.put(message.getSessionID(), message);
			if(messageComplete(messageMap)){
				byte[] bytes = recreateMessage(messageMap);
                
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInput in;
                Object obj;
                try {
                    in = new ObjectInputStream(bis);
                    obj = in.readObject();
                    message.getReceiver().tell(obj, message.getSender());
                } catch (IOException | ClassNotFoundException ex) {
                    Logger.getLogger(LargeMessageProxy.class.getName()).log(Level.SEVERE, null, ex);
                }
                
				currentSession = -1;
			}
		}
	}

	private byte[] recreateMessage(SortedMap<Integer, BytesMessage<?>> messageMap) {
		Set<Integer> keys = messageMap.keySet();
		List<Byte> byteList = new ArrayList<>();
		for(Integer key : keys){
			byte[] hey = (byte[]) messageMap.get(key).getBytes();
			for(int i=0; i<hey.length; i++){
				byteList.add(hey[i]);
			}
		}
		byte[] res = new byte[byteList.size()];
		for(int i=0; i<byteList.size(); i++){
			res[i] = (byte) byteList.get(i);
		}
		return res;
	}

	private boolean messageComplete(SortedMap<Integer, BytesMessage<?>> messageMap) {
		if(messageMap.firstKey() != 0){
			return false;
		}
		if(messageMap.get(messageMap.lastKey()).hasNext){
			return false;
		}
		Set<Integer> keys = messageMap.keySet();
		int currentValue = 0;
		for(Integer key : keys){
			if(key != currentValue){
				return false;
			}
			currentValue++;
		}
		return true;
	}
}
