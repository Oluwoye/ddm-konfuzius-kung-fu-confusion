package de.hpi.ddm.actors;

import java.io.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

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

		public LargeMessage(T data, T sender) {
			this.message = data;
			this.receiver = (ActorRef) sender;
		}


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
		private boolean hasNext = false;

		public BytesMessage(T message, T sender, T receiver, int i, boolean b) {

		}

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
			System.out.println(yourBytes);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}

		for(int i=0; i<yourBytes.length; i++){
			if(i != yourBytes.length - 1){
				receiverProxy.tell(new BytesMessage<>(yourBytes[i], this.sender(), message.getReceiver(), i, true), this.self());
			} else {
				receiverProxy.tell(new BytesMessage<>(yourBytes[i], this.sender(), message.getReceiver(), i, false), this.self());
			}

		}
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}
