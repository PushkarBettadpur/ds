package client;


import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Message;
import app_kvClient.TextMessage;
import java.net.UnknownHostException;

import app_kvClient.Client;
import app_kvClient.ClientSocketListener;
import java.io.IOException;


public class KVStore implements KVCommInterface, ClientSocketListener {

	private String address;
	private int port;
	private Client client;
	private static final String PROMPT = "Client> ";
	boolean requestCompleted = false;

	private String key;
	private String value;
	private KVMessage.StatusType status;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws UnknownHostException, IOException {
		client = new Client(address, port);
		client.addListener(this);
		client.start();

		try {
			Thread.sleep(500);
			TextMessage serverReply = client.receiveMessage();
			handleNewMessage(serverReply);
		} catch (Exception e) {
			System.out.println("Exception while connecting to the server.");
		}
	}

	@Override
	public void disconnect() {
		if(client != null) {
			client.closeConnection();
			client = null;
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {

		Message newRequest = new Message(key, value, KVMessage.StatusType.PUT);
		sendMessage(newRequest.toString());

		TextMessage serverReply = this.receiveMessage();
		handleNewMessage(serverReply);

		return new Message(this.key, this.value, this.status);
	}

	@Override
	public KVMessage get(String key) throws Exception {

		Message newRequest = new Message(key, "null", KVMessage.StatusType.GET);
		sendMessage(newRequest.toString());

		TextMessage serverReply = this.receiveMessage();
		handleNewMessage(serverReply);

		return new Message(this.key, this.value, this.status);

	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

	public void sendMessage(String msg){
		try {
			client.sendMessage(new TextMessage(msg));
		} catch (IOException e) {
			printError("Unable to send message!");
			disconnect();
		}
	}

	private TextMessage receiveMessage() {
		try {
			return client.receiveMessage();
		} catch (IOException e) {
			printError("Unable to receive message!");
			disconnect();
		}
		return null;
	}

	public Client getClient() {
		return this.client;
	}

	@Override
	public void handleNewMessage(TextMessage msg) {

		String[] tokens = msg.getMsg().split(" ", 3);

			try {
				this.status = KVMessage.StatusType.valueOf(tokens[0]);
				this.key = tokens[1];
				this.value = tokens[2];
				System.out.print(tokens[0] + "<[" + tokens[1]+"]");
				if (!tokens[2].equals("null")) {
					System.out.print(": " + tokens[2]);
				}
				System.out.println(">");
			} catch (Exception e) {
				System.out.println(msg.getMsg());
			}

			if (msg.getMsg().trim().equals("Server aborted")) {
				disconnect();
			}
	}

	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: "
					+ address + " / " + port);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: "
					+ address + " / " + port);
			System.out.print(PROMPT);
		}
	}
}
