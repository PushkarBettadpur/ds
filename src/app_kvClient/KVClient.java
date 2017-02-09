package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import app_kvClient.Client;
import app_kvClient.ClientSocketListener;
import app_kvClient.TextMessage;

import client.KVStore;
import common.messages.Message;


public class KVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "Client> ";
	private BufferedReader stdin;
	private boolean stop = false;
	KVStore kvClient = null;

	private String serverAddress;
	private int serverPort;

	public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");


		if(tokens[0].equals("quit")) {
			stop = true;
			kvClient.disconnect();
			System.out.println(PROMPT + "Application exit!");

		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					this.kvClient = new KVStore(serverAddress, serverPort);
					kvClient.connect();
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}

		} else  if (tokens[0].equals("send")) {
			if(tokens.length == 2) {
				if(kvClient != null && kvClient.getClient() != null
					&& kvClient.getClient().isRunning()) {

					StringBuilder msg = new StringBuilder();
					for(int i = 1; i < tokens.length; i++) {
						msg.append(tokens[i]);
						if (i != tokens.length -1 ) {
							msg.append(" ");
						}
					}
					kvClient.sendMessage(msg.toString());
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No message passed!");
			}

		}
		else if(tokens[0].equals("put")) {
			if(kvClient != null && kvClient.getClient() != null &&
			 	kvClient.getClient().isRunning()) {
				try {
					if(tokens.length == 3) {

						String	key = tokens[1];
						String	value = tokens[2];

						if(key.length() > 20)
						{
							printError("Max Length of Key is 20 bytes");
						}
						else if(value.length()>(120*1024))
						{
							printError("Max Length of Value is 120KBytes");
						}
						else
						{
							kvClient.put(key, value);
						}


						//logger.info("Requested \t" + "(" key + "," + value + ")");
					}
					else if(tokens.length > 3)
					{
						String key = tokens[1];
						String [] value = cmdLine.split(" ",3);

						//System.out.println(value[2] + tokens.length);

						if(key.length() > 20)
						{
							printError("Max Length of Key is 20 bytes");
						}
						else if(value[2].length()>(120*1024))
						{
							printError("Max Length of Value is 120KBytes");
						}
						else
						{
							kvClient.put(key,value[2]);
						}


					}
					else if(tokens.length == 2) {


						String	key = tokens[1];
						String	value = "null";

						if(key.length() > 20)
						{
							printError("Max Length of Key is 20 bytes");
						}
						else
						{
							kvClient.put(key,value);
						}


						//logger.info("Requested \t<"
						//+ "(" key + "," + null+ ")");
					}
					else {
						printError("Invalid number of parameters!");
					}
				} catch(Exception e) {
					printError("Unknown Error");
					logger.info("Unknown error", e);
				}
			}
			else
			{
				printError("Please connect to a server!");
			}
		}
		else if(tokens[0].equals("get"))
		{
			if(kvClient != null && kvClient.getClient() != null &&
				kvClient.getClient().isRunning()){

				try {
					if(tokens.length == 2)
					{
						String key = tokens[1];
						kvClient.get(key);
					}
					else
					{
						printError("Invalid number of parameters!");
					}
				}
				catch(Exception e) {
					printError("Unknown Error");
					logger.info("Unknown error", e);
				}
			}
			else
			{
				printError("Please connect to a server!");
			}
		}

		else if(tokens[0].equals("disconnect")) {
            if (kvClient != null)
    			kvClient.disconnect();
            else
                printError("Please connect to a server!");
            kvClient = null;

		} 
        
        else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT +
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}

		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t Inserts a key and value into the server \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t Retrieves the value associated with the key from the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT
				+ "Possible log levels are:");
		System.out.println(PROMPT
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {

		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);

			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
