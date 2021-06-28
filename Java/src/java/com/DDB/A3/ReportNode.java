// Abdullah Arif
// a runnable object that will be used to find and report to a node
package com.DDB.A3;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class ReportNode implements Runnable {
	private static final int TIMEOUT = 10000, MISSED_THRESHOLD=3;
	private static final long MOURNING_PERIOD = TIMEOUT *2; // how long it node wait after leader is presumed dead
	private PrintStream reportWriter;
	private BufferedReader reportReader;
	private final Peer peer;
	private Socket reportSocket;
	private final AtomicBoolean leader;
	private final ReentrantLock socketWorks =  new ReentrantLock();
	final Condition socketFree = socketWorks.newCondition();
	private int socketNumber ;
	public ReportNode(Peer p, int socketNumber) {
		reportWriter = null;
		reportSocket = new Socket();
		leader = new AtomicBoolean(false);
		this.socketNumber = socketNumber;
		peer = p;
	}

	// Used to find and report to new report Node
	public void run() {
		boolean candidate = false;
		int countMissedBeats = 0 ; // Used to count the times leader failed to response to us
		String response; //hold response from leader

		while (!Thread.interrupted()) {
			// If we have a new report node then we have to wait for socket to be updated
			while(!reportSocket.isConnected() || reportSocket.isClosed()){
				synchronized(socketWorks) {
					socketWorks.lock(); /* remove all threads waiting for node */
					Thread t = new Thread(this::findReportNodes);
					if( !(peer.isLeader()) && !candidate) {
						t.start();
					}
					try {
						/* Candidate and socket will just wait for a new candidate to be made*/
						socketFree.await();
						t.interrupt();
						t.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			// System.out.println("In report node main loop");
			
			/* if an active candidate */
			if(candidate && !leader.get()){
				// System.out.println("Waiting on that candidate response ");
				response = this.getResponse();
				if(response.equals("ADD")){
					/* get follower's IP and add follower */
					response = this.getResponse(); 
					peer.addAllNodes(response);
				}
				response = this.getResponse();
				if(response.equals("DELETE")){
					response = this.getResponse(); /* get follower IP */
					peer.removeAllNodes(response); /* delete follower */
				}

			}
			// Handle response
			response = this.getResponse();
			if (response.equals("BYE") || countMissedBeats > MISSED_THRESHOLD) {
				closeConnection();
				if(candidate){
					// Start leader transition after mourning period
					try{
						Thread.sleep(MOURNING_PERIOD);
					} catch (InterruptedException e) {
		                e.printStackTrace();
		            }
		            // System.out.println("BECAME LEADER");// become the leader
					peer.setLeaderMode();
					leader.lazySet(true); 
				}

				/* if a leader and your report node died that means you no longer have a candidate
				 and if you just became a leader then you are no longer a candidate */
				if(leader.get()){ 
					peer.candidateDied();
					candidate = false; 
				}
			}
			else if (response.equals("LEADER")){
				if(candidate){
					reportWriter.println("NO");
				}
				else{
					/* you are now set to be a leader on next bye you will eave and become leader */
					leader.lazySet(true); 
					reportWriter.println("YES");
					response = this.getResponse();
					peer.addAllNodes(response);
					/* as a candidate the leader will go through a mourning period and become a leader */
					candidate = true; 
				}
			} 
			else if (response.equals("CANDIDATE")){
//				System.out.println("Received candidate request");
				candidate = true;
				// System.out.println("Became a candidate");
				reportWriter.println("YES");
			}
			else if (response.equals("ALIVE?")){
				reportWriter.println("ALIVE");
			}
			else if(response.equals("MISSED")){
				countMissedBeats++;
			}
			else{
				System.err.println("ERROR: Report node does not know how to handle the message \"" + response +"\"");
			}
		}
	}

	private String getResponse(){
		String response = "BYE";
		try{
			response = reportReader.readLine().trim();
			// System.out.println("REPORT NODE RECEIVED: " + response);
		} catch(SocketTimeoutException e){
			// e.printStackTrace();
			response = "MISSED";
		} catch (IOException e) {
			// e.printStackTrace();
		}
		return response;
	}
	// functions returns true if node has an active node that it reports to
	public boolean foundNode() {
		return reportSocket.isConnected();
	}

	public synchronized void setCandidate(Socket s){
		// System.out.println("MADE A NEW CANDIDATE");
		socketWorks.lock();
		reportSocket = s;
		socketFree.signalAll();
	}

	public void setLeader(){
		leader.lazySet(true);
	}
	// Close the threads safely
	public void close() {
		Thread.currentThread().interrupt();
	}

	// Find or update the node that the current node will send the "heartbeat" signal
	// If for someone reason things get way out of sync even with the built in wait - then allow manual SWITCH
	private synchronized void findReportNodes() {
//		System.out.println("Searching for node");
		// while (!Thread.interrupted()) {
		// default 80
		
		try (ServerSocket hostSocket = new ServerSocket(socketNumber)) {
			socketWorks.lock();
			hostSocket.setSoTimeout(TIMEOUT); // IO calls should time out after 10 seconds
			Socket potentialReport = null;
			while (potentialReport == null && !Thread.interrupted() && !reportSocket.isConnected() && !peer.isLeader()) {
//				System.out.println("Searching for a node");
				try {
					potentialReport = hostSocket.accept();
					// System.out.println("socket connected " + potentialReport.getRemoteSocketAddress());
					potentialReport.setSoTimeout(TIMEOUT);

				} catch(SocketTimeoutException e) {
					potentialReport = null;
				}
			}
			
			if(potentialReport != null){
				// System.out.println("potential report is closed? " + potentialReport.isClosed());
				reportSocket = potentialReport; // replace the socket
				reportWriter = new PrintStream(reportSocket.getOutputStream());
				reportReader = new BufferedReader(new InputStreamReader(reportSocket.getInputStream()));
			}
			
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
		}  catch(SocketException e) {
			// e.printStackTrace();
			// System.err.println("Will remove this in a bit");
		} catch (IOException e) {
			System.err.println("ERROR: Socket could not be created, check processes permissions");
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			System.out.println("Closed report node scanner");
		}
		finally{
			socketFree.signalAll();
			socketWorks.unlock();
		}
	}

	private void closeConnection() {
		try {
			reportSocket.close();
			reportWriter.close();
			reportReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		reportSocket = new Socket();
	}
}

