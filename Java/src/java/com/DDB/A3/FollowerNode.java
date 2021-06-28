/* Abdullah Arif
* COMP-4680
* Program to handle the one on one FollowerNodes that leader nodes have 
* implements all the basic key store operations for followers to use */
package com.DDB.A3;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class FollowerNode implements Runnable {
    private Socket followerSocket = null;
    private BufferedReader reader = null;
    private PrintStream writer = null;
    private int missedBeats = 0;
    private final AtomicBoolean kill;
    private final CountDownLatch checkBeat;
    private CountDownLatch becomeCandidate;
    private CountDownLatch becomeLeader;
    private String ip; 
    private static final long SLEEP_TIMER = 5000;
    public static int socketNumber;
    public static final int MISSED_BEATS_THRESHOLD = 3;
    private static FollowerNode candidate;
    private String nodesToTransfer;
    private static Peer peer;

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Constructor dependent if used by candidate or leader */
    /* Constructor used by leader*/
    public FollowerNode(Socket s) throws IOException {
        this(s.getInetAddress().getHostAddress());
        followerSocket = s;
        this.initializeConnection();
    }

    /* Constructor used by candidate*/
    public FollowerNode(String ip){
        this.ip = ip;
        checkBeat = new CountDownLatch(1);
        kill = new AtomicBoolean(false);
        this.becomeCandidate = new CountDownLatch(0);
        this.becomeLeader = new CountDownLatch(0);
    }

    /* A one time initialization done at the start of the program*/
    public static void staticInitialize(Peer p, int socketNumber){
        peer = p;
        FollowerNode.socketNumber = socketNumber;
    }

    /* Helper function to get connection started */
    public synchronized void initializeConnection() throws IOException {
        if (writer != null)
            return;
        if(followerSocket == null){
            // System.out.println("FOLLOWER NODE IS TRYING TO INITIALIZE CONNECTION");
            followerSocket = new Socket();
            InetAddress address = InetAddress.getByName(ip);
            // 30 seconds wait to accept
            followerSocket.connect(new InetSocketAddress(address, socketNumber), 30000 );
        }
        followerSocket.setSoTimeout(20000);
        writer = new PrintStream(followerSocket.getOutputStream());
        reader = new BufferedReader(new InputStreamReader(followerSocket.getInputStream()));
    }
   
    

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Main loop for nodes */
    @Override
    public void run() {
        try {
            missedBeats = 0;
            // System.out.println("Follower with " + ip + " running");
            while (missedBeats <= MISSED_BEATS_THRESHOLD && !kill.get()) {
                /* if don't have to check the beat we just wait */
                /*System.out.println("Not my larva " + checkBeat.getCount());*/
                checkBeat.await();
                /*System.out.println("This is my heart beat song");*/
                if (followerSocket == null)
                    this.initializeConnection();
                this.checkAlive();
                if (becomeCandidate.getCount() == 1)
                    this.electCandidate();
                if (becomeLeader.getCount() == 1)
                    this.electLeader();
                if (candidate == this)  // if current node is the candidate it has to be update its follower list
                    this.handleCandidateMessages();
            }
            if(kill.get())
                writer.println("BYE");

        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Interrupted follower node: " + ip + "shutting down");
        } catch(IOException e){
            e.printStackTrace();
            System.err.println("ERROR: Cannot connect with client. Deleting follower:(");
        } finally { // On end close streams and delete from set
            becomeCandidate.countDown();
            becomeLeader.countDown();
            this.close();
        }
    }

    private void handleCandidateMessages() {
        // System.out.println("talking to candidate");
        String addQueue = peer.getAddQueue();
        /* if nothing to add then tell candidate to no wait for add list */
        if (addQueue.equals("")) {
            writer.println("NO ADD");
        } else {
            writer.println("ADD"); // tell candidate they are about to get list of followers to add
            writer.println(addQueue);
        }
        String deleteQueue = peer.getDeleteQueue();
        if (deleteQueue.equals("")) {
            writer.println("NO DELETE");
        } else {
            writer.println("DELETE");
            writer.println(deleteQueue);
        }
    }

    private void electLeader() {
        String response;
        /* If we have node getting transferred that means we are electing a new leader*/
        writer.println("LEADER");
        response = this.getResponse(); /* Get the response from the node */
        if(response.equals("YES")){
            writer.println(nodesToTransfer);
            nodesToTransfer = null;
        }
        else{
            System.err.println("Warning: node " + ip + " refused election to leader" );
        }
        becomeLeader.countDown();

    }

    private void electCandidate() {
        String response;
        // ** handle make candidate send follower list **
        writer.println("CANDIDATE");
        // System.out.println("Will you be my candidate? "+ ip);
        response = this.getResponse();
        if(response.equals("YES")){
            peer.setCandidate(this);
            candidate = this;
        }
        else{
            System.err.println("Warning: node " + ip + " refused to become candidate" );
        }
        becomeCandidate.countDown();

    }

    private void checkAlive() throws InterruptedException {
        String response;
        // System.out.println("Checking up on my larva");
        writer.println("ALIVE?"); // Send prompt
        Thread.sleep(SLEEP_TIMER);
        response = this.getResponse();
        // case "MISSED":
        if ("ALIVE".equals(response)) { // If node alive clear the count of the times beat was missed
            this.clearMissedBeat();
        } else {
            this.incrementMissedBeat();
        }
    }

    /* Handle closing of node */
    private synchronized void close(){
        // System.out.println("Closing connection on follower with ip-address of " + ip); 
        try{
            if(followerSocket != null && followerSocket.isClosed())
                followerSocket.close();
             if(reader != null)
                reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writer.close();
        peer.removeNode(this); // Tell main function to consider this dead
    }

    /* Handle getting the response from the follower node */
    public String getResponse() {
        try {
            String response;
            if ((response = reader.readLine()) != null) {
                /*System.out.println("FOLLOWER NODE RECEIVED: "+ response);*/
                return response;
            } // This should be triggered because the socket set with a time out
        }catch (SocketTimeoutException ignored) {
//            System.out.println("Missed beat from follower " + this.getIp());
        } catch (IOException e) {
//            e.printStackTrace();
           kill.lazySet(true);
        }
        return "MISSED";
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Handle state if node by setting certain flags */
    // add one to the amount of beats missed
    public void incrementMissedBeat() {
        missedBeats++;
    }

    // reset the missed beat
    public void clearMissedBeat() {
        missedBeats = 0;
    }

    /* on next cycle it run protocol to transition node into the candidate node */
    public void initiateCandidateElection(){
        becomeCandidate = new CountDownLatch(1);
    }

    /* Used by the candidate manager in peer */
    public synchronized boolean isCandidate(){
        try {
            becomeCandidate.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return this == candidate;
    }

    /* on next cycle it run protocol to transition node into a new leader */
    public void initiateLeaderElection(String nodesToTransfer){
        this.nodesToTransfer = nodesToTransfer;
        becomeLeader = new CountDownLatch(1);
    }

    /* Used by the peer node to determine if leader accepted election */
    public synchronized boolean isLeader(){
        try {
            becomeLeader.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return nodesToTransfer == null;
    }

    /* set if this is an active node */
    public void setCheckBeat(boolean b) {
        // if we transitioning from leader to candidate or vice-versa we don't want to count old missed beats
        if (b != (checkBeat.getCount()==0))
            this.clearMissedBeat();
        /*System.out.println("My old count " + checkBeat.getCount());*/
        checkBeat.countDown();
        /*System.out.println("My new count " + checkBeat.getCount());*/
    }

    public void killNode(){
        kill.lazySet(true);
    }
    public Socket getSocket(){
        return followerSocket;
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Basic function used by hash set */
    public String getIp() {
        if (this.ip == null){
            this.ip = followerSocket.getInetAddress().getHostAddress();
        }
        return this.ip;
    }

    @Override
    public int hashCode() { 
        return this.getIp().hashCode();
    }

    @Override
    public boolean equals(Object other) { 
        if (other == null)
            return false;
        if (this.getClass() != other.getClass())
            return false;
        FollowerNode f = (FollowerNode) (other);
        return f.getIp().equals(this.getIp());
    }
}
