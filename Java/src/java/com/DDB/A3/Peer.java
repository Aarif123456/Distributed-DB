package com.DDB.A3;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;


/* Level is used to hold the current level of the node */
enum Level {
  UNNAMED_MEMBER,
  FOLLOWER_LEVEL,
  CANDIDATE_LEVEL,
  LEADER_LEVEL
}
public class Peer implements Runnable{
    /* The name of the node **  might change to a 64 bit string ** */
    public String name;

    /* Enum variable used to track state of the node*/
    public Level level; 

    /* Key Value store - used top store data */
    private final KVSNetworkAPI kvs;

    /*  Report node is used to send keep alive signal 
    *   For candidate nodes it is also where they receive a list of followers to update their list
    */
    public final ReportNode reportNode; // send keep alive signal to this node
    /* Group chat handles the multi-cast functionality of the program 
    *   it will be used to accept nodes by leaders
    *   it will also be used to ensure uniqueness of name 
    */
    public final GroupChat gc;

    /* Used to ensure data isn't loss from KVS on node failure
    *  For the leader node, the backup node is always being changed to the newest node added
    */
    public BackupNode backupNode;

    /*  list of nodes that node is keeping track of - it will be empty for regular nodes
    *   For candidate nodes the nodes won't be active
    */
    public final LinkedHashSet<FollowerNode> followers;
    
    /* Object used JSON conversion */
    public final Gson gson;

    /* The number of maximum follower a node can have */
    public final int MAX_FOLLOWERS = 3; // ** TESTING NORMALLY 25 **

    /* node refers to the candidate node */
    public FollowerNode candidate;

    /* Used to keep track of updates to the list */
    private final Set<String> addQueue;
    private final Set<String> deleteQueue;
    private Config config;

    private static final String ALPHA_NUMERIC_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

    
    public static void main(String[] args) throws IOException{
        Peer p = new Peer();
        p.run();
    }

    public Peer() throws  IOException{
        gson = new Gson();
        StringBuilder configJson = new StringBuilder();
        String message;
        BufferedReader br = new BufferedReader(new FileReader("src/java/com/DDB/A3/Peer.config.json"));
        while ((message = br.readLine())!= null){
            configJson.append(message).append(" ");
        }
        config = gson.fromJson(configJson.toString(), Config.class);;
        reportNode = new ReportNode(this,config.targetFollowerSocket);
        kvs = new KVSNetworkAPI();
        followers = new LinkedHashSet<>();
        addQueue = new HashSet<>();
        deleteQueue = new HashSet<>();
        FollowerNode.staticInitialize(this,config.receiveFollowerSocket);
        KVSNetworkAPI.staticInitialize(this, config.backupTargetPort);
        gc = new GroupChat(config.groupChatIP, config.groupChatPort, this);
        backupNode = new BackupNode(config.backupAcceptPort, this);
        level = Level.UNNAMED_MEMBER; // start of as unnamed member
    }
    
    /* start all the needed thread then ** start the local KVS manipulation program ** */
    public void run(){
        // Start listening at its port for a node to become leader and if found then start reporting
        new Thread(reportNode).start(); //
        new Thread(gc).start(); // start listening to appropriate group chats
        new Thread(backupNode).start();
        try {
            name = config.oldName;
            System.out.println("Creating name");
            while(name.equals("!nameless") || !gc.checkName(name))
                name = generateName(config.nameLength);
            System.out.println("You are " + name);
            // ** SAVE Name to config file **
            gc.setFollowerLevel();
            joinNetwork(); // node will try to join an existing network
            if(!reportNode.foundNode()){
                this.setLeaderMode();
                System.out.println("Became a leader");
            }
            retrieveOldData();
            kvs.run(); // run the local loop
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            reportNode.close();
//            gc.close() // Add a kill thing to all the code to stop properly
        }

        // ** new thread allow for manipulation of KVS store locally like old client program **

        // Exit stuff --> put in finally
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Handle node state */
    /* Become the leader node - let group chat, report node and follower nodes know*/
    public void setLeaderMode(){
        /* Start checking follower health */
        for(FollowerNode f : followers)
            f.setCheckBeat(true);

        /* start candidate managing thread */
        new Thread(this::manageCandidateNodes).start();

        /* Tell group-chat we have become a leader so that it can start looking for new nodes */
        gc.setLeaderLevel();

        /* Make the report node the leader */
        reportNode.setLeader();

        level = Level.LEADER_LEVEL;
    }

    public boolean isLeader(){
        return (level == Level.LEADER_LEVEL);
    }

    /* Make the name of the node */
    public String generateName(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) 
            builder.append(ALPHA_NUMERIC_STRING.charAt((int)(Math.random()*ALPHA_NUMERIC_STRING.length())));
        return builder.toString();
    }

    public String getName(){
        return name;
    }
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Handle follower Node interaction with peer node */
    // public boolean needCandidate(){
    //     return (level.get() == LEADER_LEVEL) && !(reportNode.foundNode());
    // }
    /* Return if node is already added as active follower */
    public boolean containsFollower(String ip){
        return followers.contains(new FollowerNode(ip));
    }

    // remove a node considered dead - self-reported by dead node 
    public synchronized void removeNode(FollowerNode f){
        if(f==null) {
            System.err.println("ERROR: Tried deleting null follower node");
        }
        else{
            followers.remove(f);
            /* Send update to follower if leader*/
            if(this.isLeader() && reportNode.foundNode()){
                deleteQueue.add(f.getIp());
            }
        }
    }

    /* used by candidate - when leader tells it that a node is dead */
    public void removeNode(String ip){
        this.removeNode(new FollowerNode(ip));
    }

    public void removeAllNodes(String json){
        String[] followerNodes = gson.fromJson(json, String[].class);
        for(String f : followerNodes){
            this.removeNode(f);
        }
    }

    /* Add function from the message received in multi-cast channel
    * The reason it is synchronized is if we are waiting for multiple followers
    * and we don't want to ask them all to be our candidate - so, we block 
    */
    public synchronized void addNode(Socket reportSocket) throws IOException {
        FollowerNode f = new FollowerNode(reportSocket);
        new Thread(f).start();
        followers.add(f);
        if(this.isLeader() ){
            f.setCheckBeat(true);
            kvs.setBackup(reportSocket.getInetAddress().getHostAddress(), false);
            // make node backup
        }
        /* Send update to follower */
        if(this.isLeader() && reportNode.foundNode()){
            addQueue.add(reportSocket.getInetAddress().getHostAddress());
        }
        if(followers.size()>=MAX_FOLLOWERS){
           this.delegateToNewLeader();
        }
    }

    public void addNode(String ip){
        FollowerNode f = new FollowerNode(ip);
        new Thread(f).start();
        followers.add(f);
        if(followers.size() == 1){
            f.setCheckBeat(true);
            // And make leader your backup node
        }
    }

    public void addAllNodes(String json){
        String[] followerNodes = gson.fromJson(json, String[].class);
        for(String f : followerNodes){
            this.addNode(f);
        }
    }

    /* used to create a new leader when the node is full */
    private synchronized void delegateToNewLeader(){
        Iterator<FollowerNode> iterator = followers.iterator();
        ArrayDeque<FollowerNode> removeNodes = new ArrayDeque<>();
        ArrayDeque<String> ipAddressToRemove = new ArrayDeque<>();
        FollowerNode removeNode;
        int numElementsRemoved = followers.size()/2; // remove half the elements
        while(numElementsRemoved >0 && iterator.hasNext()){
            removeNode = iterator.next();
            if(removeNode == candidate){ // don't want to remove candidate
                continue;
            }
            removeNodes.add(removeNode);
            ipAddressToRemove.add(removeNode.getIp());
            numElementsRemoved--;
        }
        for(FollowerNode newLeader: removeNodes){
            /* Don't send the new leader their own IP */
            ipAddressToRemove.remove(newLeader.getIp());
            newLeader.initiateLeaderElection(gson.toJson(ipAddressToRemove));
            if(newLeader.isLeader()) // if node became leader then we are done
                break;
            ipAddressToRemove.add(newLeader.getIp());
        }

        /* Send good bye */
        for(FollowerNode transferNode: removeNodes){
            transferNode.killNode();
        }       
    }

    
    public String getAddQueue(){
        /* if added got deleted then don't force node to add it then delete it */
        addQueue.removeAll(deleteQueue);
        String response = (addQueue.isEmpty()) ? "" : gson.toJson(addQueue);
        /* clear queue after processing */
        addQueue.clear(); 
        return response;
    }

    public String getDeleteQueue(){
        String response = (deleteQueue.isEmpty()) ? "" : gson.toJson(deleteQueue);
        /* clear queue after processing */
        deleteQueue.clear(); 
        return response;
    }

    /* start thread to look for a possible candidate 
    *  triggered when node becomes leader or by report node when candidate node dies
    */
    public void findCandidate(){
        new Thread(this::manageCandidateNodes).start();
    }
    
    public void setCandidate(FollowerNode f){
        reportNode.setCandidate(f.getSocket());
        candidate = f;
        /* Create list of followers to send to follower */
        for(FollowerNode f2 : followers){ 
            if(f == f2){ // don't add follower to own list
                continue;
            }
            addQueue.add(f2.getIp());
        }
    }

    public void candidateDied(){
        candidate = null;
        this.findCandidate();
    }

    /* function to find and replacing candidate nodes -> It is run by leader node upon becoming a leader */
    private void manageCandidateNodes (){
        while(!reportNode.foundNode()){

            while(!reportNode.foundNode()) {
                try{
                    for (FollowerNode f : followers) {
                        f.initiateCandidateElection();
                        if (f.isCandidate()) {
                            break;
                        }
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }

            }

        } 
    }

    public boolean needBackup(){
        return kvs.needBackup();
    }
    public void setBackup(String ip){
        kvs.setBackup(ip, false);
    }

    public void changedBackup(String newBackupInfo){
        backupNode.informBackupChanged(newBackupInfo);
    }
    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Handle group chat interaction */
    /* Get the KVS store used by node - used by group chat to allow remote manipulation */
    public KVSNetworkAPI getKVSAPI(){
        return kvs;
    }

//    /* Used by candidate node to stay updated with leader list leader by removing leader who have failed */
//    public void removeAllLeaderNodes(String leaderIPs) {
//        gc.addLeader(leaderIPs);
//    }
//
//    /* Used by candidate node to stay updated with leader list leader by adding in nodes who became leaders*/
//    public void addAllLeaderNodes(String leaderIPs) {
//         gc.removeLeader(leaderIPs);
//    }

    /* function to join -  send message to join - node with fewest follower will become the leader of the new node
    *  For founder node - the first follower will become the report node and candidate node
    *  Returns true if it joined a network
    */
    public void joinNetwork() throws IOException {
        var ipSet = IpFinder.findIP();
        GroupChat.Command message = gc.getJoinMessage(ipSet); // gets serialized version of join message
        int tries=config.joinTries; // ## get from setting ##
        System.out.println("Attempting to join network please wait...");
        for(int i=1; i<=tries; i++){
            gc.sendCommand(message);
            try {
                if (reportNode.foundNode()) // Don't want to go to sleep if we have already found a node
                    throw new InterruptedException();
                Thread.sleep(5000 * i); // slowly backs off - to give nodes time to process things as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Assumed network was joined");
                level = Level.FOLLOWER_LEVEL;
                break;
            }
        }
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Handle KVS network API interaction */
    /* Used by leader node to switch backup node */
    // private void leaderMakeNewBackupNode(String backupIP){
    //     // leader will alternate between two ports used to make a connection as a backup node - two avoid collision
    //     // Connect to new node as backup node
    //     // give it all the info in it's KVS
    //     // clear the KVS in the old backup node
    //     // Disconnect the old backup (since you are disconnecting the backup node won't react)
    //     // Ask new backup node to make old backup node new backup node
    // }

    /*Used to add all the old data back to KVS */
    public void addOldData(HashMap<String, Object> values) throws IOException{
        kvs.addAll(values);
        gc.sendCommand("DELETE ARCHIVE");
    }
    /* Used by returning node to get back old data */ 
    private void retrieveOldData() throws IOException {
        var ipSet = IpFinder.findIP();
        GroupChat.Command message = gc.getReturnMessage(ipSet); // gets serialized version of join message
        int tries=config.retrieveTries;
        System.out.println("Attempt to retrieve old data...");
        for(int i=1; i<=tries; i++){
            gc.sendCommand(message);
            try {
                if (backupNode.retrievedData()) { // Don't want to go to sleep if we have already found a node
                    throw new InterruptedException();
                }
                Thread.sleep(5000L * i); // slowly backs off - to give nodes time to process things as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Got backup data");
                break;
            }
        }
    }

    /* Every node will their back up if they have a nodes old stuff */
    public void checkReturner( String name, String ip){
        /* if we have a kvs in our name send out connect to returner*/
        backupNode.checkReturner(name, ip , config.backupTargetPort);
    }

    public void deleteArchive(String name){
        backupNode.deleteArchive(name);
    }
    /* Add archive to backup node */
    public void addArchive(String name, HashMap<String, Object> data){
        backupNode.addArchive(name, data);
    }   
    private static class Config{
        public int receiveFollowerSocket = 2000;
        public int targetFollowerSocket = 2000;
        public int groupChatPort = 80;
        public String groupChatIP = "224.0.0.1";
        public int backupAcceptPort = 100;
        public int backupTargetPort = 100;
        public int joinTries = 2;
        public int retrieveTries = 3;
        public String oldName = "!nameless";
        public int nameLength = 64;
    }
}
