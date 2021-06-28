// Abdullah Arif
// Class representing different "multi-cast channels" which will handle various things
package com.DDB.A3;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.Timestamp;

// imports
public class GroupChat implements Runnable {
    protected final MulticastSocket socket;
    protected final InetAddress group;
    private static final Gson gson =  new Gson(); // Object used JSON conversion
    private final Peer peer;
    private String name;
    private final DatagramSocket writeSocket;
    public static final int UNNAMED_MEMBER =-1, FOLLOWER_LEVEL=0,  LEADER_LEVEL=2; // CANDIDATE_LEVEL=1,
    private final long initialTime;
    private final AtomicInteger level;

    /* Atomic boolean to control privileges 
    leader level: allows nodes to take in nodes - only used by leaders
    leaderList: allows node to update list of leaders - used by leader and candidates ** unimplemented as the group chat works **
    nameChat: ensures that the name used by a node is unique - used by all nodes
    */
    public GroupChat(String multicastAddress, int multicastPort, Peer peer) throws IOException {
        this.peer = peer;
        socket = new MulticastSocket(multicastPort);
        group = InetAddress.getByName(multicastAddress);
        if(!group.isMulticastAddress()){
           throw new IOException("ERROR: the set IP address does not support multi-casting"); 
        }
        writeSocket = new DatagramSocket();
        socket.joinGroup(group);
        level = new AtomicInteger(UNNAMED_MEMBER);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        initialTime = timestamp.getTime();
//        leaderList = new HashSet<>();
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Handle state of Group-chat */
    // return true if function currently does name have an accepted name
//    public boolean nameless(){
//        return this.name == null || this.name.equals("");
//    }

    // Set group chat level to follower
    public void setFollowerLevel(){
        level.lazySet(FOLLOWER_LEVEL);
    } 

    // Set group chat level to Leader --> Removed candidate because it does not matter to group chat
    public void setLeaderLevel(){
        level.lazySet(LEADER_LEVEL);
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Main loop - used to handle the various multi-cast channel*/
    public void run() {
        byte[] buf; // array where info is received
        DatagramPacket packet; // create container to receive packet
        String received;
        try {
            while (!Thread.interrupted()) {
                buf = new byte[256];
                packet = new DatagramPacket(buf, buf.length); 
                try {
                    socket.receive(packet);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                received = new String( packet.getData(), 0, packet.getLength() );
                processData(received);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(group != null){
                try {
                    socket.leaveGroup(group);
                    writeSocket.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } 
    }

    /* Used to internally process data received through the channels*/ 
    private synchronized void processData(String json) throws IOException {
        // deserialize message from JSON
        Command message = gson.fromJson(json, Command.class);
        String command = message.command;
        
        switch(level.get()){
            case LEADER_LEVEL: 
                // this.updateLeaderList();
                if (command.equals("GREETINGS")) {// need name to add and payload is IP address
                    // assuming name hasn't been taken - if it has it might mess up the kvs calls
                    acceptNode(gson.fromJson(message.payload, String[].class));
                }
            // INTENTIONAL FALL THROUGH HERE! 
            case FOLLOWER_LEVEL: // Lastly, check if message can be handled at follower level
                if (command.equals("RETURNER")){
                    String[] ips = gson.fromJson(message.payload, String[].class);
                    if(ips.length >0){
                        peer.checkReturner(message.name, ips[0]);
                    }
                    else {
                        System.err.println("ERROR: FOUND NO IP ADDRESSES IN RETURNER");
                    }
                }

                if (command.equals("DELETE ARCHIVE")){
                    peer.deleteArchive(message.name);
                }
            default: // INTENTIONAL FALL THROUGH HERE! 
                if (command.equals("DENY NAME"))
                    if(message.name.equals(name) && level.get() == UNNAMED_MEMBER)
                        name = "";
                if (command.equals("NAME") ){ //checkName
                    //  name is unique - so if someone tries taking it they must have a different time stamp
                    // Could potentially also check the IP - in case by some coincidence - two nodes made the same name with the same name
                    if (message.name.equals(name) && initialTime != message.timeSent )
                        sendCommand("DENY NAME");
                }
        }
    }

    /* Using the list of ip addresses from the greeting */
    private synchronized void acceptNode(String[] ips) {
        // System.out.println("Received the following ips " + ips);
        for(String ip : ips){
            if(peer.containsFollower(ip)){
                System.out.println("Already have follower disregarding call ");
                break;
            }
            Socket s = new Socket();
            try {
                var address =  new InetSocketAddress(InetAddress.getByName(ip), FollowerNode.socketNumber);
                // System.out.println("Trying address " + address);
                /* 30 seconds wait to accept */
                s.connect(address,30000);
                if(s.getRemoteSocketAddress().equals(s.getLocalSocketAddress())){
                    System.err.println("WARNING: Attempted to connect to self - check for multiple running instances");
                    break;
                }
                if(s.isConnected()) {
                    peer.addNode(s); // if successful then add node
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Handle sending various remote commands to group chat */
    /* Send network command */
    public void sendCommand(Command command) throws IOException {
        String message = gson.toJson(command);
        // System.out.println("NODE " + name + " send out " + message);
        // send command to group chat
        byte[] buf = message.getBytes(); // Send the serialized command
        DatagramPacket packet = new DatagramPacket(buf, buf.length, group, 80);
        writeSocket.send(packet);
    }

    /* Send remote commands using a payload */
    public void sendCommand(String commandName, Object payload) throws IOException{
        Command command = new Command(commandName, name, gson.toJson(payload));
        sendCommand(command);
    }

    /* Send remote commands with no arguments */
    public void sendCommand(String commandName) throws IOException{
        Command command = new Command(commandName, name);
        sendCommand(command);
    }

    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Specific commands used for joining*/
    /* Ask if name can be used */
    public boolean checkName(String name) throws IOException {
        Command command = new Command("NAME", name, null, initialTime);
        sendCommand(command);
        this.name = name;
        // Wait for someone to deny you a name
        try {
            Thread.sleep(15000); // wait 20 seconds for a reply ** May change to 30 depending on the other timers **
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // If name was reset it means it was denied
        return !this.name.equals("");
        // if name is unique return true and set privilege level
    }
 
    // creates join message depending on the IP
    public Command getJoinMessage(Object[] ipSet){
        String message = gson.toJson(ipSet);
        return new Command("GREETINGS", name, message, initialTime);
    }
    public Command getReturnMessage(Object[] ipSet){
        String message = gson.toJson(ipSet);
        return new Command("RETURNER", name, message);
    }
//    public void addLeader(String leaderIPs ){
//        String[] ipArray= gson.fromJson(leaderIPs, String[].class);
//        leaderList.addAll(Arrays.asList(ipArray));
//    }
//
//    public void removeLeader(String leaderIPs) {
//        String[] ipArray= gson.fromJson(leaderIPs, String[].class);
//        leaderList.removeAll(Arrays.asList(ipArray));
//    }


    // * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * //
    /* Class used to quickly serialize commands before sending */
    public static class Command{
        public final String command;
        public final String name;
        public final String payload;
        public final long timeSent;
        Command(String command, String name, String payload, long ts){
            this.command = command;
            this.name = name;
            this.payload = payload;
            timeSent = ts;
        }
        Command(String command, String name, String payload){
            this(command, name, payload, (new Timestamp(System.currentTimeMillis())).getTime());
        }

        Command(String command, String name){
            this(command, name, null);
        }
    }
}

