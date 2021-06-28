/*  
*	Backup node class handle the server side part of the buddy system - It has access to the KVS network API
*	It handle 3 main types of incoming requests
*   1. Normal backup request - will make a new KVS API to handle as the users back up - will return the next in line
*   2. Handles request to give back old data 
*   3. Backing up a back-up once the original node dies
*	
*/


/* Report node will get backup from node which it will send to Peer node which will froward it to the KVS network API - which will send the backup request
*  When a node dies the KVS network API - will get the next in line (the current node's backup ) - which is stored as a static variable 
*/
package com.DDB.A3;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executors;

public class BackupNode implements Runnable{

    /* 
    *   In reality every node will have a max of 3 nodes open at a time
    *   right before it switches backup node 
    *   and for a little while and while it is getting back it's old data
    */
    private final int maxClients; 
    private int socketNumber;
    private HashMap<String,KVSNetworkAPI> backupMap;
    private HashMap<String,KVSNetworkAPI> archivalMap;
    private HashSet<PrintStream> backupWriter;
    public boolean retrieved;
    /* Object used JSON conversion */
    public final Gson gson;
    private Peer peer;

    public BackupNode(int socketNumber, Peer peer) {
        backupMap = new HashMap<>();
        archivalMap = new HashMap<>();
        backupWriter = new HashSet<>();
        maxClients = 30;
        this.socketNumber = socketNumber;
        gson = new Gson();
        retrieved = false;
        this.peer = peer;
    }

    public void checkReturner(String name, String ip, int port){
        if(archivalMap.containsKey(name) && archivalMap.get(name).count()>0){
            var data = archivalMap.get(name);
            // If have stuff reach out and give back it's stuff
            try (Socket backupSocket = new Socket(InetAddress.getByName(ip), port)) {
                if(backupSocket.getRemoteSocketAddress().equals(backupSocket.getLocalSocketAddress())){
                    System.err.println("WARNING: Attempted to connect to self - check for multiple running instances");
                }
                var newWriter = new PrintStream(backupSocket.getOutputStream());
                var newReader = new BufferedReader(new InputStreamReader(backupSocket.getInputStream()));
                
                newWriter.println("!nameless");
                do{
                    /* Tell target node how much items you have*/
                    newWriter.println(data.count());
                    /* Give the target it's old stuff back */
                    newWriter.println(data.getAll());
                } while(!newReader.readLine().trim().equals("SUCCESS"));
                newWriter.println("YOUR STUFF");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /* Return if the node has gotten the back-up node*/
    public boolean retrievedData(){
        return retrieved;
    }

    /* Main loop used get actively store the backup nodes */
    @Override
    public void run(){
        try (ServerSocket hostSocket = new ServerSocket(socketNumber)) {
            /* cap the amount of maximum backupMap */
            var pool = Executors.newFixedThreadPool(maxClients);
            /* Stay on to lookout for more clients - essentially an infinite loop */
            while (!Thread.interrupted())
                pool.execute(new Backups(hostSocket.accept()));
        } catch (IOException e) {
            System.out.println("Socket could not be created, check processes permissions");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Something went wrong :(");
            e.printStackTrace();
        }
    }

    /* inform all nodes about change in back up node so they can change their next in line  */
    public void informBackupChanged(String newBackupInfo) {
        for( var p: backupWriter){
            p.println("BACKUP CHANGE");
            p.println(newBackupInfo);
        }
    }

    /* If our back-up gave us a copy of a new backup then store it */
    public void addArchive(String name, HashMap<String, Object> data){
        if(!archivalMap.containsKey(name)){
            var tmpKVS = new KVSNetworkAPI();
            tmpKVS.addAll(data);
            archivalMap.put(name, tmpKVS);
        }
    }

    /* Once a node receives it's old back up - it will send a multi-cast to delete the old back up 
    *   this ensure the older copy of the data is no longer stored in the network */
    public void deleteArchive(String name){
        archivalMap.remove(name);
    }

    /* Class used for handling the active back-up in a new thread*/
    private class Backups implements Runnable {
        private final Socket socket;
        private PrintStream p;
        private BufferedReader r;
        private KVSNetworkAPI backupKVS;
        private String name="FAILED";
        Backups(Socket socket) {
            this.socket = socket;
            backupKVS = new KVSNetworkAPI();
        }

        @Override
        public void run() {
            try {
                p = new PrintStream(socket.getOutputStream());
                r = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                KVSCommandObject command;
                this.greetingExchange();

                /* loop while backupMap is active */
                do {
                    var json = r.readLine();
                    try{
                        command = gson.fromJson(json, KVSCommandObject.class);
                    }catch (NullPointerException e){
                       // e.printStackTrace();
                        break;
                    }

                }while(backupMap.containsKey(name) && this.runCommand(command.command, command.key, command.payload) );
            } catch (IOException ignored) {
                /* necessary because we don't want a crash on one backupMap to break the program */
            } catch (Exception e) {
                System.out.println("Error:" + socket);
                e.printStackTrace();
            } finally {
                backupWriter.remove(p);
                if(backupMap.containsKey(name)){
                    backupMap.remove(name);
                    if(!name.equals("FAILED")){
                        archivalMap.put(name,this.backupKVS);
                        /* Give back up to your back so it is at-least in two nodes*/
                        this.duplicateArchiveKVS(name, this.backupKVS);
                        // System.out.println("added archive");
                        System.out.println("Node " + name + " died");
                    }
                }
                try {socket.close();
                } catch (IOException e) { e.printStackTrace();}
                
            }
        }

        private void duplicateArchiveKVS(String name, KVSNetworkAPI archive){
            String archiveInfo = archive.toString();
            for( var p: backupWriter){
                p.println("ADD ARCHIVE");
                p.println(name);
                p.println(archiveInfo);
            }
        }
        private void greetingExchange() throws IOException {
            /* Ask for old stuff - if leader can find they will connect with the other back up and get the data*/
            var response = "FAIL";
            name = r.readLine();
            /* Add backup to map */ 
            backupMap.put(name, this.backupKVS);
            HashMap<String, Object> values = new HashMap<>();
            while(response.equals("FAIL")){
                /* Get count */
                int count = Integer.parseInt(r.readLine().trim());
                Type collectionType = new TypeToken<HashMap<String, Object>>(){}.getType();
                var json = r.readLine().trim();
                if(json.length()>0){
                    values= gson.fromJson(json, collectionType);
                }
               
                if(values.size() == count){
                    response = "SUCCESS";
                    backupKVS.addAll(values);
                }
                p.println(response);
            }
            var status = r.readLine().trim();
            switch (status){
                case "LEADER":
                 /* If, the node reaching out to you is the leader node get info for your */
                    var ip = r.readLine();
                    // System.out.println("ip"+ ip);
                    // System.out.println(port);
                    peer.setBackup(ip);
                    backupWriter.add(p);
                    break;
                case "YOUR STUFF":
                    /* Get rid of the backup because the nice node was only giving you your stuff back */
                    backupMap.remove(name);
                    peer.addOldData(values);
                    retrieved = true;
                    break;
                default: /* case "NORMAL": 
                    /* In default case the last back up node just died so give them your next back up node as their next in line*/
                    p.println(backupKVS.getBackupIP());
//                    p.println(backupKVS.getBackupPort());
                    /* Tell the number of stuff in your archive KVS then give it all to it*/
                    backupWriter.add(p);
                    archivalMap.forEach(this::duplicateArchiveKVS);
            }            
        }

        public boolean runCommand(String command, String key, String payload) {
            // System.out.println("GOT COMMAND "+ command);
            // System.out.println("key "+ key);
            // System.out.println("Payload "+ payload);
            if (command.equals("insert") || command.equals("update") || command.equals("upsert")) {
                switch (command) {
                    case "insert":
                        backupKVS.insert(key, payload);
                        break;
                    case "update":
                        backupKVS.update(key, payload);
                        break;
                    default: // upsert
                        backupKVS.upSert(key, payload);
                }
            }
            else if (command.equals("get") || command.equals("delete") || command.equals("find")) {
                switch (command) {
                    case "get":
                        backupKVS.get(key);
                        break;
                    case "delete":
                        backupKVS.delete(key);
                        break;
                    //left value as boolean to make it easier for backupMap to use
                    default: // for find
                        backupKVS.find(key);
                }
            } 
            else //commands with no parameter
                switch (command) {
                    case "clear":
                        backupKVS.clear();
                        break;
                    case "count":
                        backupKVS.count();
                        break;
                    case "get_all":
                        backupKVS.getAll();
                        break;
                    case "relieve": 
                    backupMap.remove(name);
                    default:
                        System.err.println("COMMAND:" + command +"EXITING LOOP");
                        return false;
                }

            return true;
        }

    }
     private static class KVSCommandObject{
        public String command = null;
        public String key = null;
        public String payload = null;

        KVSCommandObject(String command, String key, String payload){
            this.command = command;
            this.key = key;
            this.payload = payload;
        }

        KVSCommandObject(String command, String key){
            this(command, key, null);
        }

        KVSCommandObject(String command){
            this(command, null, null);
        }
    }
}