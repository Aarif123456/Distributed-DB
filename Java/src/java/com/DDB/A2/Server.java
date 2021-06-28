package com.DDB.A2;/* Abdullah Arif
* COMP-4680
* Multi-threaded server capable of handling multiple client simultaneously
* implements all the basic key store operations for clients to use */

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {

    public static final Set<PrintStream> client = new HashSet<>();
    public static Gson gson; // Object used JSON conversion
    private static KeyValueStore<String, Object> kvs;  // Key Value store
    private static AtomicBoolean metaLock; // controls Access to key-value store

    public static void main(String[] args) {
        kvs = new KeyValueStore<>();
        metaLock = new AtomicBoolean(false);
        gson = new Gson();
        int socketNumber = 2000, maxClients = 25;
        try {
            // Open configuration file
            BufferedReader br = new BufferedReader(new FileReader("src/java/com/DDB/A2/Server.config"));
            socketNumber = Integer.parseInt(br.readLine().trim());
            maxClients = Integer.parseInt(br.readLine().trim());
        } catch (IOException e) {
            System.err.println(
                "Check to make sure Server.config is in directory and server has read permission");
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Make sure the Server.config file is in correct format");
            System.err.println("First Line holds socket number: default 2000");
            System.err.println("Second line holds the maximum number of clients: default 25");
            e.printStackTrace();
        }
        runServer(socketNumber, maxClients);
    }

    public static void runServer(int socketNumber, int maxClients) {
        IpFinder.findIP();
        try (ServerSocket hostSocket = new ServerSocket(socketNumber)) {
            // cap the amount of maximum client
            var pool = Executors.newFixedThreadPool(maxClients);
            //Stay on to lookout for more clients - essentially an infinite loop
            while (client.size() < maxClients + 1)
                pool.execute(new ClientHandler(hostSocket.accept()));
        } catch (IOException e) {
            System.out.println("Socket could not be created, check processes permissions");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Something went wrong :(");
            e.printStackTrace();
        }
    }

    // get object from JSON file
    public static HashMap<String, Object> deserializeJson(String jsonData) {
        Type collectionType = new TypeToken<HashMap<String, Object>>() {}.getType();
        return gson.fromJson(jsonData, collectionType);
    }

    public static String insert(String key, Object value) {
        while (metaLock.get()) ; // stay until lock is free
        metaLock.lazySet(true); // set lock
        String output;
        if (kvs.find(key)) // If key already exists then send an error
            output = "ERROR: " + key + " already exist as a key in the database cannot update";
        else {
            kvs.insert(key, value);
            output = "Successfully created Key-Value pair for " + key;
        }
        metaLock.lazySet(false); // free up lock
        return output;
    }

    public static String get(String key) { // Get Serialized version of value
        if (kvs.find(key)) // If key exists send appropriate value
            return kvs.get(key).toString();
        return "ERROR: The key " + key + ", does not exist in the database, cannot get";
    }

    public static String delete(String key) {
        if (!kvs.find(key))
            return "ERROR: The key " + key + ", does not exist in the database, cannot delete";
        while (metaLock.get()) ; // stay until lock is free
        metaLock.lazySet(true); // set lock
        String deletedValue = kvs.delete(key).toString();
        metaLock.lazySet(false); // free up lock
        return "The value of the deleted key " + key + " was " + deletedValue;
    }

    public static boolean find(String key) {
        return kvs.find(key);
    }

    public static String update(String key, Object value) {
        if (kvs.update(key, value))
            return "Successfully updated key " + key + "!";
        else
            return "Error: Key not found, server could not update :(";
    }

    public static String upSert(String key, Object value) {
        while (metaLock.get()) ; // stay until lock is free
        metaLock.lazySet(true); // set lock just in case we have to add key
        kvs.upSert(key, value);
        metaLock.lazySet(false); // free up lock
        return "Successfully upSerted " + key;
    }

    public static String clear() {
        if (count() == 0)
            return "Key-value store is already empty!";
        while (metaLock.get()) ; // stay until lock is free
        metaLock.lazySet(true); // set lock to take control of Key-Value store
        kvs.clear();
        metaLock.lazySet(false); // free up lock
        return "You cleared the Key-Value Store O.O";
    }

    public static int count() {
        return kvs.count();
    }

    private static class ClientHandler implements Runnable {

        private final Socket socket;
        private PrintStream p;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                System.out.println("New client joined " + socket);
                client.add(p = new PrintStream(socket.getOutputStream()));
                Scanner sc = new Scanner(socket.getInputStream());
                String input, command;
                StringBuilder payload;
                this.protocolPrint();
                while (sc.hasNextLine()) { // loop while client is active
                    command = sc.nextLine().toLowerCase();
                    payload = new StringBuilder(); // (re)set payload
                    while (sc.hasNextLine()) {
                        input = sc.nextLine();
                        if (input.trim().equals("END"))
                            break;
                        payload.append(input); // get payload
                    }
                    // run the clients command
                    this.runCommand(command, payload.toString());
                    p.println("END");
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) { // necessary because we don't want a crash on one client to break the program
                System.out.println("Error:" + socket);
                e.printStackTrace();
            } finally {
                if (p != null)
                    client.remove(p);
                try {socket.close();
                } catch (IOException e) { e.printStackTrace();}
                System.out.println("Closed: " + socket);
            }
        }

        private void protocolPrint() {
            p.println("Protocol:");
            p.println("command");
            p.println("number of values");
            p.println("payload");
            p.println("END");
        }

        public void runCommand(String command, String payload) {
            p.println("SERVER RETURN"); // let's client know that return is coming
            if (command.equals("insert") || command.equals("update") || command.equals("upsert")) {
                HashMap<String, Object> data = deserializeJson(payload);
                for (String key : data.keySet())
                    switch (command) {
                        case "insert":
                            p.println(insert(key, data.get(key)));
                            break;
                        case "update":
                            p.println(update(key, data.get(key)));
                            break;
                        default: // upsert
                            p.println(upSert(key, data.get(key)));
                    }
            }
            else if (command.equals("get") || command.equals("delete") || command.equals("find")) {
                String[] keys;
                keys = gson.fromJson(payload, String[].class);
                    for (String key : keys)
                        switch (command) {
                            case "get":
                                p.println(get(key));
                                break;
                            case "delete":
                                p.println(delete(key));
                                break;
                            //left value as boolean to make it easier for client to use
                            default: // for find
                                p.println(key);
                                p.println(find(key));
                        }
            } 
            else //commands with no parameter
                switch (command) {
                    case "clear":
                        p.println(clear());
                        for (PrintStream player : client) // let everyone know who cleared they store
                                player.println("Client " + socket + " cleared the key-value store");
                        break;
                    case "count":
                        p.println(count());
                        break;
                    default:
                        p.println(command + " is an invalid command");
                }
        }
    }
}
