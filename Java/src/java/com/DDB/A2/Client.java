package com.DDB.A2;/* Abdullah Arif
* COMP-4680
* Client class that will connect to the server 
* implements all the basic key store operations */

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Client {

    public static Gson gson; // object for JSON conversion
    private static Scanner sc;

    public static void main(String[] args) {
        gson = new GsonBuilder().create();
        sc = new Scanner(System.in);
        String serverIP = "127.0.1.1"; //default loop back
        int socketNumber = 2000;
        try {
            // Open configuration file
            BufferedReader br = new BufferedReader(new FileReader("src/java/com/DDB/A2/Client.config"));
            serverIP = br.readLine().trim();
            socketNumber = Integer.parseInt(br.readLine().trim());
        } catch (IOException e) {
            System.err.println(
                "Check to make sure Client.config is in directory and client has read permission");
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Make sure the Client.config file is in correct format");
            System.err.println("First Line holds server ip: X.X.X.X where X is an integer between 0-255");
            System.err.println("Second line holds socket number: default 2000");
            e.printStackTrace();
        }
        runClient(serverIP, socketNumber);
    }

    /* Main program for client - connect to server */
    public static void runClient(String socketIP, int socketNumber) {
        int command = 8;  // hold clients command
        String[] commandList = {"insert", "update", "upSert", "get", "delete", "find", "clear", "count"};
        String serverResponse; // hold server response
        String output = "";
        // Connect to a server using the IP address and port #
        try (Socket serverSocket = new Socket(InetAddress.getByName(socketIP), socketNumber)) {
            //Link to server
            PrintStream p = new PrintStream(serverSocket.getOutputStream());
            Scanner serverScanner = new Scanner(serverSocket.getInputStream());
            while (serverScanner.hasNextLine() ) {
                // unfortunately Java's scanner is horrible with detecting input when used over a socket
                if (command < 9)
                    while (!serverSocket.isClosed() && !(serverResponse = serverScanner.nextLine()).trim().equals("END"))
                        System.out.println(serverResponse);
                userPrompt();
                command = sc.nextInt(); //if user entered number then get
                sc.nextLine(); // clear line
                // quit
                if (command == 0)
                    break;
                else if (command < 7) {
                    System.out.println("Enter all the argument you want to send and enter SEND to send");
                    if (command < 4) //Handle all the one input stuff
                        output = twoInput();
                    else //Handle the two argument commands
                        output = oneInput();
                }
                if (command < 9) {
                    p.println(commandList[command - 1]);
                    System.out.println(commandList[command - 1]);
                    p.println(output);
                    p.println("END");
                    System.out.println(output);
                    output = "";
                }
            }
            System.out.println("Successfully exited client");
        } catch (IOException e) {
            System.out.println("Socket could not be created, check processes permissions");
            e.printStackTrace();
            sc.close();
        }
//        finally{
//            if(!sc.isClosed())
//                sc.close();
//        }
    }


    /* prompt for the client */
    private static void userPrompt() {
        System.out.println("Select a command to enter or 0 to quit");
        System.out.println("1.Insert - Create a new key-value pairs");
        System.out.println("2.Update - update key value pairs");
        System.out.println("3.UpSert - update the key value pair or insert if it doesn't exist");
        System.out.println("4.Get - retrieve the values of the corresponding keys");
        System.out.println("5.Delete - remove the values of the corresponding keys");
        System.out.println("6.Find - Check if keys exist in store");
        System.out.println("7.Clear - Empty out the store");
        System.out.println("8.Count - get the size of the key-value store");
    }

    /* Handle the functions with one arguments, used ArrayDeque instead a linked list because it is more efficient*/
    private static String oneInput() {
        String input;
        Deque<String> keys = new ArrayDeque<>();
        System.out.println("Enter the list of keys");
        while (sc.hasNextLine() && !(input = sc.nextLine()).equals("SEND"))
            keys.add(input); 
        return gson.toJson(keys);
    }

    /* Handles function with one parameter, Use HashMap to store functions  */
    private static String twoInput() {
        String input;
        Map<String, String> pairs = new HashMap<>();
        System.out.println("Enter the list of key value pairs");
        System.out.println("Separate them with a comma (e.g. key, value)");
        while (sc.hasNextLine() && !(input = sc.nextLine()).equals("SEND")) {
            String[] argument = input.split(",");
            pairs.put(argument[0], argument[1]);
        }
        return gson.toJson(pairs);
    }
}
