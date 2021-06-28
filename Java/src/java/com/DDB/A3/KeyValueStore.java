/* Abdullah Arif
* Custom key value store structure for COMP-4680
* T is the type for the key and G is the type for the Value */
package com.DDB.A3;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashMap;

public class KeyValueStore<T, G> {
    final HashMap<T, G> map;

    //Create
    public KeyValueStore() {
        map = new HashMap<>();
    }

    // Insert - enables the user to insert a key and corresponding value into the store
    public synchronized void insert(T key, G value) {
        map.put(key, value);
    }

    //Get - enables the sure to retrieve a value using the corresponding key
    public G get(T key) {
        return map.get(key);
    }

    // Delete - enables the user to delete a value from the store using the corresponding key.
    public synchronized G delete(T key) {
        return map.remove(key);
    }

    // Find - enables the user to check if a key exists in the store or not
    public boolean find(T key) {
        return map.containsKey(key);
    }

    // Update - enables the user to update the value of an existing key.
    public synchronized boolean update(T key, G newValue) {
        if (map.containsKey(key)) {
            map.replace(key, newValue);
            return true;
        }
        return false; // if there was no previous value
    }

    // UpSert - enables the user to update the value of an existing key and if the key does not exist,
    // it will insert the new value with the corresponding key
    public synchronized void upSert(T key, G newValue) {
        if (map.containsKey(key)) {
            map.replace(key, newValue);
            return; // key was not create
        }
        // if there was no previous value
        map.put(key, newValue);
    }

    //Clear - enables the user to remove all the  key-value items stored in the key-value store
    public synchronized void clear() {
        map.clear();
    }

    //Count returns the number of key-value items stored in the key-value store
    public int count() {
        return map.size();
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(map);
    }

}



 
