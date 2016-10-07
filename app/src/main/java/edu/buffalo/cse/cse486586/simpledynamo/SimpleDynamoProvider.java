package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.CursorWrapper;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    private SQLiteDatabase db;
    static final String database_name = "keyvalue";
    static final String table_name = "keyvaluetable";
    static final int Database_version = 1;
    static final String Create_DB_table = " CREATE TABLE " + table_name + " (" +
            "key TEXT," +
            "value TEXT);";

    String myPort = "";
    String node_id = "";
    static final int SERVER_PORT = 10000;
    int chordsize = 0;
    int GDUMPcount = 0;
    String successorportid = "";
    String succesornodeid = "";
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    String predecessornodeid = "";
    String predecessorportid = "";
    LinkedList<String> nodelist = new LinkedList<String>();
    boolean waitflag = true;
    Cursor resultquery;
    String[] nodelistarray = {"5554", "5556", "5558", "5560", "5562"};
    HashMap<String, String> predecessormap = new HashMap<String, String>();
    HashMap<String, String> successormap = new HashMap<String, String>();

    private static class Databasehelper extends SQLiteOpenHelper {
        Databasehelper(Context context) {
            super(context, database_name, null, Database_version);
        }

        public void onCreate(SQLiteDatabase db) {
            db.execSQL(Create_DB_table);
        }

        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("Drop table if exists" + table_name);
            onCreate(db);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String[] key = {selection};
        String hashkey = "";
        String hashnode_id = "";
        String hashsuccesornodeid = "";
        String msgTosend = "";
        String hashpredecessornodeid = "";
        Boolean flag = true;
        if (key[0].equals("@")) {
            return db.delete(table_name, null, null);
        }
        if (key[0].equals("*")) {
            for (int i = 0; i < nodelist.size(); i++) {
                msgTosend = "deleteall,";
                try {
                    String nodetosendport = Integer.toString(Integer.parseInt(nodelist.get(i)) * 2);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(nodetosendport));
                    new multiclientthread(socket, msgTosend).start();
                } catch (UnknownHostException e) {
                    Log.e("deleteall", "Unknownhostexception");
                } catch (IOException e) {
                    Log.e("deleteall", "IOException");
                }
            }
            return db.delete(table_name, null, null);
        }
        String recievedmsg = "";
        String nodetosend = nodetosend(shagenerator(key[0]));
        msgTosend = "delete," + key[0] + ",1,nofail";
        recievedmsg += sendrecievemsg(msgTosend, nodetosend, "delete");
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        String key = values.getAsString("key");
        String[] key1 = {key};
        qBuilder.setTables(table_name);
        String hashkey = shagenerator(key);
        String msgTosend = "";
        String nodeTosend = nodetosend(hashkey);
        String recievedmsg = "";
        msgTosend = "insert," + key + "," + values.getAsString("value") + ",1,nofail";
        Log.e("nodetosendinsert", nodeTosend + "," + msgTosend);
        recievedmsg += sendrecievemsg(msgTosend, nodeTosend, "insert");
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Context context = getContext();
        for (int i = 0; i < nodelistarray.length; i++) {
            nodelist.add(nodelistarray[i]);
        }
        Collections.sort(nodelist, new nodecomparator());
        Databasehelper dbHelper = new Databasehelper(context);
        db = dbHelper.getWritableDatabase();
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        node_id = Integer.toString(Integer.parseInt(myPort) / 2);
        int nodeposition = nodelist.indexOf(node_id);
        if ((!nodelist.get(nodeposition).equals(nodelist.getLast())) && (!nodelist.get(nodeposition).equals(nodelist.getFirst()))) {
            succesornodeid = nodelist.get(nodeposition + 1);
            successorportid = Integer.toString(Integer.parseInt(succesornodeid) * 2);
            predecessornodeid = nodelist.get(nodeposition - 1);
            predecessorportid = Integer.toString(Integer.parseInt(predecessornodeid) * 2);
        } else {
            if (nodelist.get(nodeposition).equals(nodelist.getFirst())) {
                succesornodeid = nodelist.get(nodeposition + 1);
                successorportid = Integer.toString(Integer.parseInt(succesornodeid) * 2);
                predecessornodeid = nodelist.getLast();
                predecessorportid = Integer.toString(Integer.parseInt(predecessornodeid) * 2);
            } else {
                succesornodeid = nodelist.getFirst();
                successorportid = Integer.toString(Integer.parseInt(succesornodeid) * 2);
                predecessornodeid = nodelist.get(nodeposition - 1);
                predecessorportid = Integer.toString(Integer.parseInt(predecessornodeid) * 2);
            }
        }
        for (int i = 0; i < nodelist.size(); i++) {
            if (nodelist.get(i).equals(nodelist.getFirst())) {
                predecessormap.put(nodelist.get(i), nodelist.getLast());
                successormap.put(nodelist.get(i), nodelist.get(i + 1));
            } else if (nodelist.get(i).equals(nodelist.getLast())) {
                predecessormap.put(nodelist.get(i), nodelist.get(i - 1));
                successormap.put(nodelist.get(i), nodelist.getFirst());
            } else {
                predecessormap.put(nodelist.get(i), nodelist.get(i - 1));
                successormap.put(nodelist.get(i), nodelist.get(i + 1));
            }

        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("serversocket", "Can't create a ServerSocket");
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        /* database querying
         * http://www.tutorialspoint.com/android/android_content_providers.htm
         */

        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        String hashkey = "";
        String hashnode_id = "";
        String hashsuccesornodeid = shagenerator(succesornodeid);
        String msgTosend = "";
        String hashpredecessornodeid = shagenerator(predecessornodeid);
        String value1 = "";
        String key1 = "";
        if (selection.equals("@")) {
            Cursor cursor = db.rawQuery("SELECT * FROM " + table_name, null);
            Log.i("Curosrcount", cursortoString(cursor));
            return cursor;
        }
        LinkedList<String> Globalkey = new LinkedList<String>();
        LinkedList<String> Globalvalue = new LinkedList<String>();
        if (selection.equals("*")) {
            for (int i = 0; i < nodelist.size(); i++) {
                String allrecievedmsg = "";
                msgTosend = "queryall," + myPort;
                allrecievedmsg += sendrecievemsg(msgTosend, nodelist.get(i), "queryall");
                String[] input = allrecievedmsg.split(",");
                if(input.length>1) {
                    String[] key = StringtoArray(input[1]);
                    String[] value = StringtoArray(input[2]);
                    for (int j = 0; j < key.length; j++) {
                        Globalkey.add(key[j]);
                        Globalvalue.add(value[j]);
                        Log.i("allreceivedmsg", allrecievedmsg);
                    }
                }
            }
            CursorWrapper allquerycursor1 = new CursorWrapper(queryresultall(Globalkey, Globalvalue));
            return allquerycursor1;
        }
        hashkey = shagenerator(selection);
        String nodeToSend = nodetoread(hashkey);
        /* referred for full duplex connection  http://codetoearn.blogspot.com/2013/01/tcp-socket-example-basics.html
         *
         */

        if(nodeToSend.equals(node_id)){
            Log.e("local query","executing "+selection);
            String[] key={selection};
            Cursor cursor= db.query(table_name, null, "key=?", key, null, null, null);
            while((cursor==null)&&(cursor.getCount()==0)) {
                cursor= db.query(table_name, null, "key=?", key, null, null, null);
            }
            return cursor;
        }

        String recievedmsg = "";
        msgTosend = "query," + selection + "," + myPort;
        Log.e("query",msgTosend);
        recievedmsg = sendrecievemsg(msgTosend, nodeToSend, "query");
        String[] input = recievedmsg.split(",");
        Log.e("recievedmsgfinal", input[0]);
        Cursor query = new CursorWrapper(queryresult(input));
        return query;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String shagenerator(String input) {
        try {
            return genHash(input);
        } catch (NoSuchAlgorithmException e) {
            Log.e("sha error", "shagenerator errpr");
        }
        return null;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        protected Void doInBackground(ServerSocket... sockets) {
            recovermethod();
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket socket1 = serverSocket.accept();
                    new serversocketthread(socket1).start();
            }catch(IOException e){
                Log.e("servertask","exception");
                }
            }
        }
    }

    /* referred oracle documentation on multiserver
    * https://docs.oracle.com/javase/tutorial/networking/sockets/examples/KKMultiServerThread.java
    */
    private class serversocketthread extends Thread{
        private Socket socket1 = null;
        public serversocketthread(Socket socket1) {
            super("serversocketthread");
            this.socket1 = socket1;
        }
        public void run(){
            String t = "";
            String p = "";
            String recievedback = "";
            try{
            DataInputStream inputStream = new DataInputStream(socket1.getInputStream());
            DataOutputStream outputStream = new DataOutputStream(socket1.getOutputStream());
            t = inputStream.readUTF();
            Log.i("t", t);
            String[] input = t.split(",");
            if (input[0].equals("insert")) {
                ContentValues mContentValues = new ContentValues();
                SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
                mContentValues.put("key", input[1]);
                mContentValues.put("value", input[2]);
                String[] key1 = {input[1]};
                int count = Integer.parseInt(input[3]);
                Cursor cursor = db.query(table_name, null, "key=?", key1, null, null, null);
                qBuilder.setTables(table_name);
                cursor.moveToFirst();
                if ((cursor != null) && (cursor.getCount() > 0)) {
                    long rowID = db.update(table_name, mContentValues, "key=?", key1);
                } else {
                    if((nodetosend(shagenerator(input[1])).equals(node_id))||(nodetosend(shagenerator(input[1])).equals(predecessornodeid))||(nodetosend(shagenerator(input[1])).equals(predecessormap.get(predecessornodeid)))) {
                        long rowID = db.insert(table_name, null, mContentValues);
                    }
                    mContentValues.clear();
                }
                count++;
                if (((count < 4) && (input[4].equals("nofail")))) {
                    String recievedback1 = input[0] + "," + input[1] + "," + input[2] + "," + Integer.toString(count) + ",nofail";
                    sendrecievemsg(recievedback1, succesornodeid, "insert", outputStream);
                } else if ((count < 3) && (input[4].equals("fail"))) {
                    String recievedback1 = input[0] + "," + input[1] + "," + input[2] + "," + Integer.toString(count) + ",fail";
                    sendrecievemsg(recievedback1, succesornodeid, "insert", outputStream);
                } else {
                    String ackmsg = "ack";
                    outputStream.writeUTF(ackmsg);
                    outputStream.flush();
                    outputStream.close();
                }
            }

            if (input[0].equals("deleteall")) {
                db.delete(table_name, null, null);
            }
            if (input[0].equals("delete")) {
                Log.e("delete", input[1] + "," + input[2]);
                int count = Integer.parseInt(input[2]);
                String[] deletekey = {input[1]};
                db.delete(table_name, "key=?", deletekey);
                count++;
                if (((count < 4) && (input[3].equals("nofail")))) {
                    String recievedback1 = input[0] + "," + input[1] + "," + Integer.toString(count) + ",nofail";
                    sendrecievemsg(recievedback1, succesornodeid, "delete", outputStream);

                } else if ((count < 3) && (input[3].equals("fail"))) {
                    String recievedback1 = input[0] + "," + input[1] + "," + Integer.toString(count) + ",fail";
                    sendrecievemsg(recievedback1, succesornodeid, "delete", outputStream);

                } else {
                    String ackmsg = "ack,";
                    outputStream.writeUTF(ackmsg);
                    outputStream.flush();
                }
            }

            if (input[0].equals("queryall")) {
                Log.e("queryall", "recieved");
                Cursor cursor = db.rawQuery("SELECT * FROM " + table_name, null);
                String queryresulttosend = "";

                queryresulttosend = "queryallresult," + cursortoString(cursor);
                try {
                    outputStream.writeUTF(queryresulttosend);
                    outputStream.flush();
                } catch (UnknownHostException e) {
                    Log.e("queryallservertask", "Unknownhostexception");
                } catch (IOException e) {
                    Log.e("queryallservertask", "IOException");
                }
            }
            if (input[0].equals("query")) {
                Log.e("query",t);
                String[] key = {input[1]};
                Cursor cursor= db.query(table_name, null, "key=?", key, null, null, null);
                while((cursor==null)&&(cursor.getCount()==0)) {
                    cursor= db.query(table_name, null, "key=?", key, null, null, null);
                }
                Log.e("queryresult", cursortoString(cursor));
                String queryresult = "queryresult," + cursortoString(cursor);
                try {
                    outputStream.writeUTF(queryresult);
                    outputStream.flush();
                } catch (UnknownHostException e) {
                    Log.e("queryservertask", "Unknownhostexception");
                } catch (IOException e) {
                    Log.e("queryservertask", "IOException");
                }
            }
            inputStream.close();
            outputStream.close();
            socket1.close();
        } catch (IOException e) {
            e.printStackTrace();
            Log.e("servertask", "IOexceptionerror");
        }
            t = "";
            p = "";
            recievedback = "";
        }
    }




    private class multiclientthread extends Thread {
        private Socket socket = null;
        private String msgtoSend = null;

        public multiclientthread(Socket socket, String msgtoSend) {
            super("multiclientthread");
            this.socket = socket;
            this.msgtoSend = msgtoSend;
        }

        public void run() {
            boolean flag = true;
            while (flag) {
                try {
                    try {

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.write(msgtoSend);
                        out.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    socket.close();
                } catch (IOException e) {
                    Log.e("socketexcpetion", "socketclose exception");
                }
                flag = false;
            }
        }
    }

    /*
     * reference for building a comparator for linkedlist
     * http://www.java2novice.com/java-collections-and-util/linkedlist/sort-comparator/
     */
    private class nodecomparator implements Comparator<String> {
        public int compare(String nodeid1, String nodeid2) {
            String hashnodeid1 = "";
            String hashnodeid2 = "";
            try {
                hashnodeid1 = genHash(nodeid1);
                hashnodeid2 = genHash(nodeid2);
            } catch (NoSuchAlgorithmException e) {
                Log.e("nodeportcount", "nodeportcount error");
            }
            if (hashnodeid1.compareTo(hashnodeid2) < 0) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private String nodetosend(String input) {
        /*
          Send hash value as parameter ,do not forget
         */
        String hashkey = input;
        for (int i = 0; i < nodelist.size(); i++) {
            String hashnode = shagenerator(nodelist.get(i));
            String predecessorhash = shagenerator(predecessormap.get(nodelist.get(i)));
            if ((predecessorhash.compareTo(hashkey) < 0) && (hashnode.compareTo(hashkey) > 0)) {

                return nodelist.get(i);
            }
            if ((nodelist.get(i).equals(nodelist.getLast()) && (predecessorhash.compareTo(hashkey) < 0) && (hashnode.compareTo(hashkey) < 0))) {

                return nodelist.getFirst();
            }
            if ((nodelist.get(i).equals(nodelist.getFirst())) && (hashnode.compareTo(hashkey) > 0)) {
                return nodelist.getFirst();
            }
        }
        return null;
    }

    private Cursor queryresultall(LinkedList<String> input1, LinkedList<String> input2) {
        MatrixCursor cursorresult = new MatrixCursor(new String[]{"key", "value"});
        String[] arrayinput1 = new String[input1.size()];
        String[] arrayinput2 = new String[input2.size()];
        for (int i = 0; i < input1.size(); i++) {
            arrayinput1[i] = input1.get(i);
            arrayinput2[i] = input2.get(i);
        }
        for (int i = 0; i < arrayinput1.length; i++) {
            cursorresult.addRow(new Object[]{arrayinput1[i], arrayinput2[i]});
        }
        return cursorresult;
    }

    private Cursor queryresult(String[] input) {
        MatrixCursor cursorresult = new MatrixCursor(new String[]{"key", "value"});
        input[1] = input[1].substring(1, input[1].length() - 1);
        input[2] = input[2].substring(1, input[2].length() - 1);
        cursorresult.addRow(new Object[]{input[1], input[2]});
        return cursorresult;
    }

    private String[] StringtoArray(String input) {
        String x = input.substring(1, input.length() - 1);
        x = x.replaceAll("\\s+", "");
        String[] y = x.split("#");
        return y;
    }

    private String cursortoString(Cursor cursor) {
        String key[] = new String[cursor.getCount()];
        String value[] = new String[cursor.getCount()];
        int i = 0;
                /* cursor traversal reference
                 * http://stackoverflow.com/questions/4920528/iterate-through-rows-from-sqlite-query
                 */

        cursor.moveToFirst();
        while (!cursor.isAfterLast()) {
            key[i] = cursor.getString(cursor.getColumnIndex("key"));
            value[i] = cursor.getString(cursor.getColumnIndex("value"));
            i++;
            cursor.moveToNext();
        }
        i = 0;
        String keyasArray = Arrays.toString(key);
        String valueasArray = Arrays.toString(value);
        keyasArray = keyasArray.replace(",", "#");
        valueasArray = valueasArray.replace(",", "#");
        return keyasArray + "," + valueasArray;

    }

    private String nodetoread(String input) {
        String nodeposition = nodetosend(input);
        nodeposition = successormap.get(nodeposition);
        nodeposition = successormap.get(nodeposition);
        return nodeposition;
    }

    public String sendrecievemsg(String msgTosend, String nodetosend, String msgType, DataOutputStream outputStream1) {
        String recievedmsg = "";
        try {
            String nodetosendport = Integer.toString(Integer.parseInt(nodetosend) * 2);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(nodetosendport));
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());
            outputStream.writeUTF(msgTosend);
            outputStream.flush();
            String ackmsg = "ack,";
            String character = inputStream.readUTF();
            recievedmsg = character;
            inputStream.close();
            outputStream.close();
            try {
                outputStream1.writeUTF(ackmsg);
                outputStream1.flush();
                outputStream1.close();
            } catch (IOException e) {
                Log.e("outputStream1", "the guy querying failed");
            }
        } catch (IOException e) {
            Log.e("nodefail", "executing " + msgTosend+", "+nodetosend);
            recievedmsg = "";
            if (msgType.equals("insert")) {
                recievedmsg += nodefail(msgTosend, nodetosend, "insert", outputStream1);
            } else if (msgType.equals("delete")) {
                recievedmsg += nodefail(msgTosend, nodetosend, "delete", outputStream1);
            } else if (msgType.equals("query")) {
                recievedmsg += nodefail(msgTosend, nodetosend, "query", outputStream1);
            } else {
                Log.e("queryall", "nodefailexception");
            }
        }
        return recievedmsg;
    }

    public String nodefail(String msgTosend, String node_id, String msgtype) {
        String ackmsg = "";
        String nextnode_id = "";
        Log.e("nodefail", msgTosend);
        String[] input = msgTosend.split(",");
        try {
            if (msgtype.equals("insert")) {
                nextnode_id = successormap.get(node_id);
                msgTosend = input[0] + "," + input[1] + "," + input[2] + "," + input[3] + ",fail";
            } else if (msgtype.equals("delete")) {
                nextnode_id = successormap.get(node_id);
                msgTosend = input[0] + "," + input[1] + "," + input[2] + ",fail";
            } else {
                msgTosend = msgTosend;
                nextnode_id = predecessormap.get(node_id);
            }
            String nextnodesendport = Integer.toString(Integer.parseInt(nextnode_id) * 2);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(nextnodesendport));
            DataOutputStream outputStream1 = new DataOutputStream(socket.getOutputStream());
            DataInputStream inputStream1 = new DataInputStream(socket.getInputStream());
            outputStream1.writeUTF(msgTosend);
            outputStream1.flush();
            String character1 = inputStream1.readUTF();
            ackmsg += character1;
            inputStream1.close();
            outputStream1.close();
        } catch (IOException e) {
            Log.e("nodefail", "IOexception2nd red flag");
        }
        return ackmsg;
    }

    public String sendrecievemsg(String msgTosend, String nodetosend, String msgType) {
        String recievedmsg = "";
        try {
            Log.e("sendrecievemsg", "executing");
            String nodetosendport = Integer.toString(Integer.parseInt(nodetosend) * 2);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(nodetosendport));
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());
            outputStream.writeUTF(msgTosend);
            outputStream.flush();
            String character = inputStream.readUTF();
            recievedmsg = character;
            inputStream.close();
            outputStream.close();
        } catch (IOException e) {
            Log.e("nodefail", e.toString());
            Log.e("nodefail", "executing " + msgTosend+", "+nodetosend);
            recievedmsg = "";
            if (msgType.equals("insert")) {
                recievedmsg = nodefail(msgTosend, nodetosend, "insert");
            } else if (msgType.equals("delete")) {
                recievedmsg = nodefail(msgTosend, nodetosend, "delete");
            } else if (msgType.equals("query")) {
                recievedmsg = nodefail(msgTosend, nodetosend, "query");
            } else {
                Log.e("queryall", "nodefailexception");
            }

        }
        return recievedmsg;
    }

    public String nodefail(String msgTosend, String node_id, String msgtype, DataOutputStream outputStream) {
        String ackmsg = "";
        String nextnode_id = "";
        Log.e("nodefail", msgTosend);

        String[] input = msgTosend.split(",");
        try {
            if (msgtype.equals("insert")) {
                nextnode_id = successormap.get(node_id);
                if(Integer.parseInt(input[3])<3) {
                    msgTosend = input[0] + "," + input[1] + "," + input[2] + "," + input[3] + ",fail";
                }
                else{
                    return "ack,";
                }
            } else if (msgtype.equals("delete")) {
                nextnode_id = successormap.get(node_id);
                msgTosend = input[0] + "," + input[1] + "," + input[2] + ",fail";
            } else {
                nextnode_id = predecessormap.get(node_id);
            }
            String nextnodesendport = Integer.toString(Integer.parseInt(nextnode_id) * 2);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(nextnodesendport));

            DataOutputStream outputStream1 = new DataOutputStream(socket.getOutputStream());
            DataInputStream inputStream1 = new DataInputStream(socket.getInputStream());
            outputStream1.writeUTF(msgTosend);
            outputStream1.flush();
            String ackmsg1 = "ack,";
            String character1 = inputStream1.readUTF();
            ackmsg += character1;
            inputStream1.close();
            outputStream1.close();
            outputStream.writeUTF(ackmsg);
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            Log.e("nodefail", "IOexception fail 2nd");
        }
        return ackmsg;
    }


    public void recovermethod() {
        LinkedList<String> recover = new LinkedList<String>();
        recover.add(succesornodeid);
        recover.add(predecessornodeid);
        String msgTosend = "";
        String allrecievedmsg = "";
        LinkedList<String> Globalkey = new LinkedList<String>();
        LinkedList<String> Globalvalue = new LinkedList<String>();
        for (int i = 0; i < recover.size(); i++) {
            msgTosend = "queryall," + myPort;
            allrecievedmsg = sendrecievemsg(msgTosend, recover.get(i), "queryall");
            Log.e("queryallrecover",allrecievedmsg);
            String[] input = allrecievedmsg.split(",");
            if (input.length == 3) {
                String[] key = StringtoArray(input[1]);
                String[] value = StringtoArray(input[2]);
                for (int j = 0; j < key.length; j++) {
                    Globalkey.add(key[j]);
                    Globalvalue.add(value[j]);
                }
            }
        }
        for (int i = 0; i < Globalkey.size(); i++) {
            String nodeTosend = nodetosend(shagenerator(Globalkey.get(i)));
            Log.e("queryallrecover1","queryresult ,"+Globalkey.get(i)+","+shagenerator(Globalkey.get(i))+", "+nodeTosend);
            if (nodeTosend.equals(node_id) || nodeTosend.equals(predecessormap.get(predecessornodeid)) || nodeTosend.equals(predecessornodeid)) {
                ContentValues mContentValues = new ContentValues();
                SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
                mContentValues.put("key", Globalkey.get(i));
                mContentValues.put("value", Globalvalue.get(i));
                String[] key1 = {Globalkey.get(i)};
                Cursor cursor = db.query(table_name, null, "key=?", key1, null, null, null);
                qBuilder.setTables(table_name);
                cursor.moveToFirst();
                if ((cursor != null) && (cursor.getCount() > 0)) {
                    long rowID = db.update(table_name, mContentValues, "key=?", key1);
                } else {
                    long rowID = db.insert(table_name, null, mContentValues);
                    mContentValues.clear();
                }
            }

        }
    }

}

