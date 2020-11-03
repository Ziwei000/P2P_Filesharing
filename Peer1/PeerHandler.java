import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.io.*;
import java.net.*;

public class PeerHandler {
    static String exactfileName;
    static int ttchunckc;
    static List<Integer> chunksIHave = new ArrayList<Integer>();
    Map<Integer, Integer> statistics = new HashMap<Integer, Integer>();
    static List<Integer> alreadyRequestedChunkList = new CopyOnWriteArrayList<Integer>();
    
    boolean isInitiated = false;
    int myPort;
    

    static String gpbcid(int chunkID) {
        return "./temp/" + exactfileName + chunkID;
    }

    boolean ddnmchunck() {
        for (int i = 1; i <= ttchunckc; i++) {
            if (!chunksIHave.contains(i)) {
                return true;
            }
        }
        return false;
    }

    public static void ccffchuncks(String exactfileName, int chunkCount) {
        final int BUFFERSIZE = 100000;
        File output = new File("./" + exactfileName);
        try {
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(output), BUFFERSIZE);
            byte[] bytes = new byte[BUFFERSIZE];
            int bytesRead;
            for (int chunkID = 1; chunkID <= chunkCount; chunkID++) {
                String fullName = "./temp/" + exactfileName + chunkID;
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(fullName));
                while ((bytesRead = in.read(bytes, 0, bytes.length)) != -1) {
                    out.write(bytes, 0, bytesRead);
                }
                try {
                    in.close();
                } catch (Exception exception) {
                }
            }
            out.close();
        } catch (Exception exception) {
        }
    }

    

    boolean ddinit() {
        return !isInitiated;
    }

    static List<Integer> ggmchunks() {
        return chunksIHave;
    }

    int rrcch() {
        return ggmchunks().size();
    }
    synchronized public boolean isalreq(int nextChunkID) {
        if(alreadyRequestedChunkList.contains(nextChunkID)){
            return true;   
        }
        else{
            alreadyRequestedChunkList.add(nextChunkID);
            return false;
        }
    }

    public void run(int fileOwnerPort, int myPort, int peerPort) {
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get("./temp"));
            this.myPort = myPort;
            new ConsumeFileOwner(fileOwnerPort).start();
            new ConsumeFileOwner(peerPort).start();
            new FileMergerThread().start();
            new ServePeer(myPort).start();
        } catch (IOException e) {}
    }
    private class FileMergerThread extends Thread {
        public void run(){
            while(ddinit() || ddnmchunck()){
                try{
                    Thread.sleep(2000);
                } catch(Exception exception){exception.printStackTrace();}
            }
            ccffchuncks(exactfileName, ttchunckc);
            System.out.println(exactfileName + " have merged!!!");
            statistics.entrySet().forEach(entry->{
                System.out.println("Standby: " + entry.getKey() + " " + entry.getValue() + " chunks has been completed.");
            });
        }
    }

    private class ConsumeFileOwner extends Thread {
        Socket socket;
        int port;
        int nextChunkID = -1;
        Utility utility = new Utility();
        ConsumeFileOwner(int fileOwnerPort){
            port = fileOwnerPort;
        }
        public void run(){
            
            while(ddinit() || ddnmchunck()){
                try{
                    
                    socket = new Socket("localhost", port);
                    String request="", response="";
                    if(ddinit()){
                        request = myPort+" init";
                        utility.sendsss(new ObjectOutputStream(socket.getOutputStream()), request);
                        response = utility.recievesss(new ObjectInputStream(socket.getInputStream()));
                        System.out.println("["+myPort + "]-["+port+"] " + request);
                        System.out.println("["+port + "]-["+myPort+"] " + response);
                        String[] responseSplitted = response.split(" ");
                        if(Boolean.parseBoolean(responseSplitted[0])){
                            exactfileName = responseSplitted[1];
                            ttchunckc = Integer.parseInt(responseSplitted[2]);
                            isInitiated = true;
                        }
                    } else if (nextChunkID<0){
                        request = myPort+" getlist";
                        utility.sendsss(new ObjectOutputStream(socket.getOutputStream()), request);
                        response = utility.recievesss(new ObjectInputStream(socket.getInputStream()));
                        System.out.println("{"+myPort + "}-{"+port+"} " + request);
                        
                        String[] responseSplitted = response.split(" ");
                        if(Boolean.parseBoolean(responseSplitted[0])){
                            List<Integer> myChunks = ggmchunks();
                            List<Integer> chunksHeHasAndIDont = new ArrayList<Integer>();
                            for(int i=1;i<responseSplitted.length;i++){
                                if(!myChunks.contains(Integer.parseInt(responseSplitted[i]))){
                                    chunksHeHasAndIDont.add(Integer.parseInt(responseSplitted[i]));
                                    System.out.print(responseSplitted[i] + " ");
                                }
                            }

                            nextChunkID = chunksHeHasAndIDont.get(new Random().nextInt(chunksHeHasAndIDont.size()));
                            boolean isPresent = isalreq(nextChunkID);
                            nextChunkID = isPresent ? -1 : nextChunkID;
                            System.out.println(port + " has [" + nextChunkID + "] that I don't have in this time. I want it in this time.");
                        }
                    } else if (nextChunkID>0){
                        request = myPort+" get "+nextChunkID;
                        System.out.println("["+myPort + "]-["+port+"] " + request);
                        utility.sendsss(new ObjectOutputStream(socket.getOutputStream()), request);
                        try {
                            utility.receivefff(gpbcid(nextChunkID), socket.getInputStream());
                            System.out.println("["+port + "] gave me a chunk id [" + nextChunkID + "]");
                        } catch (Exception exception) {}
                        chunksIHave.add(nextChunkID);
                        if(statistics.get(port) == null){statistics.put(port, 1);}
                        else{statistics.put(port, statistics.get(port)+1);}
                        nextChunkID = -1;
                    }
                } catch (Exception exception){
                    
                } finally {
                    try{
                        if(socket!=null)socket.close();
                    } catch (Exception exception){exception.printStackTrace();}
                }
            }
        }

        
    }

    private static class ServePeer extends Thread {
        int port;
        ServerSocket serverSocket;
        Utility utility = new Utility();
        ServePeer(int myPortAsServer) {
            port = myPortAsServer;
        }
        public void run(){
            try {
                serverSocket = new ServerSocket(port);
                System.out.println("Server is at  a port:!!! " + port + ".");
                try{
                    while(true){
                        handleRequest();
                    }
                } catch (Exception exception){exception.printStackTrace();}
            } catch(Exception exception){exception.printStackTrace();}
            finally {
                try{serverSocket.close();} catch (Exception exception){exception.printStackTrace();}
            }
        }
        public void handleRequest(){
            try{
                Socket socket = serverSocket.accept();
                String request = utility.recievesss(new ObjectInputStream(socket.getInputStream()));
                String response = "";
                String[] requestSplitted = request.split(" ");
                String requesterID = requestSplitted[0];
                String requestType = requestSplitted[1];
                System.out.println("["+requesterID +"] "+requestType + ((requestSplitted.length>2) ? " "+requestSplitted[2] : ""));
                switch(requestType){
                    case "getlist":
                        List<Integer> chunkList = ggmchunks();
                        response = "true";
                        for(int i=0;i<chunkList.size();i++){
                            response += " " + chunkList.get(i);
                        }
                        System.out.println("RESPONSE A ["+requesterID+"]  " + response);
                        utility.sendsss(new ObjectOutputStream(socket.getOutputStream()), response);
                        break;
                    case "init":
                        response = "false " + exactfileName + " " + ttchunckc;
                        System.out.println("RESPONSE A ["+requesterID+"]  " + response);
                        utility.sendsss(new ObjectOutputStream(socket.getOutputStream()), response);
                        break;
                    
                    case "get":
                        String desiredFilePath = gpbcid(Integer.parseInt(requestSplitted[2]));
                        System.out.println("RESPONSE A ["+requesterID+"]  with chunk "+ requestSplitted[2] + " from path " + desiredFilePath);
                        utility.sendfff(desiredFilePath, socket.getOutputStream());
                        break;
                    case "thanks":
                        break;
                }
            } catch (Exception exception){exception.printStackTrace();}
        }
    }
}

final class Utility {
    public void sendsss(ObjectOutputStream objectOutputStream, String message) {
        try {
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
    public String recievesss(ObjectInputStream objectInputStream){
        try{
            return (String) objectInputStream.readObject();
        } catch (Exception exception){
            
        }
        return null;
    }
    public void sendfff(String exactfileName, OutputStream outputStream) {
        try {
            File file = new File("./" + exactfileName);
            byte[] byteArray = new byte [(int)file.length()];
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            bufferedInputStream.read(byteArray,0,byteArray.length);
            outputStream.write(byteArray, 0, byteArray.length);
            outputStream.flush();
            outputStream.close();
        } catch (Exception exception) {
            System.out.println("\t" + exception.getLocalizedMessage() + "\n");
        } finally {
            try {
                if(outputStream != null) {outputStream.close();}
            } catch (Exception exception) {
                System.out.println("\t" + exception.getLocalizedMessage() + "\n");
            }
        }
    }

    public void receivefff(String filePath, InputStream inputStream) {
        try {
            File someFile = new File(filePath);
            FileOutputStream fileOutputStream = new FileOutputStream(someFile);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            int bytesRead = 0;
            int b;
            while ((b = inputStream.read()) != -1) {
                bufferedOutputStream.write(b);
                bytesRead++;
            }
            bufferedOutputStream.flush();
            bufferedOutputStream.close();
            fileOutputStream.close();
            inputStream.close();
        } catch(Exception exception) {
            System.out.println("\t" + exception.getLocalizedMessage() + "\n");
        } finally {
            try {
                if(inputStream != null) {inputStream.close();}
            } catch (Exception exception) {
                System.out.println("\t" + exception.getLocalizedMessage() + "\n");
            }
        }
    }
}
