import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

public class FileOwner {

    static String chunkFilePath = "./temp/chunk.txt";
    static ServerSocket serverSocket;
    static int chunkCount = 0;
    //static int recenta = 100;
    static String fileName = "test.pdf";
   // static int finalre = 0;
    

    public static void openchunckb(){
        try{
            File chunkDatabase = new File(chunkFilePath);
            FileWriter fileWriter = new FileWriter(chunkDatabase);
            fileWriter.write(fileName + " " + chunkCount);
            for(int i=1;i<=chunkCount;i++){
                fileWriter.write("\n"+i+" "+"./temp/"+fileName+i);
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (Exception exception){
            exception.printStackTrace();
        }
    }

    public static void iiichunks(){
        creatdic(chunkFilePath);
        File file = new File("./" + fileName);
        ccff(file);
        openchunckb();
        System.out.println("Segmented " + fileName + " into " + chunkCount + " chunks.");
    }

    public static void ccff(File file){
        RandomAccessFile randomAccessFile = null;
        int chunkSize = 100000;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {e.printStackTrace();}
        FileChannel channel = randomAccessFile.getChannel();
        int chunkID = 1;
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        while(true) {
            try {
                if (!(channel.read(buffer) > 0)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            File chunkFile = new File("./temp/" + file.getName()+chunkID);
            FileChannel chunkFileChannel = null;
            try {
                chunkFileChannel = new FileOutputStream(chunkFile).getChannel();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            buffer.flip();
            try {
                chunkFileChannel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.clear();
            try {
                chunkFileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            chunkID++;
            chunkCount++;
            try {
                chunkFileChannel.close();
            } catch (Exception exception){}
        }
    }

    

    public static void creatdic(String filePath){
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get("./temp"));
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
            writer.write("");
            writer.close();
        } catch(Exception exception) {
            System.err.println(exception.getLocalizedMessage());
        }
    }

    public static String trunkbyid(int chunkID){
        Scanner scanner = null;
        try {
            File file = new File(chunkFilePath);
            scanner = new Scanner(file);
            scanner.nextLine();
            while(scanner.hasNextLine()){
                String[] row = scanner.nextLine().split(" ");
                int id = Integer.parseInt(row[0]);
                if(id == chunkID){
                    return row[1];
                }
            }
        } catch (Exception exception) {
            System.err.println(exception.getLocalizedMessage());
        } finally {
            try {
                scanner.close();
            } catch (Exception exception){}
        }
        return null;
    }

    public static List<Integer> cccclist(){
        List<Integer> list = new ArrayList<Integer>();

        try {
            File file = new File(chunkFilePath);
            Scanner scanner = new Scanner(file);
            scanner.nextLine();
            while(scanner.hasNextLine()){
                String[] row = scanner.nextLine().split(" ");
                int chunkID = Integer.parseInt(row[0]);
                list.add(chunkID);
            }
            try {
                scanner.close();
            } catch (Exception exception){}
        } catch (Exception exception) { exception.printStackTrace();}

        return list;
    }

 

    public static void main(String[] arguments){
        try {
            iiichunks();
            serverSocket = new ServerSocket(Integer.parseInt(arguments[0]));
            System.out.println("Listening :  " + arguments[0] + ".");
            try{
                while(true){
                    new RequestHandler(serverSocket.accept());
                }
            } catch (Exception exception){exception.printStackTrace();}
        } catch (Exception exception){exception.printStackTrace();
        } finally {
            try{
                serverSocket.close();
            } catch (Exception exception){exception.printStackTrace();}
        } 
    }

    private static class RequestHandler extends Thread {
        Socket socket;
        Utility utility = new Utility();
        RequestHandler(Socket socket){
            this.socket = socket;
            try {
                this.handleRequest();
            } catch(Exception exception){exception.printStackTrace();}
            finally {
                try {
                    socket.close();
                } catch (Exception exception){exception.printStackTrace();}
            }
        }
        public void handleRequest(){
            try{
                String request = utility.receivesss(new ObjectInputStream(socket.getInputStream()));
                String response = "";
                String[] requestSplitted = request.split(" ");
                String requesterID = requestSplitted[0];
                String requestType = requestSplitted[1];
                System.out.println("["+requesterID +"] "+requestType + ((requestSplitted.length>2) ? " "+requestSplitted[2] : ""));
                switch(requestType){
                    case "init":
                        response = "true " + fileName + " " + chunkCount;
                        System.out.println("RESPOND!! ["+requesterID+"]  " + response);
                        utility.sendsss(new ObjectOutputStream(socket.getOutputStream()), response);
                        break;
                    case "getlist":
                        List<Integer> chunkList = cccclist();
                        response = "true";
                        for(int i=0;i<chunkList.size();i++){
                            response += " " + chunkList.get(i);
                        }
                        utility.sendsss(new ObjectOutputStream(socket.getOutputStream()), response);
                        break;
                    case "get":
                        String desiredFilePath = trunkbyid(Integer.parseInt(requestSplitted[2]));
                        System.out.println("RESPOND!! ["+requesterID+"]  with chunk size :    "+ requestSplitted[2] + " come from a path " + desiredFilePath);
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
    public String convertFileIntoString(String filePath){
        try {
            File file = new File(filePath);
            byte[] byteArray = new byte [(int)file.length()];
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            bufferedInputStream.read(byteArray,0,byteArray.length);
            System.out.println(byteArray.length);
        } catch (Exception exception) {
            System.out.println("\t" + exception.getLocalizedMessage() + "\n");
        }
        return "";
    }

    public String receivesss(ObjectInputStream objectInputStream){
        try{
            return (String) objectInputStream.readObject();
        } catch (Exception exception){
            
        }
        return null;
    }
    
    public void sendfff(String filePath, OutputStream outputStream) {
        System.out.println("path " + filePath);
        try {
            File file = new File(filePath);
            System.out.println("path " + filePath + " name " + file.getName());
            byte[] byteArray = new byte [(int)file.length()];
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            bufferedInputStream.read(byteArray,0,byteArray.length);
            System.out.println(byteArray.length);
            outputStream.write(byteArray, 0, byteArray.length);
            outputStream.flush();
            outputStream.close();
        } catch (Exception exception) { exception.printStackTrace();} 
        finally {
            try {
                if(outputStream != null) {outputStream.close();}
            } catch (Exception exception) { exception.printStackTrace();}
        }
    }
}
