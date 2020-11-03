public class Peer {
    public static void main(String[] arguments){
        
        PeerHandler peerHandler = new PeerHandler();
        peerHandler.run(Integer.parseInt(arguments[0]), 
        Integer.parseInt(arguments[1]),
        Integer.parseInt(arguments[2]));
        
    }
}