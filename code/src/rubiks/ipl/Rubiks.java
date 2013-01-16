package rubiks.ipl;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;

/**
 * Solver for rubik's cube puzzle.
 * 
 * @author Niels Drost, Timo van Kessel
 * 
 */
public class Rubiks implements MessageUpcall {
    
    /**
     * Port type used for sending a work request to the master
     */
    PortType requestPortType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_MANY_TO_ONE);

    /**
     * Port type used for sending a reply back
     */
    PortType replyPortType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT,
            PortType.CONNECTION_ONE_TO_ONE);
    
    /**
     * Port type used to broadcast result
     */
    PortType broadcastPortType = new PortType(PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_MANY, PortType.CONNECTION_DOWNCALLS);
    
    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT,
            IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED,
            IbisCapabilities.TERMINATION);

    private Ibis ibis;

    private Cube cube;

    private boolean isMaster;
    private boolean shouldStopWorking;
    private int bestResult = Integer.MAX_VALUE;
    private int numBestSolutions;

    private boolean finished;
    
    public static final boolean PRINT_SOLUTION = false;
    private static final String WORK_REQ_PORT = "work_req_port";
    private static final String BROADCAST_PORT = "broadcast_port";

    private static final int MSG_TYPE_WORK_REQ  = 0;
    private static final int MSG_TYPE_RESULT    = 1;

    
    /**
     * Recursive function to find a solution for a given cube. Only searches to
     * the bound set in the cube object.
     * 
     * @param cube
     *            cube to solve
     * @param cache
     *            cache of cubes used for new cube objects
     * @return the number of solutions found
     */
    private static int solutions(Cube cube, CubeCache cache) {
        if (cube.isSolved()) {
            return 1;
        }

        if (cube.getTwists() >= cube.getBound()) {
            return 0;
        }

        // generate all possible cubes from this one by twisting it in
        // every possible way. Gets new objects from the cache
        Cube[] children = cube.generateChildren(cache);

        int numSolutions = 0;

        for (Cube child : children) {
            // recursion step
            int childSolutions = solutions(child, cache);
            if (childSolutions > 0) {
                numSolutions += childSolutions;
                if (PRINT_SOLUTION) {
                    child.print(System.err);
                }
            }
            // put child object in cache
            cache.put(child);
        }

        return numSolutions;
    }

    /**
     * Solves a Rubik's cube by iteratively searching for solutions with a
     * greater depth. This guarantees the optimal solution is found. Repeats all
     * work for the previous iteration each iteration though...
     * 
     * @param cube
     *            the cube to solve
     */
    private int solve(Cube cube) {
        // cache used for cube objects. Doing new Cube() for every move
        // overloads the garbage collector
        CubeCache cache = new CubeCache(cube.getSize());
        return solutions(cube, cache);
    }

    public static void printUsage() {
        System.out.println("Rubiks Cube solver");
        System.out.println("");
        System.out
                .println("Does a number of random twists, then solves the rubiks cube with a simple");
        System.out
                .println(" brute-force approach. Can also take a file as input");
        System.out.println("");
        System.out.println("USAGE: Rubiks [OPTIONS]");
        System.out.println("");
        System.out.println("Options:");
        System.out.println("--size SIZE\t\tSize of cube (default: 3)");
        System.out
                .println("--twists TWISTS\t\tNumber of random twists (default: 11)");
        System.out
                .println("--seed SEED\t\tSeed of random generator (default: 0");
        System.out
                .println("--threads THREADS\t\tNumber of threads to use (default: 1, other values not supported by sequential version)");
        System.out.println("");
        System.out
                .println("--file FILE_NAME\t\tLoad cube from given file instead of generating it");
        System.out.println("");
    }
    
    public void master(int size, int twists, int seed, String fileName) throws IOException{
        isMaster = true;
        cube = null;

        // create cube
        if (fileName == null) {
            cube = new Cube(size, twists, seed);
        } else {
            try {
                cube = new Cube(fileName);
            } catch (Exception e) {
                System.err.println("Cannot load cube from file: " + e);
                System.exit(1);
            }
        }
        
        // print cube info
        System.out.println("Searching for solution for cube of size "
                + cube.getSize() + ", twists = " + twists + ", seed = " + seed);
        cube.print(System.out);
        System.out.flush();
        
        System.out.print("Bound now:");
        
        // create port to receive work requests
        ReceivePort receiver = ibis.createReceivePort(requestPortType, WORK_REQ_PORT, this);
        
        receiver.enableConnections();
        receiver.enableMessageUpcalls();
        
        long start = System.currentTimeMillis();
        
        ibis.registry().waitUntilTerminated();
        
        long end = System.currentTimeMillis();
        
        receiver.close();
        
        System.out.println();
        System.out.println("Solving cube possible in " + numBestSolutions + " ways of "
                + bestResult + " steps");
        
        // NOTE: this is printed to standard error! The rest of the output is
        // constant for each set of parameters. Printing this to standard error
        // makes the output of standard out comparable with "diff"
        System.err.println("Solving cube took " + (end - start)
                + " milliseconds");
    }
    
    private Cube requestWork(IbisIdentifier master) throws IOException{
        // Create a send port for sending the request and connect.
        SendPort sendPort = ibis.createSendPort(requestPortType);
        sendPort.connect(master, WORK_REQ_PORT);
        
        // Create a receive port for receiving the reply from the server
        // this receive port does not need a name, as we will send the
        // ReceivePortIdentifier to the server directly
        ReceivePort receivePort = ibis.createReceivePort(replyPortType, null);
        receivePort.enableConnections();
        
        // Send the request message. This message contains the identifier of
        // our receive port so the server knows where to send the reply
        WriteMessage request = sendPort.newMessage();
        request.writeObject(receivePort.identifier());
        request.writeInt(MSG_TYPE_WORK_REQ);
        request.finish();

        // Get cube object from msg
        ReadMessage reply = receivePort.receive();
        Cube workCube = null;
        try {
            workCube = (Cube) reply.readObject();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        reply.finish();

        // Close ports.
        sendPort.close();
        receivePort.close();
        
        return workCube;
    }
    

    private void broadcastResult(int numSolutions, int numSteps) throws IOException {
        SendPort sendPort = ibis.createSendPort(broadcastPortType);
        
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        for (IbisIdentifier joinedIbis : joinedIbises) {
            if(!joinedIbis.equals(ibis.identifier())){
                // broadcast to all joined nodes, except ourselves.
                sendPort.connect(joinedIbis, BROADCAST_PORT);
            }
        }
        
        System.out.println("Sending result broadcast to " + joinedIbises.length + " ibises. #solutions: " + numSolutions + ". numSteps: " + numSteps);
        
        try {
            WriteMessage message = sendPort.newMessage();
            // one way communication, no receiving port waiting for a reply, so send null
            message.writeObject(null);
            
            message.writeInt(MSG_TYPE_RESULT);
            message.writeInt(numSolutions);
            message.writeInt(numSteps);
            message.finish();
        } catch (IOException e) {
            // Nothing to do. Some node left the pool.
        }

        sendPort.close();
    }
    
    public void slave(IbisIdentifier master) throws IOException {
        isMaster = false;
        
        int numSolutions = 0;
        
        while(numSolutions == 0){
            Cube workCube = requestWork(master);
            System.out.println("[" + ibis.identifier().name() + "] Received work. Bound: " + workCube.getBound());
            if(workCube == null){
                // no work to do
                return;
            }
            
            numSolutions = solve(workCube);
            System.out.println("[" + ibis.identifier().name() + "] Bound:  " + workCube.getBound() + ". Number of solutions: " + numSolutions);            
            
            if(numSolutions > 0){
                int numSteps = workCube.getBound();
                broadcastResult(numSolutions, numSteps);
                
                ibis.registry().terminate();
            }
        }
    }

    public void run(String[] args) throws IbisCreationFailedException, IOException{
        // default parameters of puzzle
        int size = 3;
        int twists = 11;
        int seed = 0;
        String fileName = null;

        // number of threads used to solve puzzle
        // (not used in sequential version)

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--size")) {
                i++;
                size = Integer.parseInt(args[i]);
            } else if (args[i].equalsIgnoreCase("--twists")) {
                i++;
                twists = Integer.parseInt(args[i]);
            } else if (args[i].equalsIgnoreCase("--seed")) {
                i++;
                seed = Integer.parseInt(args[i]);
            } else if (args[i].equalsIgnoreCase("--file")) {
                i++;
                fileName = args[i];
            } else if (args[i].equalsIgnoreCase("--help") || args[i].equalsIgnoreCase("-h")) {
                printUsage();
                System.exit(0);
            } else {
                System.err.println("unknown option : " + args[i]);
                printUsage();
                System.exit(1);
            }
        }
        
        ibis = IbisFactory.createIbis(ibisCapabilities, null, requestPortType, replyPortType, broadcastPortType);

        // create broadcast receiver
        ReceivePort broadcastReceiver = ibis.createReceivePort(broadcastPortType, BROADCAST_PORT, this);
        broadcastReceiver.enableConnections();
        broadcastReceiver.enableMessageUpcalls();
        
        IbisIdentifier master = ibis.registry().elect("master");
        
        if (master.equals(ibis.identifier())) {
            master(size, twists, seed, fileName);
        } else {
            slave(master);
        }
        
        broadcastReceiver.close();

        ibis.end();
    }
    
    /**
     * Main function.
     * 
     * @param arguments
     * 
     *            list of arguments
     */
    public static void main(String[] args) {
        try {
            new Rubiks().run(args);
        } catch (IbisCreationFailedException e) {
            System.err.println("Exception while creating Ibis instance");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("IOException while electing a master");
            e.printStackTrace();
        }

    }

    @Override
    public void upcall(ReadMessage msg) throws IOException, ClassNotFoundException {
        ReceivePortIdentifier requestor = (ReceivePortIdentifier) msg.readObject();
        
        if(requestor != null){
            System.out.println("[" + ibis.identifier().name() + "] new msg upcall from " + requestor.ibisIdentifier().name());
        } else {
            System.out.println("[" + ibis.identifier().name() + "] new msg upcall");
        }
        
        int msgType = msg.readInt();
        
        switch(msgType){
            case MSG_TYPE_WORK_REQ:
                msg.finish();
                handleWorkReqMsg(requestor);
                break;
            case MSG_TYPE_RESULT:
                int numSolutions = msg.readInt();
                int numSteps = msg.readInt();
                msg.finish();
                handleResultMsg(numSolutions, numSteps);
                break;
        }
        
        // finish the request message. This MUST be done before sending
        // the reply message. It ALSO means Ibis may now call this upcall
        // method again with the next request message
    }
    
    public synchronized void handleWorkReqMsg(ReceivePortIdentifier requestor) throws IOException{
        System.out.println("[" + ibis.identifier().name() + "] handle work request msg from " + requestor.ibisIdentifier().name());
        
        // create a sendport for the reply
        SendPort replyPort = ibis.createSendPort(replyPortType);

        // connect to the requestor's receive port
        replyPort.connect(requestor);

        // create a reply message
        WriteMessage reply = replyPort.newMessage();

        Cube workCube = getWorkCube();
        reply.writeObject(workCube);
        reply.finish();

        replyPort.close();
        
        System.out.println("[" + ibis.identifier().name() + "] sent work to " + requestor.ibisIdentifier().name());
    }
    
    private Cube getWorkCube() {
        System.out.println("[" + ibis.identifier().name() + "] get work cube");
        
        // only send work if the best result is not optimal yet
        
        synchronized(cube){
            if(bestResult > cube.getBound()+1){
                cube.setBound(cube.getBound()+1);
                System.out.print(" " + cube.getBound());
                
                return cube;
            }
        }

        return null;
    }

    public synchronized void handleResultMsg(int numSolutions, int numSteps) throws IOException{
        System.out.println("[" + ibis.identifier().name() + "] handle result msg");
        
        if(numSteps < bestResult){
            bestResult = numSteps;
            numBestSolutions = numSolutions;
            
            synchronized(cube){
                if(!isMaster & cube.getBound() >= bestResult-1){
                    ibis.registry().terminate();
                }
            }
        }
    }
    
}
