package rubiks.ipl;

import ibis.ipl.AlreadyConnectedException;
import ibis.ipl.ConnectionFailedException;
import ibis.ipl.ConnectionRefusedException;
import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortMismatchException;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * Solver for rubik's cube puzzle.
 * 
 * @author Niels Drost, Timo van Kessel
 * 
 */
public class Rubiks implements MessageUpcall {
    
    public static final boolean PRINT_SOLUTION  = false;
    private static final boolean DEBUG          = false;
    
    private static final String WORK_REQ_PORT   = "work_req_port";
    private static final String BROADCAST_PORT  = "broadcast_port";
    private static final String SEQ_NUM         = "seq_num";

    private static final int MSG_TYPE_WORK_REQ  = 0;
    private static final int MSG_TYPE_RESULT    = 1;
    
    private static final int CUBES_PER_REQ = 9; // each worker solves 9 subroots of the tree
    private static final int INITIAL_BOUND = 5;
    
    private boolean queueReady = false;
    private Object queueLock;
    
    
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
    private int numBestSolutions = 0;

    private long start;

    private int currentBound;

    private LinkedList<Cube[]> workQueue; // work queue: array of 9 cubes to be solved
    private ReceivePort receiver;

    
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
    
    private Cube initCube(int size, int twists, int seed, String fileName){
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
        
        return cube;
    }
    
    public void createWorkQueue(int size, int twists, int seed, String fileName) throws IOException{
        queueLock = new Object();
        
        cube = initCube(size, twists, seed, fileName);

        // create port to receive work requests
        receiver = ibis.createReceivePort(requestPortType, WORK_REQ_PORT, this);
        
        receiver.enableConnections();
        receiver.enableMessageUpcalls();
        
        // print cube info
        System.out.println("Searching for solution for cube of size "
                + cube.getSize() + ", twists = " + twists + ", seed = " + seed);
        cube.print(System.out);
        System.out.flush();
        
        System.out.print("Bound now:");
        
        start = System.currentTimeMillis();
        
        System.out.print(" 1");
        
        // generate work queue
        CubeCache cache = new CubeCache(cube.getSize());
        Cube[] children = cube.generateChildren(cache);
        Cube[] grandChildren = new Cube[children.length * children.length];
        workQueue = new LinkedList<Cube[]>();
        
        // get a list of all grand children. 
        int numGrandChildren = 0;
        for(Cube child : children){
            if(child.isSolved()){
                // we did one twists. Therefore there can be only one solution
                bestResult = 1;
                numBestSolutions = 1;
                computeResults();
                return;
            }
            
            System.arraycopy(child.generateChildren(cache), 0, grandChildren, numGrandChildren, children.length);
            numGrandChildren += children.length;
        }
        
        System.out.print(" 2");
        
        Cube[] cubes = null;
        for(int i=0; i<numGrandChildren; i++){
            if(i % CUBES_PER_REQ == 0){
                cubes = new Cube[CUBES_PER_REQ];
            }
            
            if(grandChildren[i].isSolved()){
                bestResult = 2;
                numBestSolutions++;
            }
            
            // set bound to 5. This way each worker takes some time computing before
            // requesting more work, giving all workers a fair chance of obtaining work
            // (even if some might arrive later)
            grandChildren[i].setBound(INITIAL_BOUND);
            
            cubes[i % CUBES_PER_REQ] = grandChildren[i];
            
            if(i % CUBES_PER_REQ == CUBES_PER_REQ-1){
                workQueue.add(cubes);
            }
        }
        
        if(numBestSolutions > 0){
            computeResults();
            return;
        }
        
        synchronized (queueLock){
            queueReady = true;
            queueLock.notifyAll();
        }
    }
    
    
    public void doWork(int size, int twists, int seed, String fileName, IbisIdentifier master) throws IOException{
        int numSolutions;
        
        ArrayList<Cube> allCubes = new ArrayList<Cube>();
        Cube[] cubes = null;
        boolean reqMoreWork = true;
        
        int prevBound = 2;
        
        while(!shouldStopWorking){
            if(reqMoreWork){
                // only request extra work if prev call succeeded
                cubes = requestWork(master);
                
                if(cubes == null || cubes.length == 0){
                    // did not receive extra work. Continue with current cubes, increasing their bounds
                    
                    reqMoreWork = false;
                    
                    debug("Im working with " + allCubes.size() + " cubes");
                    
                    if(allCubes.size() == 0){
                        break;
                    }
                    
                    cubes = allCubes.toArray(new Cube[allCubes.size()]);
                    
                }
            }
            
            if(isMaster && prevBound < cubes[0].getBound()+1){
                if(prevBound == 2 && cubes[0].getBound() == INITIAL_BOUND){
                    System.out.print(" 3 4 5");
                }
                System.out.print(" " + (cubes[0].getBound()+1));
                prevBound = cubes[0].getBound()+1;
            }
            
            for(Cube cube : cubes){
                cube.setBound(cube.getBound()+1);
                currentBound = cube.getBound();
                
                numSolutions = solve(cube);
                
                if(numSolutions > 0 && currentBound < bestResult){
                    debug("Num solutions: " + numSolutions);
                    
                    bestResult = currentBound;
                    numBestSolutions = numSolutions;
                    
                    broadcastResult(numSolutions, currentBound);
                    
                    shouldStopWorking = true;
                    break;
                }
                
                if(reqMoreWork){
                    // add cube to all cubes, only if it is a new cube
                    allCubes.add(cube);
                }
            }
        }
        
        if(isMaster){
            computeResults();
            if(bestResult < Integer.MAX_VALUE){
                // compute results if we found any
                ibis.registry().waitUntilTerminated();
            }
            
        } else {
            terminate();
        }
        
    }
    
    private Cube[] requestWork(IbisIdentifier master) throws IOException{
        if(isMaster){
            // the master holds the queue, so does not need to perform network communication
            return getWorkCubes();
        } 
        
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
        Cube[] cubes = new Cube[CUBES_PER_REQ];
        try {
            cubes = (Cube[]) reply.readObject();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        reply.finish();
        
        // Close ports.
        sendPort.close();
        receivePort.close();
        
        return cubes;
    }
    
    private synchronized Cube[] getWorkCubes(){
        try {
            return workQueue.pop();
        } catch (NoSuchElementException e){
            // list is empty
            return new Cube[0];
        }
        
    }
    
    private void broadcastResult(int numSolutions, int numTwists) throws IOException {
        debug("Broadcasting result. numSolutions: " + numSolutions + ". numTwists: " + numTwists);
        SendPort sendPort = ibis.createSendPort(broadcastPortType);
        
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        if(joinedIbises.length <= 1){
            debug("Only one node. Not broadcasting");
            return;
        }
        
        for (IbisIdentifier joinedIbis : joinedIbises) {
            if(!joinedIbis.equals(ibis.identifier())){
                // broadcast to all joined nodes, except ourselves.
                
                try {
                    sendPort.connect(joinedIbis, BROADCAST_PORT);
                } catch (ConnectionRefusedException e){
                    //  receiver denies the connection. ignore
                } catch (AlreadyConnectedException e){
                    // port was already connected to the receiver. ignore
                } catch (PortMismatchException e){
                    // receiveport port and the sendport are of different types. will (should) not occur
                } catch (ConnectionFailedException e){
                    // something else went wrong. ignore
                }
            }
        }
        
        debug("Broadcasting to " + (joinedIbises.length-1) + " nodes");
        
        try {
            WriteMessage message = sendPort.newMessage();
            
            // one way communication, no receiving port waiting for a reply, so send null
            message.writeObject(null);
            
            message.writeInt(MSG_TYPE_RESULT);
            message.writeInt(numSolutions);
            message.writeInt(numTwists);
            message.finish();
        } catch (IOException e) {
            // Nothing to do. Some node left the pool.
        }

        sendPort.close();
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
        
        isMaster = master.equals(ibis.identifier());
        if(isMaster){
            createWorkQueue(size, twists, seed, fileName);
            
            // master waits until first slave requests work. This way the master
            // does not hijack all the work before the slaves have time to ask for it
            synchronized(this){
                try {
                    wait(500);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        
        doWork(size, twists, seed, fileName, master);
        
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
        } catch (Exception e) {
            // ignore all exceptions (some might arise when the master finds
            // the result very fast, and slaves try to connect after that.
        } 

    }

    @Override
    public void upcall(ReadMessage msg) throws IOException, ClassNotFoundException {
        ReceivePortIdentifier requestor = (ReceivePortIdentifier) msg.readObject();

        int msgType = msg.readInt();

        switch (msgType) {
            case MSG_TYPE_WORK_REQ:
                debug("Received new work req msg");
                msg.finish();
                handleWorkReqMsg(requestor);
                break;
            case MSG_TYPE_RESULT:
                debug("Received new result msg");
                int numSolutions = msg.readInt();
                int numSteps = msg.readInt();
                msg.finish();
                handleResultMsg(numSolutions, numSteps);
                break;
        }
    }
    
    public synchronized void handleWorkReqMsg(ReceivePortIdentifier requestor) throws IOException{
        notifyAll();
        
        // create a sendport for the reply
        SendPort replyPort = ibis.createSendPort(replyPortType);

        // connect to the requestor's receive port
        replyPort.connect(requestor);

        // create a reply message
        WriteMessage reply = replyPort.newMessage();

        // msg received by master. Make sure work queue has been created
        synchronized (queueLock){
            while(!queueReady){
                try {
                    queueLock.wait();
                } catch (InterruptedException e) {
                }
            }
        }
        
        Cube[] cubes = getWorkCubes();
        reply.writeArray(cubes);
        reply.finish();

        replyPort.close();
    }
    
    public synchronized void handleResultMsg(int numSolutions, int numTwists) throws IOException{
        if(cube == null){
            // it is possible that the cube is not initialized yet
            terminate();
            return;
        }
        
        if(numTwists < bestResult){
            debug("Result is better than mine");
            bestResult = numTwists;
            numBestSolutions = numSolutions;
            
            if(currentBound >= bestResult){
                debug("My bound is larger or equal to the best result");
                shouldStopWorking = true; 
                
                if(!isMaster){
                    debug("Not a master, terminating");
                    // slave simply terminates at this point
                    terminate();
                } else {
                    computeResults();
                }
            }
            
        }
        
    }
    
    private synchronized void computeResults(){
//        debug("I'm the master. Waiting for slaves to terminate");
        // master is in charge of printing final result
        
        try {
            ibis.registry().terminate();
            ibis.end();
            
            if(receiver != null){
                receiver.close();
            }
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        long end = System.currentTimeMillis();
        
        System.out.println();
        System.out.println("Solving cube possible in " + numBestSolutions + " ways of "
                + bestResult + " steps");
        
        // NOTE: this is printed to standard error! The rest of the output is
        // constant for each set of parameters. Printing this to standard error
        // makes the output of standard out comparable with "diff"
        System.err.println("Solving cube took " + (end - start)
                + " milliseconds");
        
        System.exit(1);
    }
    
    private void terminate(){
        try {
            ibis.registry().terminate();
            ibis.end();
        } catch (IOException e) {
            // do nothing. 
        }
        
        System.exit(1);
    }
    
    private void debug(String str){
        if(DEBUG) System.out.println("[" + ibis.identifier() + "] " + str);
    }
    
}
