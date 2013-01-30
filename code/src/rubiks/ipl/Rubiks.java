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

    private long start;

    private int currentBound;
    
    public static final boolean PRINT_SOLUTION = false;
    
    private static final String BROADCAST_PORT = "broadcast_port";
    private static final String SEQ_NUM = "seq_num";

    
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
    
    
    public void doWork(int size, int twists, int seed, String fileName) throws IOException{
        cube = initCube(size, twists, seed, fileName);
        
        if(isMaster){
            // print cube info
            System.out.println("Searching for solution for cube of size "
                    + cube.getSize() + ", twists = " + twists + ", seed = " + seed);
            cube.print(System.out);
            System.out.flush();
            
            System.out.print("Bound now:");
            
            start = System.currentTimeMillis();
        }
        
        if(isMaster){
            System.out.print(" 1");
        }
        
        CubeCache cache = new CubeCache(cube.getSize());
        Cube[] children = cube.generateChildren(cache);
        Cube[] grandChildren = new Cube[children.length * children.length];
        
        if(isMaster){
            System.out.print(" 2");
        }
        
        // get a list of all grand children. 
        int numGrandChildren = 0;
        for(Cube child : children){
            System.arraycopy(child.generateChildren(cache), 0, grandChildren, numGrandChildren, children.length);
            numGrandChildren += children.length;
        }
        
        
        int numNodes = getNumNodes();
        
        // calculate the number of roots each node will handle
        int numRoots = numGrandChildren / numNodes;
        int seqNum = new Long(ibis.registry().getSequenceNumber(SEQ_NUM)).intValue();
        
        System.out.println("[" + ibis.identifier() + "] My seqNum: " + seqNum + ". numRoots: " + numRoots);
        
        Cube[] cubes = new Cube[numRoots];
        System.arraycopy(grandChildren, seqNum * numRoots, cubes, 0, numRoots);
        // at this point cubes[] contains all the sub roots of the tree this node will work with
        
        int numSolutions;
        
        while(!shouldStopWorking){
            if(isMaster && cube.getBound() >= 2){
                System.out.print(" " + (cube.getBound()+1));
            }
            
            for(Cube cube : cubes){
                if(cube.getBound() == 0){
                    // we did two twists (one to generate children, one for grand children),
                    // so set bound accordingly
                    cube.setBound(2);
                }
                
                cube.setBound(cube.getBound()+1);
                currentBound = cube.getBound();
                
                numSolutions = solve(cube);
                
                if(numSolutions > 0){
                    System.out.println("[" + ibis.identifier() + "] Num solutions: " + numSolutions);
                    
                    int numSteps = cube.getBound();
                    broadcastResult(numSolutions, numSteps);
                    
                    shouldStopWorking = true;
                }
            }
        }
        
        if(isMaster){
            computeResults();
        } else {
            terminate();
        }
        
    }
    
    private int getNumNodes(){
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        System.out.println("[" + ibis.identifier() + "] num nodes: " + joinedIbises.length);
        return joinedIbises.length;
    }
    
    private void broadcastResult(int numSolutions, int numSteps) throws IOException {
        System.out.println("[" + ibis.identifier() + "] Broadcasting result. numSolutions: " + numSolutions + ". numTwists: " + numSteps);
        if(getNumNodes() <= 1){
            System.out.println("[" + ibis.identifier() + "] Only one node. Not broadcasting");
            return;
        }
        
        SendPort sendPort = ibis.createSendPort(broadcastPortType);
        
        IbisIdentifier[] joinedIbises = ibis.registry().joinedIbises();
        for (IbisIdentifier joinedIbis : joinedIbises) {
            if(!joinedIbis.equals(ibis.identifier())){
                // broadcast to all joined nodes, except ourselves.
                sendPort.connect(joinedIbis, BROADCAST_PORT);
            }
        }
        
        System.out.println("[" + ibis.identifier() + "] Broadcasting to " + (joinedIbises.length-1) + " nodes");
        
        try {
            WriteMessage message = sendPort.newMessage();
            message.writeInt(numSolutions);
            message.writeInt(numSteps);
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
        
        ibis = IbisFactory.createIbis(ibisCapabilities, null, broadcastPortType);

        // create broadcast receiver
        ReceivePort broadcastReceiver = ibis.createReceivePort(broadcastPortType, BROADCAST_PORT, this);
        broadcastReceiver.enableConnections();
        broadcastReceiver.enableMessageUpcalls();
        
        IbisIdentifier master = ibis.registry().elect("master");
        
        isMaster = master.equals(ibis.identifier());
        doWork(size, twists, seed, fileName);
        
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
        System.out.println("Received new result msg");
        int numSolutions = msg.readInt();
        int numSteps = msg.readInt();
        msg.finish();
        handleResultMsg(numSolutions, numSteps);
    }
    
    public synchronized void handleResultMsg(int numSolutions, int numSteps) throws IOException{
        if(cube == null){
            // it is possible that the cube is not initialized yet
            terminate();
            return;
        }
        
        if(numSteps < bestResult){
            System.out.println("[" + ibis.identifier() + "] Result is better than mine");
            bestResult = numSteps;
            numBestSolutions = numSolutions;
            
            if(currentBound >= bestResult){
                System.out.println("[" + ibis.identifier() + "] My bound is larger or equal to the best result");
                shouldStopWorking = true; 
                
                if(!isMaster){
                    System.out.println("[" + ibis.identifier() + "] Not a master, terminating");
                    // slave simply terminates at this point
                    terminate();
                } else {
                    computeResults();
                }
            }
            
        }
        
    }
    
    private void computeResults(){
        System.out.println("[" + ibis.identifier() + "] I'm the master. Waiting for slaves to terminate");
        // master is in charge of printing final result
        
        // wait until all other processes have terminated
        if(getNumNodes() > 1){
            ibis.registry().waitUntilTerminated();
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
    
}
