package rubiks.ipl;

import rubiks.ipl.Cube.Twist;
import ibis.ipl.ConnectionFailedException;
import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisConfigurationException;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.RegistryEventHandler;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class Rubiks implements MessageUpcall, RegistryEventHandler {

    private enum LogLevel {
        VERBOSE,        // all info
        DEBUG,          // info useful for debugging
        WARN,           // non critical errors 
        ERROR,          // critical errors only
        NONE
    }
    
    private static final LogLevel LOG_LEVEL     = LogLevel.NONE;
    private static final boolean PRINT_SOLUTION = false;
    
    private static final int MSG_TYPE_WORK_REQ  = 0;
    private static final int MSG_TYPE_RESULT    = 1;
    
    /**
     * Port name and type used for sending a work request to the master
     */
    private static final String WORK_REQ_PORT_NAME = "work_req_port";
    private static final PortType WORK_REQ_PORT_TYPE = new PortType(
            PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, 
            PortType.RECEIVE_AUTO_UPCALLS, PortType.CONNECTION_MANY_TO_ONE);

    /**
     * Port type used for sending a reply back
     */
    private static final PortType REPLY_PORT_TYPE = new PortType(
            PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, 
            PortType.RECEIVE_EXPLICIT, PortType.CONNECTION_ONE_TO_ONE);
    
    /**
     * Port name and type used to broadcast result
     */
    private static final String BROADCAST_PORT_NAME = "broadcast_port";
    private static final PortType BROADCAST_PORT_TYPE = new PortType(
            PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, 
            PortType.RECEIVE_AUTO_UPCALLS, PortType.CONNECTION_ONE_TO_MANY, 
            PortType.CONNECTION_DOWNCALLS);

    /**
     * Ibis Capabilities required to run 
     */
    private static final IbisCapabilities IBIS_CAPABILITIES = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT,
            IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

    /**
     * Number of cubes a worker gets when it asks for work
     */
    private static final int CUBES_PER_WORK_REQ = 9;
    
    /**
     * Initial bound workers will start with
     */
    private static final int INITIAL_BOUND = 5;

    private Ibis ibis;
    private ReceivePort workReqReceiver;
    
    private boolean isMaster;
    
    private final Object queueReadyLock = new Object();
    private boolean queueReady = false;

    private long startMs;

    private LinkedList<Cube[]> workQueue;
    private int numWorkChunks;

    private int numSolutions = 0;
    private int numTwists = Integer.MAX_VALUE;
    private ArrayList<ArrayList<Twist>> solutions;

    private boolean shouldStopWorking = false;
    private boolean printedResult;
    
    private final HashMap<IbisIdentifier, Integer> workers = new HashMap<IbisIdentifier, Integer>();
    private int numExpectedWorkers;
    private int lastPrintedBound;
    private long runtimeMs;
    
    private final Object numWorkersLock = new Object();
    private final Object waitForIbisLock = new Object();
    
    private final ArrayList<IbisIdentifier> joinedIbises = new ArrayList<IbisIdentifier>();
    
    
    @Override
    public void upcall(ReadMessage msg) throws IOException, ClassNotFoundException {
        ReceivePortIdentifier requestor = (ReceivePortIdentifier) msg.readObject();

        int msgType = msg.readInt();

        switch (msgType) {
            case MSG_TYPE_WORK_REQ:
                log(LogLevel.DEBUG, "Received new work req msg", null);
                IbisIdentifier worker = (IbisIdentifier) msg.readObject();
                msg.finish();
                handleWorkReqMsg(requestor, worker);
                break;
            case MSG_TYPE_RESULT:
                log(LogLevel.DEBUG, "Received new result msg", null);
                ArrayList<ArrayList<Twist>> solutions = (ArrayList<ArrayList<Twist>>) msg.readObject();
                msg.finish();
                handleResultMsg(solutions);
                break;
        }
    }
    
    private void handleWorkReqMsg(ReceivePortIdentifier requestor, IbisIdentifier worker) throws IOException{
        // create a sendport for the reply
        SendPort replyPort = ibis.createSendPort(REPLY_PORT_TYPE);

        log(LogLevel.VERBOSE, "Created send port", null);
        
        // connect to the requestor's receive port
        replyPort.connect(requestor);
        
        log(LogLevel.VERBOSE, "Connected requestor", null);

        // create a reply message
        WriteMessage reply = replyPort.newMessage();
        
        log(LogLevel.VERBOSE, "Created WriteMessage", null);

        // msg received by master. Make sure work queue has been created
        synchronized (queueReadyLock){
            while(!queueReady){
                try {
                    queueReadyLock.wait();
                } catch (InterruptedException e) {
                    log(LogLevel.WARN, "Waiting for queue ready was interrupted", e);
                }
            }
        }
        
        Cube[] cubes = getWorkCubes(worker);
        log(LogLevel.VERBOSE, "Number of cubes to be sent to slave: " + cubes.length, null);
        reply.writeArray(cubes);
        reply.finish();

        replyPort.close();
    }
    
    private void handleResultMsg(ArrayList<ArrayList<Twist>> solutions){
        int numSolutions = solutions.size();
        int numTwists = solutions.get(0).size();
        
        if(numTwists < this.numTwists){
            // the received result is better than our result, so replace it
            this.solutions = solutions;
            this.numSolutions = numSolutions;
            this.numTwists = numTwists;
            
            shouldStopWorking = true;
        } else if(numTwists == this.numTwists){
            // the received result is the same as our result, so we might find new solutions
            for(ArrayList<Twist> solution : solutions){
                if(!this.solutions.contains(solution)){
                    this.solutions.add(solution);
                }
            }
            
            numSolutions = this.solutions.size();
        } else {
            // ignore, our result is better
        }
    }
    
    /**
     * Print usage to std out
     */
    private static void printUsage() {
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
    
    /**
     * Print msg to standard error output. If e != null, print stacktrace.
     * If level > LOG_LEVEL, the msg is ignored.
     * 
     * @param msg
     * @param e
     */
    private void log(LogLevel level, String msg, Exception e){
        if(level.ordinal() < LOG_LEVEL.ordinal()){
            return;
        }
        
        String prefix = (ibis != null) ? "[" + ibis.identifier().toString() + "] " : "";
        System.err.println(prefix + msg);
        
        if(e != null){
            e.printStackTrace();
        }
    }
    
    /**
     * Initialize a new cube object
     * 
     * @param size
     * @param twists
     * @param seed
     * @param fileName
     * @return
     * 
     * @throws Exception 
     */
    private Cube initCube(int size, int twists, int seed, String fileName) throws Exception{
        Cube cube = null;

        // create cube
        if (fileName == null) {
            cube = new Cube(size, twists, seed);
        } else {
            cube = new Cube(fileName);
        }
        
        return cube;
    }
    
    /**
     * Print the result
     * 
     * @param numSolutions
     * @param numTwists
     */
    private void printResult(int numSolutions, int numTwists){
        printedResult = true;
        
        runtimeMs = System.currentTimeMillis() - startMs;
        
        System.out.println();
        System.out.println("Solving cube possible in " + numSolutions + " ways of "
                + numTwists + " steps");
        
        // NOTE: this is printed to standard error! The rest of the output is
        // constant for each set of parameters. Printing this to standard error
        // makes the output of standard out comparable with "diff"
        System.err.println("Solving cube took " + runtimeMs + " milliseconds");
    }
    
    /**
     * Generate children and grandchildren based on the given cube,
     * and put them in a work queue. If the result is found, print it immediately.
     * 
     * @param initialCube
     */
    private void createWorkQueue(Cube initialCube){
        workQueue = new LinkedList<Cube[]>();
        CubeCache cache = new CubeCache(initialCube.getSize());
        
        // create children (1 twist from initial cube)
        System.out.print(" 1");
        log(LogLevel.DEBUG, "Generating children (1 twist from orig)", null);
        Cube[] children = initialCube.generateChildren(cache);
        
        
        // create a list of all grandChildren
        ArrayList<Cube> grandChildren = new ArrayList<Cube>();
        
        for(Cube cube : children){
            if(cube.isSolved()){
                // cube is solved in 1 twist
                numSolutions = 1;
                numTwists = 1;
                
                printResult(numSolutions, numTwists);
                
                // make sure no slaves are blocked
                synchronized (queueReadyLock){
                    queueReady = true;
                    queueReadyLock.notifyAll();
                }
                
                return;
            }
            
            grandChildren.addAll(Arrays.asList(cube.generateChildren(cache)));
        }
        
        
        // loop through all grandchildren to see if a solution has been found,
        // and to set the bound correctly
        System.out.print(" 2");
        
        Cube[] cubes = null;
        Cube cube;
        int numGrandChildren = grandChildren.size();
        for(int i=0; i<numGrandChildren; i++){
            // divide work into chunks of work
            if(i % CUBES_PER_WORK_REQ == 0){
                cubes = new Cube[CUBES_PER_WORK_REQ];
            }
            
            cube = grandChildren.get(i);
            
            if(cube.isSolved()){
                // store result if the cube is solveable in 2 twists
                numTwists = 2;
                numSolutions++;
            }

            // set the initial bound to 5, so workers don't ask for work again
            // too fast. This way all workers have a good chance of getting work.
            cube.setBound(INITIAL_BOUND);
            
            cubes[i % CUBES_PER_WORK_REQ] = cube;
            
            // add chunk of work to workQueue (if chunk is full)
            if(i % CUBES_PER_WORK_REQ == CUBES_PER_WORK_REQ-1){
                workQueue.add(cubes);
            }
        }

        if(numTwists == 2){
            // we found a solution already!
            printResult(numSolutions, numTwists);
        } 
        
        numWorkChunks = workQueue.size();
        
        // some slaves might have been waiting until the queue is ready.
        // Wake those workers up!
        synchronized (queueReadyLock){
            queueReady = true;
            queueReadyLock.notifyAll();
        }
    }

    /**
     * Initialize the master, create the initial cube, and create the work queue.
     * @param size
     * @param twists
     * @param seed
     * @param fileName
     * @throws Exception
     */
    private void initMaster(int size, int twists, int seed, String fileName) throws Exception {
        Cube cube = initCube(size, twists, seed, fileName);
        
        // create port to receive work requests
        workReqReceiver = ibis.createReceivePort(WORK_REQ_PORT_TYPE, WORK_REQ_PORT_NAME, this);
        workReqReceiver.enableConnections();
        workReqReceiver.enableMessageUpcalls();
        
        // print cube info
        System.out.println("Searching for solution for cube of size "
                + cube.getSize() + ", twists = " + twists + ", seed = " + seed);
        cube.print(System.out);
        
        System.out.flush();
        System.out.print("Bound now:");
        
        startMs = System.currentTimeMillis();
        
        createWorkQueue(cube);
    }
    
    private synchronized Cube[] getWorkCubes(IbisIdentifier worker){
        // maintain a list of unique workers, and the amount of work they have
        if(!workers.containsKey(worker)){
            workers.put(worker, 0);
            
            int numWorkers = workers.size();
            if(numWorkers > 8){
                numExpectedWorkers = 16;
            } else if(numWorkers > 4){
                numExpectedWorkers = 8;
            } else if(numWorkers > 2){
                numExpectedWorkers = 4;
            } else if(numWorkers > 1){
                numExpectedWorkers = 2;
            } else {
                numExpectedWorkers = 1;
            }
        }
        
        // try to evenly divide the number of work
        int numAllowedChunks = numWorkChunks / numExpectedWorkers;
        log(LogLevel.VERBOSE, "numAllowedChunks: " + numAllowedChunks + ". current num chunks: " + workers.get(worker) + ". Num workers: " + workers.size(), null);
        if(workers.get(worker) >= numAllowedChunks){
            return new Cube[0];
        }
        
        workers.put(worker, workers.get(worker)+1);
        
        if(printedResult){
            // we printed the result already, so no more work to do
            log(LogLevel.VERBOSE, "Already printed result, not returning any work anymore", null);
            return new Cube[0];
        }
        
        try {
            return workQueue.pop();
        } catch (NoSuchElementException e){
            // list is empty
            return new Cube[0];
        }
        
    }
    
    private Cube[] requestWork(IbisIdentifier master) throws IOException{
        if(isMaster){
            // the master holds the queue, so does not need to perform network communication
            return getWorkCubes(ibis.identifier());
        } 
        
        // Create a send port for sending the request and connect.
        SendPort sendPort = ibis.createSendPort(WORK_REQ_PORT_TYPE);
        sendPort.connect(master, WORK_REQ_PORT_NAME);
        
        // Create a receive port for receiving the reply from the server
        // this receive port does not need a name, as we will send the
        // ReceivePortIdentifier to the server directly
        ReceivePort receivePort = ibis.createReceivePort(REPLY_PORT_TYPE, null);
        receivePort.enableConnections();
        
        // Send the request message. This message contains the identifier of
        // our receive port so the server knows where to send the reply
        WriteMessage request = sendPort.newMessage();
        request.writeObject(receivePort.identifier());
        request.writeInt(MSG_TYPE_WORK_REQ);
        request.writeObject(ibis.identifier());
        request.finish();
        
        // Get cube object from msg
        ReadMessage reply = receivePort.receive();
        Cube[] cubes = new Cube[CUBES_PER_WORK_REQ];
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
    
    private void broadcastSolutions(ArrayList<ArrayList<Twist>> solutions) throws IOException{
        log(LogLevel.DEBUG, "Broadcasting solutions. numb solutions: " + solutions.size() 
                + ". Num twists: " + solutions.get(0).size(), null);
        
        if(joinedIbises.size() <= 1){
            log(LogLevel.VERBOSE, "Only one node. Not broadcasting", null);
            return;
        }
        
        SendPort sendPort = ibis.createSendPort(BROADCAST_PORT_TYPE);
        
        for (IbisIdentifier joinedIbis : joinedIbises) {
            if(!joinedIbis.equals(ibis.identifier())){
                // broadcast to all joined nodes, except ourselves.
                try {
                    sendPort.connect(joinedIbis, BROADCAST_PORT_NAME);
                } catch (ConnectionFailedException e) {
                    log(LogLevel.WARN, "Connecting for broadcast failed. Ignoring..", e);
                }
            }
        }
        
        log(LogLevel.DEBUG, "Broadcasting to " + (joinedIbises.size()-1) + " nodes", null);
        
        try {
            WriteMessage message = sendPort.newMessage();
            
            // one way communication, no receiving port waiting for a reply, so send null
            message.writeObject(null);
            
            message.writeInt(MSG_TYPE_RESULT);
            message.writeObject(solutions);
            message.finish();
        } catch (IOException e) {
            // Nothing to do. Some node left the pool.
        }

        sendPort.close();
    }
    
    /**
     * Recursive function to find a solution for a given cube. Only searches to
     * the bound set in the cube object.
     * 
     * @param cube
     * @param cache
     * @return ArrayList of unique solutions
     */
    private ArrayList<ArrayList<Twist>> solutions(Cube cube, CubeCache cache){
        ArrayList<ArrayList<Twist>> solutions = new ArrayList<ArrayList<Twist>>();
        
        if (cube.isSolved()) {
            // return the solution for this cube
            solutions.add(cube.getTwists());
            return solutions;
        }

        if (cube.getNumTwists() >= cube.getBound()) {
            return null;
        }

        // let master print current bound
        if(isMaster && (cube.getNumTwists()+1 > lastPrintedBound)){
            System.out.print(" " + (cube.getNumTwists()+1));
            lastPrintedBound = (cube.getNumTwists()+1);
        }
        
        // generate all possible cubes from this one by twisting it in
        // every possible way. Gets new objects from the cache
        Cube[] children = cube.generateChildren(cache);

        for (Cube child : children) {
            // recursion step
            ArrayList<ArrayList<Twist>> childSolutions = solutions(child, cache);
            if(childSolutions != null){
                for(ArrayList<Twist> solution : childSolutions){
                    if(!solutions.contains(solution)){
                        solutions.add(solution);
                    }
                }
                
                if(childSolutions.size() > 0){
                    if(PRINT_SOLUTION){
                        child.print(System.err);
                    }
                }
            }
            
            // put child object in cache
            cache.put(child);
        }

        return solutions;
    }
    
    /**
     * Solves a Rubik's cube by iteratively searching for solutions with a
     * greater depth. This guarantees the optimal solution is found. Repeats all
     * work for the previous iteration each iteration though...
     * 
     * @param cube
     * @return unique solutions, or an empty ArrayList
     */
    private ArrayList<ArrayList<Twist>> solve(Cube cube){
        // cache used for cube objects. Doing new Cube() for every move
        // overloads the garbage collector
        CubeCache cache = new CubeCache(cube.getSize());
        return solutions(cube, cache);
    }
    
    private void work(IbisIdentifier master){
        ArrayList<ArrayList<Twist>> solutions = new ArrayList<ArrayList<Twist>>();
        
        ArrayList<Cube> allCubes = new ArrayList<Cube>();
        Cube[] cubes = null;
        boolean requestMoreWork = true;
        
        while(!shouldStopWorking){
            // obtain cubes to work with
            if(requestMoreWork){
                try {
                    cubes = requestWork(master);
                    log(LogLevel.VERBOSE, "Received " + cubes.length + " cubes..", null);
                } catch (IOException e){
                    log(LogLevel.ERROR, "Failed getting work", e);
                    return;
                }
                
                if(cubes.length == 0){
                    // we did not receive any work, so don't ask for it next time
                    requestMoreWork = false;
                    
                    log(LogLevel.DEBUG, "Total cubes: " + allCubes.size(), null);
                    
                    if(allCubes.size() == 0){
                        // we did not receive any work at all, so stop working
                        shouldStopWorking = true;
                        continue;
                    } else {
                        cubes = allCubes.toArray(new Cube[allCubes.size()]);
                    }
                } else {
                    allCubes.addAll(Arrays.asList(cubes));
                }
            }
            
            log(LogLevel.VERBOSE, "solutions size: " + solutions.size() 
                    + ". Next bound: " + (cubes[0].getBound()+1), null);
            
            if(solutions.size() > 0){
                log(LogLevel.VERBOSE, "Solution twists: " + solutions.get(0).size(), null);
            }
            
            if(solutions.size() > 0 && cubes[0].getBound()+1 > solutions.get(0).size()){
                numTwists = solutions.get(0).size();
                numSolutions = solutions.size();
                this.solutions = solutions;
                
                // we found the solutions, and have no twists within this bound to explore
                // so we broadcast the solutions, and stop working
                try {
                    broadcastSolutions(solutions);
                } catch (IOException e) {
                    log(LogLevel.ERROR, "Failed broadcasting solutions", e);
                }
                
                return;
            } else if(this.solutions != null && this.solutions.size() > 0 && cubes[0].getBound()+1 > numTwists){
                // we received a result that is better than our result, so stop working
                return;
            }
            
            // find solutions
            for(Cube cube : cubes){
                if(shouldStopWorking){
                    break;
                }
                
                // increase bound on cube
                cube.setBound(cube.getBound()+1);
                
                ArrayList<ArrayList<Twist>> tmpSolutions = solve(cube);
                
                // add all solutions we found so far to the solutions list
                for(ArrayList<Twist> solution : tmpSolutions){
                    if(!solutions.contains(solution)){
                        solutions.add(solution);
                    }
                }
                
            }
        }
    }

    public void run(String args[]){
        // default parameters of puzzle
        int size = 3;
        int twists = 11;
        int seed = 0;
        String fileName = null;

        // parse arguments
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
        
        // create ibis instance 
        try {
            ibis = IbisFactory.createIbis(IBIS_CAPABILITIES, this, WORK_REQ_PORT_TYPE, REPLY_PORT_TYPE, BROADCAST_PORT_TYPE);
            ibis.registry().enableEvents();
            log(LogLevel.VERBOSE, "Created ibis instance", null);
        } catch (IbisCreationFailedException e) {
            log(LogLevel.ERROR, "Creating ibis instance failed", e);
            return;
        } catch (IbisConfigurationException e){
            // this might happen when the master finds a result really fast
            return;
        }
        
        // create broadcast receiver used by all nodes
        ReceivePort broadcastReceiver = null;
        try {
            broadcastReceiver = ibis.createReceivePort(BROADCAST_PORT_TYPE, BROADCAST_PORT_NAME, this);
            broadcastReceiver.enableConnections();
            broadcastReceiver.enableMessageUpcalls();
            log(LogLevel.VERBOSE, "Created broadcast receiver", null);
        } catch (IOException e) {
            log(LogLevel.ERROR, "Creating receive port for broadcast failed", e);
            return;
        }
        
        // elect master, responsible for printing and work distribution
        IbisIdentifier master = null;
        try {
            master = ibis.registry().elect("master");
            isMaster = master.equals(ibis.identifier());
            log(LogLevel.DEBUG, "Master elected: " + master, null);
        } catch (IOException e) {
            log(LogLevel.ERROR, "Electing master failed", e);
            return;
        }
        
        if(isMaster){
            // the master first has to create the work queue
            try {
                initMaster(size, twists, seed, fileName);
                log(LogLevel.VERBOSE, "Initialized master", null);
            } catch (Exception e){
                log(LogLevel.ERROR, e.getMessage(), e);
                return;
            }
            
        }
        
        if(numTwists == Integer.MAX_VALUE){
            // only execute work if we did not find a result yet
            work(master);
            log(LogLevel.VERBOSE, "Stopped working. Found solution: " + printedResult + ". ShouldStopWorking: " + shouldStopWorking, null);
        } 

        if(isMaster){
            if(printedResult && runtimeMs < 1000){
                // found the solution within 1000ms, so give workers another second to connect
                // and realize we finished.
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
            
            synchronized(waitForIbisLock){
                while(joinedIbises.size() > 1){
                    try {
                        waitForIbisLock.wait();
                    } catch (InterruptedException e) {
                    }
                }
            }
        
            if(!printedResult){
                printResult(numSolutions, numTwists);
            }
        }

        // done, so close broadcast receiver and end ibis instance
        try {
            broadcastReceiver.close();
            ibis.end();
        } catch (IOException e){
            log(LogLevel.WARN, "Closing broadcast receiver or ending ibis failed", e);
        }
    }

    public static void main(String args[]){
        new Rubiks().run(args);
    }

    @Override
    public void joined(IbisIdentifier joinedIbis) {
        synchronized (numWorkersLock) {
            joinedIbises.add(joinedIbis);
            
            if(isMaster) log(LogLevel.DEBUG, "New worker. Total: " + joinedIbises.size(), null);
        }
    }

    @Override
    public void left(IbisIdentifier leftIbis) {
        synchronized (numWorkersLock) {
            joinedIbises.remove(leftIbis);
            
            if(isMaster) log(LogLevel.DEBUG, "Worker left. Total: " + joinedIbises.size(), null);
            
            if(isMaster && joinedIbises.size() == 1){
                log(LogLevel.DEBUG, "Last worker left, waking up", null);
                
                // last worker to leave, so notify master
                synchronized (waitForIbisLock) {
                    waitForIbisLock.notifyAll();
                }
            }
        }
    }

    @Override
    public void died(IbisIdentifier corpse) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void gotSignal(String signal, IbisIdentifier source) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void electionResult(String electionName, IbisIdentifier winner) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void poolClosed() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void poolTerminated(IbisIdentifier source) {
        // TODO Auto-generated method stub
        
    }
}
