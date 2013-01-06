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
            PortType.SERIALIZATION_DATA, PortType.RECEIVE_EXPLICIT,
            PortType.CONNECTION_ONE_TO_ONE);
    
    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT);

    private Ibis ibis;

    private Cube cube;
    
    public static final boolean PRINT_SOLUTION = false;
    private static final String WORK_REQ_PORT = "work_req_port";

    
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

        int result = 0;

        for (Cube child : children) {
            // recursion step
            int childSolutions = solutions(child, cache);
            if (childSolutions > 0) {
                result += childSolutions;
                if (PRINT_SOLUTION) {
                    child.print(System.err);
                }
            }
            // put child object in cache
            cache.put(child);
        }

        return result;
    }

    /**
     * Solves a Rubik's cube by iteratively searching for solutions with a
     * greater depth. This guarantees the optimal solution is found. Repeats all
     * work for the previous iteration each iteration though...
     * 
     * @param cube
     *            the cube to solve
     */
    private static void solve(Cube cube) {
        // cache used for cube objects. Doing new Cube() for every move
        // overloads the garbage collector
        CubeCache cache = new CubeCache(cube.getSize());
        int bound = 0;
        int result = 0;

        System.out.print("Bound now:");

        while (result == 0) {
            bound++;
            cube.setBound(bound);

            System.out.print(" " + bound);
            result = solutions(cube, cache);
        }

        System.out.println();
        System.out.println("Solving cube possible in " + result + " ways of "
                + bound + " steps");
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
        System.out.println("I am the master");
        
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
        
        // create port to receive work requests
        ReceivePort receiver = ibis.createReceivePort(requestPortType, WORK_REQ_PORT, this);
        
        receiver.enableConnections();
        receiver.enableMessageUpcalls();
        
        // solve
        long start = System.currentTimeMillis();
//        solve(cube);
        long end = System.currentTimeMillis();

        receiver.close();
        
        // NOTE: this is printed to standard error! The rest of the output is
        // constant for each set of parameters. Printing this to standard error
        // makes the output of standard out comparable with "diff"
        System.err.println("Solving cube took " + (end - start)
                + " milliseconds");
    }
    
    public void slave(IbisIdentifier master) throws IOException {
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
        request.finish();

        // Get cube object from msg
        ReadMessage reply = receivePort.receive();
        Cube workCube;
        try {
            workCube = (Cube) reply.readObject();
            System.out.println("Got work to do. Bound: " + workCube.getBound());
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        reply.finish();

        // Close ports.
        sendPort.close();
        receivePort.close();
        
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
        
        ibis = IbisFactory.createIbis(ibisCapabilities, null, requestPortType, replyPortType);
        IbisIdentifier master = ibis.registry().elect("master");
        
        if (master.equals(ibis.identifier())) {
            master(size, twists, seed, fileName);
        } else {
            slave(master);
        }

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
        
        // finish the request message. This MUST be done before sending
        // the reply message. It ALSO means Ibis may now call this upcall
        // method agian with the next request message
        msg.finish();
        
        // create a sendport for the reply
        SendPort replyPort = ibis.createSendPort(replyPortType);

        // connect to the requestor's receive port
        replyPort.connect(requestor);

        // create a reply message
        WriteMessage reply = replyPort.newMessage();
        
        synchronized (cube) {
            cube.setBound(cube.getBound()+1);
            reply.writeObject(cube);
            reply.finish();
        }

        replyPort.close();
    }

}
