package edu.utexas.atallah.idgen;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *  GlobalId
 *
 *  This class can be used to generate globally unique ids at a rate of at least 100K ids per second
 *  on each processing node (JVM) that it runs on, up to 1024 individual nodes.  It is thread safe and
 *  guarantees that IDs given to callers will be globally unique (i.e. never issued more than one time).
 *  The format of the IDs is as follows (which callers should consider to be opaque) is as follows:
 *
 *      63       53       47       39       31       23       15       7
 *      +--------+--------+--------+--------+--------+--------+--------+--------+
 *      |  NodeId |          SecondsSinceEpoch (36)         |    SerialNum (17) |
 *      +--------+--------+--------+--------+--------+--------+--------+--------+
 *  Here the NodeId is is 11 bits in length (with the MSB always set to zero) to ensure that the resulting
 *  IDs are positive 64-bit integers (Java does not have a native unit64 type).  The SecondsSinceEpoch field
 *  is the conventional one from Jan 1970.
 *
 *  Implementation Notes
 *
 * 1) We can guarantee that the returned ID is unique because no more than 2**17 IDs
 *    will be issued by each node each second and they will be issued in serial fashion
 *    within that second.  If a node fails and restarts at any time, it is guaranteed that
 *    at least one second will pass so that ids which have never been issued before will
 *    be used (since SecondsSinceEpoch will be different).
 * 2) Lack of a persistence layer helps this solution run at a high rate of speed and unit
 *    tests ensure that the single node performance is at least 100K operations per second.
 * 3) Intrinsic properties are used to help guarantee correctness.  Because the IDs issued
 *    by each node are guaranteed to be unique we can reduce the problem to ensuring the
 *    lower 53 bits are unique within one node.  Because the SerialNum is monotonically
 *    increasing within each epoch second, we can guarantee that the unique ids are too.
 *    a) Crashes and Restarts
 *       When a node crashes the initialization code will ensure that it waits one
 *       second before coming into service.  This ensures that it will not be assigning
 *       any unique ids from the last batch it was handing out before the crash.
 *    b) System Failure and Restart
 *       This is handled in the same way as single node failures.  Because the seconds
 *       since the epoch value is guaranteed to have increased by 1 second old identifiers
 *       will never be given out.
 *    c) Software Defects
 *       Having proper tests is the first line of defense here, but one of the biggest
 *       defects that could occur would be during configuration if two nodes have the
 *       same node id.  One solution to that would be to use any number of techniques to
 *       have nodes broadcast their id and see if anyone else has it before assuming it's
 *       free.
 */
public class GlobalId {
    private static final long NODE_ID_BITS = 10;
    private static final long SERIAL_NUMBER_BITS = 17;
    private static final long MAX_SERIAL_NUMBER = (1<<SERIAL_NUMBER_BITS)-1;
    private static long ONE_SECOND = 1000;
    public static final long DEFAULT_NODE_ID = 1023;

    private static int nextSerialNumber = 0;        // Next serial number to be assigned (within the current second)
    private static long lastIntervalStart = 0;      // Time at which last one second interval started (msec)
    private static long lastIdTimestamp;            // Time at which last ID was issued
    private static long nodeId;

    private static final Logger log = LoggerFactory.getLogger(GlobalId.class);

    public static void init() {
        nodeId = nodeId();
        /*
         *  Ensure that each node waits at least one second before issuing new ids to prevent overlaps
         *  in case of a node bounce.
         */
        sleep(1000, "initial startup");
        log.info("GlobalId manager for node {} initialized", nodeId());
    }

    private static void sleep(long delay, String banner) {
        try {
            log.debug(String.format("Sleeping for %d msec (%s)", delay, banner));
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                    String.format("Interrupted during sleep [%s]",banner));
        }
    }

    /**
     * <code>getId</code> returns the next available globally unique id.  Although it should not be called
     * more often than 100K times/second, it will not fail should that occur.  Instead it will sleep briefly
     * (no more than 1 sec) and then return an id.
     * @return The next globally unique id as a 64 bit integer
     */

    public static synchronized long getId() {
        long currTimestamp = timestamp();
        if (nextSerialNumber >= MAX_SERIAL_NUMBER) {
            long sleepDelay = ONE_SECOND - (currTimestamp - lastIntervalStart);
            sleep(sleepDelay, "overflow");
            currTimestamp = timestamp();
            lastIntervalStart = currTimestamp;
            nextSerialNumber = 0;
        } else if (currTimestamp - lastIdTimestamp > ONE_SECOND) {
            lastIntervalStart = currTimestamp;
            nextSerialNumber = 0;
        }
        lastIdTimestamp = currTimestamp;
        return nodeId << (64-(NODE_ID_BITS+1)) |
                (currTimestamp/ONE_SECOND) << SERIAL_NUMBER_BITS |
                nextSerialNumber++;
    }

    public static long nodeId() {
        // Hardcoded for proof of concept
        return DEFAULT_NODE_ID;
    }
    public static long timestamp() {
        return System.currentTimeMillis();
    }
}
