package Test;

import Serializers.Tuple;
import Worker.Worker;

import java.time.Duration;
import java.util.*;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class WorkerTest {

    private final ManagedMessagingService channel;
    private final Serializer s;

    private WorkerTest(Address myAddr, BiConsumer<Address, Tuple> handler) throws ExecutionException, InterruptedException {

        s = new SerializerBuilder()
                .addType(Serializers.Tuple.Type.class)
                .addType(Serializers.Tuple.Request.class)
                .addType(Tuple.class)
                .build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( myAddr )
                .build();

        //this.channel.registerHandler("RETRY", (o,m) -> { handler.accept(o, s.decode(m)); }, executorService);

        this.channel.start().get();
    }

    private void sendRoolback(int id, Address addr){
        Tuple t = new Tuple(1,null, Tuple.Type.ROLLBACK, Tuple.Request.CANCEL,id);
        channel.sendAsync(addr, "CONFIRM", s.encode( t ));
    }

    private void sendCommit(int id, Address addr){
        Tuple t = new Tuple(1,null, Tuple.Type.COMMIT, Tuple.Request.GET, id);
        channel.sendAsync(addr, "CONFIRM", s.encode( t ));
    }

    private Tuple sendPreparedGET(int id, Long k, Address addr) throws ExecutionException, InterruptedException {
        Tuple t = new Tuple(k,null, Tuple.Type.PREPARED, Tuple.Request.GET, id);
        byte[] b = channel.sendAndReceive(addr, "PREPARE", s.encode(t), TIMEOUT).get();
        return s.decode(b);
    }

    private Tuple sendPreparedPUT(int id, Long k, byte[] v, Address addr) throws ExecutionException, InterruptedException {
        Tuple t = new Tuple( k, v, Tuple.Type.PREPARED, Tuple.Request.PUT, id);
        byte[] b = channel.sendAndReceive(addr, "PREPARE", s.encode(t)).get();
        return s.decode( b);
    }

    private static final Duration TIMEOUT= Duration.ofMillis(6000) ;  //   > 700
    private static final int numWorker = 3;

    private static Address[] addrWorkers;
    private static WorkerTest test;
    private static Map< Integer, List<Tuple> > results;
/*
    public static void  main(String[] args) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////
        results = new HashMap<>();

        test = new WorkerTest(Address.from("localhost:12345"), (o, t) ->{
            addToResults(t, o);
            System.out.println("[MAIN] <==  [W" + getId(o) + "]: " + t.toString());
        });


        addrWorkers = new Address[numWorker];
        Worker[] workers = new Worker[numWorker];

        for(int i = 0; i < numWorker; i ++ ){
            Address addr = Address.from( String.format("localhost:11%03d", i));

            addrWorkers[i] = addr;
            workers[i] = new Worker(i, addr);

        }

        /////////////////  TESTS  /////////////////

        Thread.sleep(1000);

        System.out.println( "##### Test0 #####");
        int result = test0();
        System.out.println( "#### " + ( result == 0 ? "DONE" :"ERROR (" + result +")" ) + " ####" );
        if( result > 0 ) return ;

        System.out.println( "##### Test1 #####");
        result = test1();
        System.out.println( "#### " + ( result == 0 ? "DONE" :"ERROR (" + result +")" ) + " ####" );
        if( result > 0 ) return ;

        System.out.println( "##### Test2 #####");
        result = test2();
        System.out.println( "#### " + ( result == 0 ? "DONE" :"ERROR (" + result +")" ) + " ####" );
        if( result > 0 ) return ;

        System.out.println( "##### Test3 #####");
        result = test3();
        System.out.println( "#### " + ( result == 0 ? "DONE" :"ERROR (" + result +")" ) + " ####" );
        if( result > 0 ) return ;


        System.out.println( "##### Test4 #####");
        result = test4();
        System.out.println( "#### " + ( result == 0 ? "DONE" :"ERROR (" + result +")" ) + " ####" );
        if( result > 0 ) return ;

        System.out.println("\n\nAll tests done successfully :D ");
    }

    private static int test0() {

        results.clear();

        Tuple t = null;

        try {
            t = test.sendPreparedPUT(0,8L, "ola".getBytes(), addrWorkers[0]);
        } catch (Exception e) {
            return 1;
        }

        if( ! t.getMsg().equals(Tuple.Type.OK))
            return 2;

        test.sendCommit(0, addrWorkers[0]);

        return 0;
    }

    private static int test1() {
        // Test a simple put, commit and get

        if( numWorker < 3) return 1;
        Tuple t1,t2,t3;

        try {
            t1 = test.sendPreparedPUT(1,1L, "ola".getBytes(), addrWorkers[0]);
            t2 = test.sendPreparedPUT(1,2L, "mundo".getBytes(), addrWorkers[1]);
            t3 = test.sendPreparedPUT(1,3L, "lindo".getBytes(), addrWorkers[2]);
        } catch (Exception e) {
            return 1;
        }

        if( ! t1.getMsg().equals(Tuple.Type.OK) || ! t2.getMsg().equals(Tuple.Type.OK) || ! t3.getMsg().equals(Tuple.Type.OK) )
            return 2;

        test.sendCommit(1, addrWorkers[0]);
        test.sendCommit(1, addrWorkers[1]);
        test.sendCommit(1, addrWorkers[2]);

        try {
            t1 = test.sendPreparedGET(2,1L, addrWorkers[0]);
            t2 = test.sendPreparedGET(2,2L, addrWorkers[1]);
            t3 = test.sendPreparedGET(2,3L, addrWorkers[2]);
        } catch (Exception e) {
            return 3;
        }

        if( ! t1.getMsg().equals(Tuple.Type.OK) || ! t2.getMsg().equals(Tuple.Type.OK) || ! t3.getMsg().equals(Tuple.Type.OK) )
            return 4;

        if( ! Arrays.equals(t1.getValue(), "ola".getBytes()) || !Arrays.equals(t2.getValue(), "mundo".getBytes()) || !Arrays.equals(t3.getValue(), "lindo".getBytes()))
            return 5;

        test.sendCommit(2, addrWorkers[0]);
        test.sendCommit(2, addrWorkers[1]);
        test.sendCommit(2, addrWorkers[2]);

        return 0;
    }

    private static int test2() {
        // test a  get of nonexistent value

        if( numWorker < 1 )
            return 1;

        Tuple t1;

        try {
            t1 = test.sendPreparedGET(3, 20L, addrWorkers[0] );
        } catch ( Exception e) {
            return 2;
        }

        return t1.getMsg().equals( Tuple.Type.ROLLBACK) ? 0 : 3;
    }

    private static int test3() {
        // test a lock of value
        // make a put without a commit or rollback and send a second put with same key

        results.clear();

        if( numWorker < 1)
            return 1;

        try {
            test.sendPreparedPUT(4,1L, "ola mundo".getBytes(), addrWorkers[0]);
        } catch (Exception e) {
            return 2;
        }

        AtomicReference<Boolean> flag = new AtomicReference<>();

        flag.set( false );

        Thread th = new Thread(() -> {
            try {
                test.sendPreparedPUT(4,1L, "ola mundo lindo".getBytes(), addrWorkers[0]);
                flag.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        th.start();

        try {
            Thread.sleep( 500 );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if( flag.getAndSet(true) ) {
            return 3;
        }
        test.sendCommit(4, addrWorkers[0]);

        test.sendCommit(5, addrWorkers[0]);

        return 0;
    }

    private static int test4() {
        // test a roolback after a put and confirm with a get

        results.clear();

        try {
            test.sendPreparedPUT(6, 5L, "ola".getBytes(), addrWorkers[0]);
        } catch ( Exception e) {
            return 1;
        }

        test.sendRoolback( 6, addrWorkers[0]);

        Tuple t1 ;

        try {
            t1 = test.sendPreparedGET( 7, 5L , addrWorkers[0]);
        } catch (Exception e) {
            return 2;
        }

        return t1.getMsg().equals(Tuple.Type.ROLLBACK)  ?  0 : 3;
    }

    private static int getId(Address addr){

        for( int i = 0; i < numWorker; i ++ )
            if( addrWorkers[i].equals( addr))
                return i;

        return -1;
    }

    private static void addToResults(Tuple t, Address o) {

        if( ! results.containsKey( getId(o)) )
            results.put( getId(o), new ArrayList<>());
        results.get( getId(o) ).add(t);

    }
*/
}
