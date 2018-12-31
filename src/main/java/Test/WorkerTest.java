package Test;

import Serializers.Tuple;
import Worker.Worker;

import java.util.*;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class WorkerTest {

    private final ManagedMessagingService channel;
    private final ExecutorService executorService;
    private final Serializer s;

    WorkerTest(Address myAddr, BiConsumer<Address,Tuple> handler) throws ExecutionException, InterruptedException {

        s = new SerializerBuilder()
                .addType(Serializers.Tuple.Type.class)
                .addType(Serializers.Tuple.Request.class)
                .addType(Tuple.class)
                .build();

        this.executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( myAddr )
                .build();

        this.channel.registerHandler("Tuple", (o,m) -> { handler.accept(o, s.decode(m)); } , executorService);

        this.channel.start().get();
    }

    void sendRoolback(int id, Address addr){
        Tuple t = new Tuple(1,null, Tuple.Type.ROLLBACK, Tuple.Request.CANCEL,id);
        channel.sendAsync(addr, "Tuple", s.encode( t ));
    }

    void sendCommit(int id, Address addr){
        Tuple t = new Tuple(1,null, Tuple.Type.COMMIT, Tuple.Request.GET, id);
        channel.sendAsync(addr, "Tuple", s.encode( t ));
    }

    void sendPreparedGET(int id, Long k, Address addr){
        Tuple t = new Tuple(k,null, Tuple.Type.PREPARED, Tuple.Request.GET, id);
        channel.sendAsync(addr, "Tuple", s.encode( t ));
    }

    void sendPreparedPUT(int id, Long k, byte[] v, Address addr){
        Tuple t = new Tuple( k, v, Tuple.Type.PREPARED, Tuple.Request.PUT, id);
        channel.sendAsync(addr, "Tuple", s.encode( t ));
    }

    private static final int SLEEPTIME = 200;
    private static final int numWorker = 5;

    private static Address[] addrWorkers;
    private static Worker[] workers;
    private static WorkerTest test;
    private static Map< Integer, List<Tuple> > results;

    public static void  main(String[] args) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////
        results = new HashMap<>();

        test = new WorkerTest(Address.from("localhost:12345"),
                (o, t) ->{

                    addToResults(t, o);
                    System.out.println("[MAIN] <==  [W" + getId(o) + "]: " + t.toString());
                });


        addrWorkers = new Address[numWorker];
        workers = new Worker[numWorker];

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

    private static int test0() throws InterruptedException {

        results.clear();

        test.sendPreparedPUT(0,8L, "ola".getBytes(), addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        if( results.keySet().size() != 1)
            return 2;

        test.sendCommit(0, addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        results.clear();

        return 0;
    }

    private static int test1() throws InterruptedException {
        // Test a simple put, commit and get

        results.clear();

        if( numWorker < 3) return 1;

        test.sendPreparedPUT(1,1L, "ola".getBytes(), addrWorkers[0]);
        test.sendPreparedPUT(1,2L, "mundo".getBytes(), addrWorkers[1]);
        test.sendPreparedPUT(1,3L, "lindo".getBytes(), addrWorkers[2]);

        Thread.sleep( SLEEPTIME );

        if( results.keySet().size() != 3)
            return 2;

        Tuple t1 = results.get(0).get(0);
        Tuple t2 = results.get(1).get(0);
        Tuple t3 = results.get(2).get(0);

        results.clear();

        if( ! t1.getMsg().equals(Tuple.Type.OK) || ! t2.getMsg().equals(Tuple.Type.OK) || ! t3.getMsg().equals(Tuple.Type.OK) )
            return 3;

        test.sendCommit(1, addrWorkers[0]);
        test.sendCommit(1, addrWorkers[1]);
        test.sendCommit(1, addrWorkers[2]);

        test.sendPreparedGET(2,1L, addrWorkers[0]);
        test.sendPreparedGET(2,2L, addrWorkers[1]);
        test.sendPreparedGET(2,3L, addrWorkers[2]);

        Thread.sleep( SLEEPTIME );

        if( results.keySet().size() != 3)
            return 4;

        t1 = results.get(0).get(0);
        t2 = results.get(1).get(0);
        t3 = results.get(2).get(0);

        results.clear();

        if( ! t1.getMsg().equals(Tuple.Type.OK) || ! t2.getMsg().equals(Tuple.Type.OK) || ! t3.getMsg().equals(Tuple.Type.OK) )
            return 5;

        if( ! Arrays.equals(t1.getValue(), "ola".getBytes()) || !Arrays.equals(t2.getValue(), "mundo".getBytes()) || !Arrays.equals(t3.getValue(), "lindo".getBytes()))
            return 6;

        test.sendCommit(2, addrWorkers[0]);
        test.sendCommit(2, addrWorkers[1]);
        test.sendCommit(2, addrWorkers[2]);

        Thread.sleep( SLEEPTIME );

        return 0;
    }

    private static int test2() throws InterruptedException {
        // test a  get of nonexistent value

        results.clear();

        if( numWorker < 1 )
            return 1;

        test.sendPreparedGET(3, 20L, addrWorkers[0] );

        Thread.sleep(SLEEPTIME );

        if( results.keySet().size() != 1)
            return 2;

        Tuple t1 = results.get(0).get(0);

        return t1.getMsg().equals( Tuple.Type.ROLLBACK) ? 0 : 3;
    }

    private static int test3() throws InterruptedException {
        // test a lock of value
        // make a put without a commit or rollback and send a second put with same key

        results.clear();

        if( numWorker < 1)
            return 1;

        test.sendPreparedPUT(4,1L, "ola mundo".getBytes(), addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        if( results.keySet().size() != 1)
            return 2;

        test.sendPreparedPUT(5,1L, "ola lindo mundo".getBytes(), addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        if( results.get(0).size() != 1)
            return 3;

        test.sendCommit(4, addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        if( results.get(0).size() != 2)
            return 4;

        test.sendCommit(5, addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        return 0;
    }

    private static int test4() throws InterruptedException{
        // test a roolback after a put and confirm with a get

        results.clear();

        test.sendPreparedPUT(6, 5L, "ola".getBytes(), addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        if( results.keySet().size() != 1)
            return 1;

        test.sendRoolback( 6, addrWorkers[0]);

        test.sendPreparedGET( 7, 5L , addrWorkers[0]);

        Thread.sleep( SLEEPTIME );

        if( results.get(0).size() != 2)
            return 2;

        Tuple t1 = results.get(0).get(1);

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

}
