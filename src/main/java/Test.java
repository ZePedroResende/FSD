import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class TestWorker {

    private static final Address worker = Address.from("localhost:11100");
    private final ManagedMessagingService channel;
    private final Serializer serializerTuple;

    TestWorker(Address myAddr) throws ExecutionException, InterruptedException {

        serializerTuple = new SerializerBuilder().addType(Tuple.Type.class).addType(Tuple.class).build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        channel = NettyMessagingService.builder().withAddress( myAddr).build();

        channel.registerHandler("reply", (o, m) -> {
            Tuple t = serializerTuple.decode(m);
            System.out.println("[MAIN] Reply received " + t.msg + " ( value = " + ( t.value == null ? "null" :new String(t.value)) + " )");
        }, executorService);

        channel.start().get();
    }

    void sendPut( Long l, byte[] b, int id) throws ExecutionException, InterruptedException {

        Tuple t = new Tuple(l , b, Tuple.Type.PREPARED, id);
        channel.sendAsync( worker, "put", serializerTuple.encode(t)).get();
    }

    void sendGet( Long l, int id) throws ExecutionException, InterruptedException {

        Tuple t = new Tuple(l,null, Tuple.Type.PREPARED, id);
        channel.sendAsync( worker, "get", serializerTuple.encode(t)).get();
    }

    void sendRoolback(int id) throws ExecutionException, InterruptedException {

        Tuple t = new Tuple(0, null ,Tuple.Type.ROLLBACK, id);
        channel.sendAsync( worker, "reply", serializerTuple.encode(t)).get();
    }

    void sendCommit(int id) throws ExecutionException, InterruptedException {

        Tuple t = new Tuple(0, null ,Tuple.Type.COMMIT, id);
        channel.sendAsync( worker, "reply", serializerTuple.encode(t)).get();
    }

    static void test1(TestWorker t) throws ExecutionException, InterruptedException {

        Scanner s = new Scanner( System.in);
        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendPut( 2L, "hello".getBytes() , 0);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendPut( 2L, "world".getBytes() , 1);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendCommit(0);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendRoolback(1);
    }

    static void test2(TestWorker t) throws ExecutionException, InterruptedException {
        Scanner s = new Scanner( System.in);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendGet( 2L, 0);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendGet( 2L, 1);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendGet( 2L, 2);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendCommit(0);
        t.sendCommit(1);
        t.sendCommit(2);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        new Worker(0, worker);

        TestWorker t = new TestWorker( Address.from("localhost:12345"));

        Thread.sleep(300);

        /////////  MESSAGES ///////

        Scanner s = new Scanner( System.in);
        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendPut( 2L, "hello".getBytes() , 0);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendCommit(0);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendGet( 2L,  1);

        System.out.println("[MAIN] Press any key"); s.nextLine();
        t.sendCommit(1);

    }
}

class TestCoord{

    private static final Address[] workerAddrs = { Address.from("localhost:11100") };
    private static final Address coordAddrs = Address.from("localhost:22200");
    private final ManagedMessagingService channel;
    private final Serializer serializerTuple;
    private final Serializer serializerMap;
    private final Serializer serializerList;

    TestCoord(Address myAddr) throws ExecutionException, InterruptedException {

        serializerTuple = new SerializerBuilder().addType(Tuple.Type.class).addType(Tuple.class).build();
        serializerMap = new SerializerBuilder().addType(Map.class).build();
        serializerList = new SerializerBuilder().addType(List.class).build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        channel = NettyMessagingService.builder().withAddress( myAddr).build();

        channel.registerHandler("reply", (o, m) -> {
            System.out.println("Recebido dentro do handler! ");
        }, executorService);

        channel.start().get();
    }

    void putAndReceive(Map<Long, byte[]> map) throws ExecutionException, InterruptedException {

        byte[] reply = channel.sendAndReceive(coordAddrs, "put", serializerMap.encode(map)).get();
        System.out.println("[MAIN] Reply : " + ( reply[0] == 1 ? "True" : "False") );
    }

    void getAndReceive(List<Long> list) throws ExecutionException, InterruptedException {

        byte[] reply = channel.sendAndReceive(coordAddrs, "get", serializerList.encode(list)).get();
        System.out.println("[MAIN] Reply : " + ( reply[0] == 1 ? "True" : "False") );
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        new Coordinator( workerAddrs , coordAddrs, 0 ,1  );

        for( int i = 0 ; i < workerAddrs.length; i ++ )
            new Worker(i, workerAddrs[i] );

        TestCoord t = new TestCoord(Address.from("localhost:12345"));

        Map<Long,byte[]> map = new HashMap<>();
        map.put( 2L, "dois".getBytes());
        map.put( 3L, "tres".getBytes());

        t.putAndReceive( map);


    }

}

class TestRecover{
    private static final String file = "worker-0";

    public static void main(String[] args){
        Serializer serializerTuple = new SerializerBuilder()
                .addType(Tuple.Type.class)
                .addType(Tuple.class)
                .build();

        SegmentedJournal<Object> j = SegmentedJournal.builder()
                .withName( file )
                .withSerializer( serializerTuple )
                .build();
        SegmentedJournalReader<Object> reader = j.openReader(0);

        while( reader.hasNext() ) {
            Tuple tuple = (Tuple) reader.next().entry();
            String value =  tuple.value == null ? "null" : new String(tuple.value);
            System.out.println( tuple.transId + " >> [" + tuple.msg + "] " + tuple.key + " ->" + value);

        }
    }
}
