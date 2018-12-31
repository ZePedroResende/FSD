package Test;

import Coordinator.Coordinator;
import Serializers.Tuple;
import Serializers.*;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import org.apache.commons.math3.analysis.function.Add;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class CoordinatorTest {
    private static final int SLEEPTIME = 200;
    private static Coordinator[] coordinators;
    private static Address[] addrCoords;
    private static Address[] workerAddress;
    public static int numCoords;
    private static CoordinatorTest test;
    private static Middleware api;
    private final ManagedMessagingService channel;
    private final ExecutorService executorService;
    private final Serializer s;
    private static Map<Long,byte[]> results;

    public CoordinatorTest(Address myAddr, BiConsumer<Address, Tuple> handler) throws ExecutionException, InterruptedException {

        s = new SerializerBuilder()
                .addType(Serializers.Tuple.Type.class)
                .addType(Serializers.Tuple.Request.class)
                .addType(Tuple.class)
                .build();

        this.executorService = Executors.newFixedThreadPool(5);

        this.channel = NettyMessagingService.builder()
                .withAddress(myAddr)
                .build();

        this.channel.registerHandler("Tuple", (o, m) -> {
            handler.accept(o, s.decode(m));
        }, executorService);

        this.channel.start().get();
    }

    void sendRoolback(int id, Address addr){
        Tuple t = new Tuple(1,null, Tuple.Type.ROLLBACK, Tuple.Request.CANCEL,id);
        channel.sendAsync(addr, "Tuple", s.encode( t ));
    }

    void sendOkget(int id, Address addr, Long key, byte[] value){
        Tuple t = new Tuple(key,value, Tuple.Type.OK, Tuple.Request.GET, id);
        channel.sendAsync(addr, "Tuple", s.encode( t ));
    }

    void sendOkput(int id, Address addr,Long key){
        Tuple t = new Tuple(key,null, Tuple.Type.OK, Tuple.Request.PUT, id);
            System.out.println("[OKPUT] <==  [" + addr + "]: " + t.toString());
            channel.sendAsync(addr, "Tuple", s.encode( t ));
    }


    public static void  main(String[] args) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////

        results = new HashMap<>();
        numCoords = 1;
        Address[] workerAddress = {Address.from("localhost:12347")};
        Address[] coordAddress = {Address.from("localhost:1234")};
        test = new CoordinatorTest(workerAddress[0],
                (o, t) ->{

                    System.out.println("[MAIN] <==  [" + o + "]: " + t.toString());
                    if(t.getMsg().equals(Tuple.Type.PREPARED)){
                        if(t.getRequest().equals(Tuple.Request.GET)){
                            test.sendOkget(t.getId(),o,t.getKey(),results.get(t.getKey()));
                        }
                        else{
                            results.put(t.getKey(),t.getValue());
                            System.out.println("OLA");
                            test.sendOkput(t.getId(),o,t.getKey());
                        }
                    }

                    if(t.getMsg().equals(Tuple.Type.COMMIT)){
                        System.out.println("[COMMIT] <==  [" + o + "]: " + t.toString());
                    }

                    if(t.getMsg().equals(Tuple.Type.ROLLBACK)){
                        System.out.println("[ROLLBACK] <==  [" + o + "]: " + t.toString());
                    }

                });

        coordinators = new Coordinator[numCoords];

        for(int i = 0; i < numCoords; i ++ ){
            coordinators[i] = new Coordinator(coordAddress, workerAddress, i);
        }

        api = new Middleware(coordAddress[0],"localhost:12346");
        /////////////////  TESTS  /////////////////

        Thread.sleep(1000);

        System.out.println( "##### Test0 #####");
        int result = test0();
        System.out.println( "#### " + ( result == 0 ? "DONE" :"ERROR (" + result +")" ) + " ####" );
        if( result > 0 ) return ;
        System.out.println("\n\nAll tests done successfully :D ");
    }

    private static int test0() throws InterruptedException, ExecutionException {

        results.clear();
        Map values = new HashMap<Long, byte[]>();
        List<Long> keys = new ArrayList();

        values.put(new Long(1), "ola".getBytes());
        CompletableFuture<Boolean> result = api.put(values);
        if (!result.get()) {
            return 1;
        }

        keys.add(new Long(1));
        CompletableFuture<Map<Long, byte[]>> result2 = api.get(keys);
        if (!result2.get().equals(values)) {
            return 2;
        }

        values.put(1, "ole".getBytes());
        result = api.put(values);
        if (!result.get()) {
            return 3;
        }

        result2 = api.get(keys);
        if (!result2.get().equals(values)) {
            return 4;
        }

        Thread.sleep(SLEEPTIME);
        results.clear();
        return 0;
    }

}

class Middleware {
    private ManagedMessagingService channel;
    private Address address;
    private Serializer requestPutS = new SerializerBuilder().addType(Map.class).addType(RequestPut.class).build();
    private Serializer responsePutS = new SerializerBuilder().addType(Boolean.class).addType(ResponsePut.class).build();
    private Serializer requestGetS = new SerializerBuilder().addType(Collection.class).addType(RequestGet.class).build();
    private Serializer responseGetS = new SerializerBuilder().addType(Map.class).addType(ResponseGet.class).build();
    private ExecutorService es;

    Middleware(Address address, String client) {
        this.address = address;
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from(client))
                .build();
        this.es = Executors.newFixedThreadPool(5);
        this.channel.start();
    }

    public Middleware(String client) {
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from(client))
                .build();
        this.es = Executors.newSingleThreadExecutor();
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){
        return channel.sendAndReceive(address, "put", requestPutS.encode(new RequestPut(values)), Duration.ofMinutes(5) ,es)
                .thenApply((i) -> {
                    System.out.println("Response");
                    ResponsePut response = responsePutS.decode(i);
                    return response.getResponse() ;
                });
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){

        return channel.sendAndReceive(address, "get", requestGetS.encode(new RequestGet(keys)), es)
                .thenApply((i) -> {
                    ResponseGet response = responseGetS.decode(i);
                    return response.getResponse();
                });
    }

}

