package Test;

import Config.Config;
import Coordinator.Coordinator;
import Serializers.*;
import Serializers.Tuple;
import Worker.Worker;
import API.Middleware;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

public class SimpleTest{
    private static final int SLEEPTIME = 200;
    private static Coordinator[] coordinators;
    private static Address[] addrCoords;
    private static Address[] workerAddress;
    public static int numCoords;
    private static Worker test;
    private static Middleware api;
    private static Serializer s;
    private static Map<Long,byte[]> results;



    public static void  main(String[] args) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////

        results = new HashMap<>();
        numCoords = 1;
        Address[] workerAddress = {Address.from("localhost:12347")};
        Address[] coordAddress = {Address.from("localhost:11000")};

        test = new Worker(0 );

        Thread.sleep(10000);

        coordinators = new Coordinator[numCoords];

        for(int i = 0; i < numCoords; i ++ ){
            try {
                coordinators[i] = new Coordinator(i, Config.loadConfig());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Thread.sleep(10000);

        try {
            api = new Middleware(1239,Config.loadConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
        /////////////////  TESTS  /////////////////

        Thread.sleep(10000);

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
        Map<Long, byte[]> lol = result2.get();
        byte[] lol1 = lol.get(new Long(1));
        String out = new String(lol1);
        if (!out.equals("ola")) {
            return 2;
        }

        values.put(new Long(1), "ole".getBytes());
        result = api.put(values);
        if (!result.get()) {
            return 3;
        }

        result2 = api.get(keys);

        lol = result2.get();
        lol1 = lol.get(new Long(1));
        out = new String(lol1);
        if (!out.equals("ole")) {
            return 4;
        }

        Thread.sleep(SLEEPTIME);
        results.clear();
        return 0;
    }

}
