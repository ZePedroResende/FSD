package API;

import Serializers.*;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Middleware {
    /*
        It isn't Thread safe
     */

    private static int NUMCOORD = 1;

    private Address[] addresses;
    private Map<Integer, CompletableFuture<Boolean>> putsWaiting;
    private Map<Integer, CompletableFuture< Map<Long,byte[]>>> getsWaiting;

    FileOutputStream fos;
    private int nextTransactionId;

    private Serializer requestPutS = new SerializerBuilder().addType(Map.class).addType(RequestPut.class).build();
    private Serializer responsePutS = new SerializerBuilder().addType(Boolean.class).addType(ResponsePut.class).build();
    private Serializer requestGetS = new SerializerBuilder().addType(Collection.class).addType(RequestGet.class).build();
    private Serializer responseGetS = new SerializerBuilder().addType(Map.class).addType(ResponseGet.class).build();

    private ManagedMessagingService channel;
    private ExecutorService es;

    public Middleware( Address[] addresses, String client) throws Exception {

        this.addresses = addresses;

        startUp( client );
    }

    public Middleware(String client) throws Exception {

        this.addresses = new Address[ NUMCOORD ];

        for( int i = 0; i < NUMCOORD; i ++ )
            this.addresses[i] = Address.from( String.format("localhost:22%03d", i) );

        startUp( client );
    }

    private void startUp(String client) throws Exception {

        File file= new File(".nextId.txt");

        try {
            Scanner sc = new Scanner( file );
            while( sc.hasNextInt() )
                nextTransactionId = sc.nextInt();
            nextTransactionId ++;
            sc.close();
        } catch (FileNotFoundException e) {
            nextTransactionId = 0;
        }

        file.createNewFile(); // if file already exists will do nothing

        fos = new FileOutputStream(".nextId.txt",false);

        this.putsWaiting= new HashMap<>();
        this.getsWaiting= new HashMap<>();

        this.channel = NettyMessagingService.builder()
                .withAddress( Address.from( "localhost:" + client) )
                .build();

        this.es = Executors.newSingleThreadExecutor();

        this.channel.registerHandler( "put", (o,m) ->{
            ResponsePut response = responsePutS.decode(m);
            CompletableFuture<Boolean> cf =  putsWaiting.remove( response.getId() );
            if( cf != null  )
                cf.complete(response .getResponse() );
        }, es);

        this.channel.registerHandler( "get", (o,m) -> {
            ResponseGet response = responseGetS.decode(m);
            CompletableFuture<Map<Long,byte[]>> cf =  getsWaiting.remove( response.getId() );
            if( cf != null  )
                cf.complete(response.getResponse() );
        }, es);

        this.channel.start().get();
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){

        int rnd = new Random().nextInt(addresses.length);

        int transId = getNextTransactionId() ;

        CompletableFuture<Boolean> cf = new CompletableFuture<>();

        this.putsWaiting.put( transId, cf);

        channel.sendAsync( addresses[rnd], "put", requestPutS.encode( new RequestPut(values, transId) ));

        return cf;
    }

    public CompletableFuture< Map<Long,byte[]> > get(Collection<Long> keys){

        int rnd = new Random().nextInt(addresses.length);

        int transId = getNextTransactionId() ;

        CompletableFuture< Map<Long,byte[]> > cf = new CompletableFuture<>();

        this.getsWaiting.put( transId, cf);

        channel.sendAsync( addresses[rnd], "put", requestPutS.encode( new RequestGet( keys, transId) ));

        return cf;
    }

    private int getNextTransactionId() {
        try {
            this.fos.write( nextTransactionId );
            this.fos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this.nextTransactionId ++;
    }
}
