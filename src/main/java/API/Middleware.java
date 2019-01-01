package API;

import Serializers.RequestGet;
import Serializers.RequestPut;
import Serializers.ResponseGet;
import Serializers.ResponsePut;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Middleware {

    private static int NUMCOORD = 1;
    private static int DEFAULTPORT = 12345;
    private static Address[] addresses;
    private Serializer requestPutS = new SerializerBuilder().addType(Map.class).addType(RequestPut.class).build();
    private Serializer responsePutS = new SerializerBuilder().addType(Boolean.class).addType(ResponsePut.class).build();
    private Serializer requestGetS = new SerializerBuilder().addType(Collection.class).addType(RequestGet.class).build();
    private Serializer responseGetS = new SerializerBuilder().addType(Map.class).addType(ResponseGet.class).build();
    private ManagedMessagingService channel;
    private ExecutorService es;

    private static void configure(){
        /*  deve ser carregado o ficheiro conif a alterar
                -> NUMCOORD
        */

        addresses = new Address[ NUMCOORD ];

        for( int i = 0; i < NUMCOORD; i ++ )
            addresses[i] = Address.from( String.format("localhost:1%04d", i) );
    }


    public Middleware( Address[] addresses, String client) {
        this.addresses = addresses;
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from(client))
                .build();
        this.es = Executors.newSingleThreadExecutor();
        this.channel.start();
    }


    public Middleware(String port) {
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from(port))
                .build();
        this.es = Executors.newSingleThreadExecutor();
        this.channel.start();
    }

    public Middleware() {
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from( "localhost:" + DEFAULTPORT ))
                .build();
        this.es = Executors.newSingleThreadExecutor();
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){

        int rnd = new Random().nextInt(addresses.length);
      
        return channel.sendAndReceive( addresses[rnd], "put", requestPutS.encode( new RequestPut(values) ), es)
                .thenApply( (i) -> {
                    ResponsePut response = responsePutS.decode(i);

                    return response.getResponse() ;
                });
    }

    public CompletableFuture< Map<Long,byte[]> > get(Collection<Long> keys){

        int rnd = new Random().nextInt(addresses.length);

        return channel.sendAndReceive(addresses[rnd], "get", requestGetS.encode(new RequestGet(keys)), es)
                .thenApply((i) -> {
                    ResponseGet response = responseGetS.decode(i);
                    return response.getResponse();
                });
    }

}

