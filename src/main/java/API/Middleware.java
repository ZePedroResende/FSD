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

    private static int  NUMCOORD = 1;
    private static int DEFAULTPORT = 12345;

    private static Address[] addresses;
    private static final Serializer s = new SerializerBuilder().addType(RequestPut.class)
                                                               .addType(RequestGet.class)
                                                               .addType(ResponsePut.class)
                                                               .addType(ResponseGet.class)
                                                               .build();

    private static void configure(){
        /*  deve ser carregado o ficheiro conif a alterar
                -> NUMCOORD
        */

        addresses = new Address[ NUMCOORD ];

        for( int i = 0; i < NUMCOORD; i ++ )
            addresses[i] = Address.from( String.format("localhost:1%04d", i) );
    }

    /* Nao faz sentido deixares o utilizador configurar  ( ????? )

    public Middleware( Address[] addresses, String client) {
        this.addresses = addresses;
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from(client))
                .build();
        this.es = Executors.newSingleThreadExecutor();
    } */

    private ManagedMessagingService channel;
    private ExecutorService es;

    public Middleware(String port) {
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from( "localhost:" + port ))
                .build();
        this.es = Executors.newSingleThreadExecutor();
    }

    public Middleware() {
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from( "localhost:" + DEFAULTPORT ))
                .build();
        this.es = Executors.newSingleThreadExecutor();
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){

        int rnd = new Random().nextInt(addresses.length);

        return channel.sendAndReceive( addresses[rnd], "put", s.encode( new RequestPut(values) ), es)
                .thenApply( (i) -> {
                    ResponsePut response = s.decode(i);
                    return response.getResponse() ;
                });
    }

    public CompletableFuture< Map<Long,byte[]> > get(Collection<Long> keys){

        int rnd = new Random().nextInt(addresses.length);

        return channel.sendAndReceive(addresses[rnd], "get", s.encode(new RequestGet(keys)), es)
                .thenApply((i) -> {
                    ResponseGet response = s.decode(i);
                    return response.getResponse();
                });
    }

}

