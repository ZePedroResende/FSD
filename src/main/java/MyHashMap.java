import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Random;


public class MyHashMap {

    private static final Address[] coord = { Address.from("localhost:11101") };

    private final ManagedMessagingService channel;
    private final Serializer serializerCollection;
    private final Serializer serializerMap;

    private Random r;

    MyHashMap(){

        r = new Random();

        this.serializerMap= new SerializerBuilder()
                .addType( Map.class )
                .build();

        this.serializerCollection= new SerializerBuilder()
                .addType( Collection.class )
                .build();

        this.channel = NettyMessagingService.builder()
                .withAddress( Address.from("localhost:12345") )
                .build();

        this.channel.start();
    }

    /*
    public CompletableFuture<Boolean> put ( Map<Long,byte[]> values){

        return new CompletableFuture<>(Boolean.TRUE);
    }


    public CompletableFuture< Map<Long,byte[]> > get (Collection<Long> keys){

        return new CompletableFuture<>( new HashMap<Long, byte[]>() );
    }
    */
}