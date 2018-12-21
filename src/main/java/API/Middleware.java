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
import java.util.function.Consumer;


public class Middleware {
    private Address[] addresses = {Address.from("localhost:12345")
            ,Address.from("localhost:12346"), Address.from("localhost:12347")
            ,Address.from("localhost:12348"),Address.from("localhost:12349")};
    private ManagedMessagingService channel;
    private Serializer s = new SerializerBuilder()
            .addType(RequestPut.class)
            .addType(RequestGet.class)
            .addType(ResponsePut.class)
            .addType(ResponseGet.class)
            .build();
    private ExecutorService es;

    public Middleware(Address[] addresses, String client) {
        this.addresses = addresses;
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from(client))
                .build();
        this.es = Executors.newSingleThreadExecutor();
    }

    public Middleware(String client) {
        this.channel = NettyMessagingService.builder()
                .withAddress(Address.from(client))
                .build();
        this.es = Executors.newSingleThreadExecutor();
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){
        int rnd = new Random().nextInt(addresses.length);
        return channel.sendAndReceive(addresses[rnd], "put", s.encode(new RequestPut(values)), es)
                .thenApply((i) -> {
                    ResponsePut response = s.decode(i);
                    return response.getResponse() ;
                });
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){

        int rnd = new Random().nextInt(addresses.length);

        return channel.sendAndReceive(addresses[rnd], "get", s.encode(new RequestGet(keys)), es)
                .thenApply((i) -> {
                    ResponseGet response = s.decode(i);
                    return response.getResponse();
                });
    }

}

