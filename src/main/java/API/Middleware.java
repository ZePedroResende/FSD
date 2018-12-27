package API;

import Serializers.RequestGet;
import Serializers.RequestPut;
import Serializers.ResponseGet;
import Serializers.ResponsePut;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

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

    private Serializer requestPutS = new Serializer.builder().addType(Map.class).addType(RequestPut.class).build();
    private Serializer responsePutS = new SerializerBuilder().addType(Boolean.class).addType(ResponsePut.class).build();
    private Serializer requestGetS = new SerializerBuilder().addType(Collection.class).addType(RequestGet.class).build();
    private Serializer responseGetS = new SerializerBuilder().addType(Map.class).addType(ResponseGet.class).build();
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
        return channel.sendAndReceive(addresses[rnd], "put", requestPutS.encode(new RequestPut(values)), es)
                .thenApply((i) -> {
                    ResponsePut response = responsePutS.decode(i);
                    return response.getResponse() ;
                });
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){

        int rnd = new Random().nextInt(addresses.length);

        return channel.sendAndReceive(addresses[rnd], "get", requestGetS.encode(new RequestGet(keys)), es)
                .thenApply((i) -> {
                    ResponseGet response = responseGetS.decode(i);
                    return response.getResponse();
                });
    }

}

