package API;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class API {

    public static Middleware middleware;

    public static void main(String[] args){
        try {
            Middleware middleware = new Middleware(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){
        return middleware.put(values);
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
        return middleware.get(keys);
    }
}

