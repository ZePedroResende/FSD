package API;

import Config.Config;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import org.omg.CORBA.CODESET_INCOMPATIBLE;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class API {

    public static Middleware middleware;

    public static void main(String[] args) throws Exception {
        Middleware middleware = new Middleware(args[0], Config.loadConfig());
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){
        return middleware.put(values);
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
        return middleware.get(keys);
    }
}

