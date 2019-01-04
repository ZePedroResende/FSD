package API;

import Config.Config;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class API {

    private Middleware middleware;

    public API() throws InterruptedException, ExecutionException, IOException {
        middleware = new Middleware( Config.defaultConfig() );
    }
    public API( int port ) throws InterruptedException, ExecutionException, IOException {
        middleware = new Middleware( port , Config.defaultConfig() );
    }
    public API(int port, Config config) throws InterruptedException, ExecutionException, IOException {
        middleware = new Middleware( port , config );
    }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){
        return middleware.put(values);
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
        return middleware.get(keys);
    }
}

