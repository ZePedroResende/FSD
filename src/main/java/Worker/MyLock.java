package Worker;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class MyLock {

    private Queue< CompletableFuture<Void> > queue;

    MyLock(){
        queue = new LinkedList<>();
    }

    public synchronized void lock( CompletableFuture<Void> cf ){

        queue.add(cf);
        if( queue.size() == 1 )
            cf.complete(null);

    }

    public synchronized void unlock() {
        if( ! queue.isEmpty() )
            queue.poll().complete(null);
    }

}
