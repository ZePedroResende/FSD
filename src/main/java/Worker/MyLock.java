package Worker;

import java.util.*;
import java.util.concurrent.CompletableFuture;

class MyLock {

    private Queue< CompletableFuture<Boolean>> queue;

    MyLock(){
        queue = new LinkedList<>();
    }

    public synchronized void lock( CompletableFuture<Boolean> cf ){

        if( ! queue.add(cf))
            System.out.println("LOCK ERROR!!! ");

        if( queue.size() == 1 )
            cf.complete(null);
    }

    public synchronized void unlock() {

        queue.remove();

        if( ! queue.isEmpty() )
            queue.element().complete(null);
    }

}
