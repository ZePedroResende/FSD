import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

class CompletableFutureLock {

    private class Pair{
        final CompletableFuture<Integer> cf;
        final int id;

        private Pair(CompletableFuture<Integer> cf, int id) {
            this.cf = cf;
            this.id = id;
        }
    }

    private Queue< Pair > next;

    CompletableFutureLock(){
        next = new ConcurrentLinkedDeque<>();
    }

    CompletableFuture<Integer> lock( int id ){

        CompletableFuture<Integer> cf = new CompletableFuture<>();

        Pair pair = new Pair( cf, id);

        if( next.isEmpty() )
            cf.complete(id);
        else
            next.add( pair );

        return cf;
    }

    public void unlock(){
        // o valor de retorno serve para verificar se a queue esta vazia
        Pair pair = next.peek();

        if(  pair  != null ) {
            pair.cf.complete(pair.id);
            next.poll();
        }

    }

}
