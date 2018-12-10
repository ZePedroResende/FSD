import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MyLock {

    List<CompletableFuture<Void>> next; // o acesso tem que ser atomico


    CompletableFuture<Void> lock(){
        CompletableFuture<Void> cf = new CompletableFuture<>();
        if( next.isEmpty() )
            cf.complete(null);
        else
            next.add( cf);
        return cf;
    }

    void unlock(){
        if( ! next.isEmpty() )
            next.get(0).complete(null);
    }

}
