
import javafx.util.Pair;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MyLock {

    LinkedHashMap<Integer,CompletableFuture<Void>> next; // o acesso tem que ser atomico
    Integer current_transactio;


    CompletableFuture<Void> lock(Integer id){
        CompletableFuture<Void> cf = new CompletableFuture<>();
        if( next.isEmpty() )
            cf.complete(null);
        else
            next.put(id, cf);
        return cf;
    }

    void unlock(){
        if( ! next.isEmpty() )
            next.get(0).complete(null);
    }

}
