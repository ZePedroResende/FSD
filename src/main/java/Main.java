import Coordinator.Coordinator;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Main {
    private static final Address[] workers = {Address.from("localhost:22201")};
    private static final Address[] coords = {Address.from("localhost:11101")};

    private static class Server {

        public static void main(String[] args) throws ExecutionException, InterruptedException {

            // Inicializaçao
            int next = 0;

            for (Address addr : workers)
                new Worker(next++, addr);

            next = 0;

            for (Address addr : coords)
                new Coordinator(workers, coords, next++);

        }
    }

    private static class Client {

        public static void main(String[] args) throws ExecutionException, InterruptedException {

            MyHashMap myHashMap = new MyHashMap();
            Map<Long, byte[]> m = new HashMap<>();
            m.put(1L, "ola".getBytes());
            m.put(2L, "ola".getBytes());
            m.put(3L, "ola".getBytes());

        }
    }

}
