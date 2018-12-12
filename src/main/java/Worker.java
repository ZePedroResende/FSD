import Serializers.Tuple;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import java.util.*;
import java.util.concurrent.*;



class Worker {

    private final ManagedMessagingService channel;
    private final ExecutorService executorService;

    private Map< Long, byte[] > map;

    private final Log log;

    Worker(int myId,  Address myAddr) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////

        this.map = new ConcurrentHashMap<>();

        Serializer serializerTuple = new SerializerBuilder()
                .addType(Tuple.Type.class)
                .addType(Tuple.class)
                .build();

        this.executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( myAddr )
                .build();


        ///////////////// Recover  /// //////////////

        this.log = new Log( "worker-" + myId, serializerTuple);

        System.out.println("[W" + myId + "] Start recover");

        this.log.recover( ( trans ) ->{
            Tuple t = (Tuple) trans;
            System.out.println( "[W" + myId + "] Recover " + t.key  + ": " + new String(t.value) );
            map.put( t.key, t.value);
        });

        System.out.println("[W" + myId + "] Finnish recover");

        ///////////////// Handlers  /////////////////

        this.channel.registerHandler( "put", (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            // add to log file
            log.addLog(tuple);

            System.out.println("[W"+ myId + "] received \"put\" " + tuple.key + ": " + tuple.value == null ? "null" : new String(tuple.value) + " - transaction " + tuple.transId );

            /*
                action
             */

        }, executorService);

        this.channel.registerHandler( "get", (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            System.out.println("[W"+ myId + "] received \"put\" " + tuple.key + ": " + tuple.value == null ? "null" : new String(tuple.value) + " - transaction " + tuple.transId );

            // add to log file
            log.addLog(tuple);

            /*
                action
             */


        }, executorService);

        this.channel.registerHandler( "reply", (o,m) ->{

            Tuple tuple = serializerTuple.decode(m);

            System.out.println("[W"+ myId +"] "+ tuple.msg + " transaction " + tuple.transId);

            // add to log file
            log.addLog( tuple );

            /*
                action
             */

        }, executorService);

        this.channel.start().get();
    }
}
*/
