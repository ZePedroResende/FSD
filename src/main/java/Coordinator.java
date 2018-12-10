import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static java.lang.Math.toIntExact;

class Coordinator {

    private final Address[] workers;
    private int numberCoords;
    private int myId;

    private final ManagedMessagingService channel;
    private final Serializer serializerCollection;
    private final Serializer serializerMap;

    private int numberOfTrans;

    synchronized int getNextTransactionId(){
        return numberCoords * (numberOfTrans++) + myId;
    }

    Coordinator( Address[] workers, Address myAddr, int myId, int numberCoords ) throws ExecutionException, InterruptedException {

        this.workers = workers;
        this.myId = myId;
        this.numberCoords = numberCoords;
        this.numberOfTrans = 0;

        Serializer serializerTuple = new SerializerBuilder()
                .addType( Tuple.Type.class )
                .addType( Tuple.class )
                .build();

        serializerCollection = new SerializerBuilder()
                .addType( Collection.class )
                .build();

        serializerMap = new SerializerBuilder()
                .addType( Map.class )
                .build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( myAddr )
                .build();

        channel.registerHandler( "put", (o, m) -> {

            System.out.println("[C"+myId +"] received \"put\" from client");

            Map<Long,byte[]> map = serializerMap.decode(m);

            int transId = getNextTransactionId();

            List<Address> workersConfirm = new ArrayList<>();

            // for each key-value send a Prepared for the correct worker and wait for reply
            // if receive a "Ok" from the worker then continue
            // else send "Rollback" for all workers who had reply with "Ok" and exit

            for( Map.Entry<Long, byte[]> entrySet : map.entrySet() ){

                Tuple tuple = new Tuple( entrySet.getKey(), entrySet.getValue(), Tuple.Type.PREPARED, transId);

                int workerId = toIntExact( entrySet.getKey() )  % workers.length;

                Address workerAddr = workers[ workerId ];

                byte[] reply;

                try {

                    reply = channel.sendAndReceive(workerAddr, "reply", serializerTuple.encode( tuple) ).get();

                } catch (Exception e) { e.printStackTrace(); return ; }

                Tuple tupleReply = serializerTuple.decode(reply);

                if( tupleReply.msg.equals(Tuple.Type.ROLLBACK) ){

                    Tuple rollback = new Tuple(0,null,Tuple.Type.ROLLBACK, transId);

                    for( Address addr : workersConfirm)
                        channel.sendAsync( addr, "reply", serializerTuple.encode( rollback) );


                    channel.sendAsync(o, "reply", new byte[] {0});
                    return ;
                }

                if( tuple.msg.equals(Tuple.Type.OK) )
                    workersConfirm.add( workerAddr );
            }

            Tuple commitReply = new Tuple(0, null, Tuple.Type.COMMIT, transId);

            for( Address addr : workersConfirm)
                channel.sendAsync( addr, "reply", serializerTuple.encode(commitReply) );

            channel.sendAsync(o, "reply", new byte[] {1});

        }, executorService);

        channel.registerHandler( "get", (o, m) -> {
            System.out.println("C"+myId +": received \"get\" from client");

        }, executorService);

        channel.start().get();
    }
}