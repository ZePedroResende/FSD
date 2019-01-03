package Worker;

import Journal.Journal;
import Serializers.Transaction;
import Serializers.Tuple;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class Worker {

    private static final int NUMCOORD = 4;
    private static final boolean DEBUG = true;

    private final ManagedMessagingService channel;

    private Map<Long, byte[]> myHashMap;
    private Map<Long, MyLock> locks;
    private Map<Integer, List<Tuple>> transactionsActions;

    public Worker(int myId,  Address myAddr) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////

        this.myHashMap = new ConcurrentHashMap<>();
        this.locks = new ConcurrentHashMap<>();
        this.transactionsActions = new ConcurrentHashMap<>();

        Serializer serializerTuple = new SerializerBuilder()
                .addType(Tuple.Type.class)
                .addType(Tuple.Request.class)
                .addType(Tuple.class)
                .build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( myAddr )
                .build();

        ///////////////// Recover  //////////////////

        Journal journal = new Journal( "worker"+ myId, serializerTuple);

        if( DEBUG)  System.out.println("[W" + myId + "] Start recover");

        List< Transaction > list =  journal.getCommitted().get();

        for( Transaction t : list){
            Tuple tuple = (Tuple) t;
            if( tuple.getRequest().equals(Tuple.Request.PUT) )
                myHashMap.put( tuple.getKey(), tuple.getValue() );
        }

        list =  journal.getUnconfirmed().get();

        for( Transaction t : list){
            Tuple tuple = (Tuple) t;
            if( tuple.getMsg().equals(Tuple.Type.OK) ){
              
                if( !  transactionsActions.containsKey( tuple.getId()) )
                    transactionsActions.put( tuple.getId(), new ArrayList<>());

                transactionsActions.get( tuple.getId()).add( tuple);

                MyLock ml = new MyLock();
                locks.put( tuple.getKey(), ml );
                CompletableFuture<Boolean> cf = new CompletableFuture<>();
                ml.lock(cf);
                cf.get();

                this.channel.sendAsync( getAddresFromId( tuple.getId()), "RETRY", serializerTuple.encode(tuple) );
            }
        }

        if( DEBUG)  System.out.println("[W" + myId + "] Finnish recover");

      
        ///////////////// Handlers  /////////////////

        BiConsumer< Address, byte[]> handlerConfirm = (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            journal.addSegment( tuple );

            if( DEBUG)  System.out.println("[W"+ myId + "] <== " + tuple.toString() );

            Tuple.Type msg = tuple.getMsg();

            if(msg.equals(Tuple.Type.ROLLBACK)){

                List<Tuple> listTuple = transactionsActions.remove(tuple.getId());

                if( listTuple != null ){
                    for (Tuple t: listTuple)
                        locks.get(t.getKey()).unlock();
                }
            }

            if(msg.equals(Tuple.Type.COMMIT)){

                if( transactionsActions.containsKey( tuple.getId()) ){

                    List<Tuple> listTuple = transactionsActions.remove( tuple.getId() );

                    for (Tuple t : listTuple)
                        if ( t.getRequest().equals(Tuple.Request.PUT))
                            myHashMap.put( t.getKey(), t.getValue());

                    for (Tuple t: listTuple)
                        locks.get( t.getKey() ).unlock();

                }
            }
        };

        BiFunction< Address, byte[], CompletableFuture<byte[]>> handlerPrepare = (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            journal.addSegment( tuple );

            if( DEBUG)  System.out.println("[W"+ myId + "] <== " + tuple.toString() );

            CompletableFuture<Boolean> cf;

            cf = new CompletableFuture<Boolean>();

            if( ! transactionsActions.containsKey( tuple.getId()) )
                this.transactionsActions.put( tuple.getId(), new ArrayList<>() );

            transactionsActions.get( tuple.getId() ).add( tuple );

            if( ! locks.containsKey( tuple.getKey() ) )
                locks.put( tuple.getKey(), new MyLock() );

            MyLock myLock = locks.get( tuple.getKey() );

            myLock.lock( cf);
                return cf.thenApply((r) -> {

                    Tuple tupleReply;

                    if (tuple.getRequest().equals(Tuple.Request.GET)) {

                        byte[] valeu = myHashMap.get(tuple.getKey());
                        tupleReply = new Tuple(tuple, valeu, valeu == null ? Tuple.Type.ROLLBACK : Tuple.Type.OK);
                    } else
                        tupleReply = new Tuple(tuple, tuple.getValue(), Tuple.Type.OK);

                    try {
                        return serializerTuple.encode(tupleReply);
                    }finally {
                        journal.addSegment( tuple );

                        if( DEBUG)  System.out.println("[W" + myId + "] ==> " + tupleReply.toString());
                    }
                });
        };

        this.channel.registerHandler( "PREPARE", handlerPrepare );

        this.channel.registerHandler( "CONFIRM", handlerConfirm, executorService );

        this.channel.start().get();
    }

    private Address getAddresFromId(int id) {
        int coord= id % NUMCOORD;
        return Address.from(  String.format("localhost:11%03d", coord));
    }

    public String mapToString(String info) {
        StringBuilder sb  = new StringBuilder();

        for( Long key : myHashMap.keySet()){

            byte[] b = myHashMap.get(key);
            String value =  b == null ? "null" : new String(b);
            sb.append( info + " " + key + " : " + value +"\n");
        }

        return sb.toString();
    }

}
