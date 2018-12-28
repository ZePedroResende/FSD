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

public class Worker {

    private final ManagedMessagingService channel;
    private final ExecutorService executorService;

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

        this.executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( myAddr )
                .build();

        Journal journal = new Journal( "worker"+myId, serializerTuple);


        ///////////////// Recover  //////////////////

        System.out.println("[W" + myId + "] Start recover");

        List<Transaction> list =  journal.getCommitted().get();

        for( Transaction t : list){
            Tuple tuple = (Tuple) t;
            if( tuple.getRequest().equals(Tuple.Request.PUT) )
                myHashMap.put( tuple.getKey(), tuple.getValue() );
        }

        this.mapToString("[W"+ myId +"]");

        System.out.println("[W" + myId + "] Finnish recover");


        ///////////////// Handlers  /////////////////

        this.channel.registerHandler( "Tuple", (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            System.out.println("[W"+ myId + "] <= received " + tuple.toString() );

            Tuple.Type msg = tuple.getMsg();

            if( msg.equals( Tuple.Type.PREPARED) ){

                CompletableFuture<Void> cf;

                cf = new CompletableFuture<>().thenRun(() ->{
                    Tuple tupleReply;

                    if( tuple.getRequest().equals( Tuple.Request.GET )){
                        byte[] valeu = myHashMap.get( tuple.getKey() );
                        tupleReply = new Tuple( tuple, valeu,  valeu == null ? Tuple.Type.ROLLBACK : Tuple.Type.OK);
                    }else
                        tupleReply = new Tuple( tuple, tuple.getValue(), Tuple.Type.OK);

                    channel.sendAsync(o, "Tuple", serializerTuple.encode(tupleReply));
                });

                if( ! transactionsActions.containsKey( tuple.getId()) ) {
                    List<Tuple> l = new ArrayList<>();
                    this.transactionsActions.put(tuple.getId(), l);
                }

                transactionsActions.get( tuple.getId() ).add( tuple );

                if( ! locks.containsKey( tuple.getKey() ) )
                    locks.put( tuple.getKey(), new MyLock());

                MyLock myLock = locks.get( tuple.getKey() );

                myLock.lock( cf);

            }

            if(msg.equals(Tuple.Type.ROLLBACK)){

                List<Tuple> listTuple = transactionsActions.remove(tuple.getId());

                for (Tuple t: listTuple) {

                    locks.get(t.getKey()).unlock();

                }

            }

            if(msg.equals(Tuple.Type.COMMIT)){

                List<Tuple> listTuple = transactionsActions.get(tuple.getId());

                for (Tuple t: listTuple) {

                    if(t.getRequest().equals(Tuple.Request.PUT)){
                        myHashMap.put(t.getKey(), t.getValue());
                    }

                }

                transactionsActions.remove(tuple.getId());

                for (Tuple t: listTuple) {

                    locks.get(t.getKey()).unlock();

                }

            }

        }, executorService);

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
