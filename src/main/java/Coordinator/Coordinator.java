package Coordinator;

import Journal.Journal;
import Serializers.*;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;


public class Coordinator {

    private final Address[] workers;
    private final Address[] coordinators;
    private final ManagedMessagingService channel;
    private final int myId;
    private final ExecutorService es;
    private final Serializer s;
    private int numberOfTrans;
    private Map<Integer,Boolean> oldTransactions;
    private Journal journal;

    public Coordinator(Address[] coordinators , Address[] workers, int myId) {
        this.coordinators = coordinators;
        this.workers = workers;
        this.myId = myId;
        this.channel = NettyMessagingService.builder().withAddress(coordinators[myId]).build();
        this.es = Executors.newSingleThreadExecutor();
        this.s = Serializer.builder()
                .addType(Tuple.Request.class)
                .addType(Tuple.Type.class)
                .addType(Tuple.class)
                .build();
        this.oldTransactions = new ConcurrentHashMap<>();
        this.journal = new Journal( "coordinator"+ myId, s);

        List< Transaction > list = null;
        try {
            list = journal.getCommitted().get();

            for( Transaction t : list) {
                Tuple tuple = (Tuple) t;
                if (tuple.getRequest().equals(Tuple.Request.PUT))
                    oldTransactions.put(tuple.getId(), true);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }


        try {
            list = journal.getLastUnconfirmed().get();
            if(list.size() != 0){
                if(list.get(0).isOk()){
                    for (Transaction t : list) {
                        Tuple tuple = (Tuple) t;
                        commitRequest(t.getId(),workers[getWorkerIndex( tuple.getKey())],((Tuple) t).getRequest());
                    }
                }else{
                    for (Transaction t : list) {
                        Tuple tuple = (Tuple) t;
                        rollbackRequest(workers[getWorkerIndex( tuple.getKey())],t.getId());
                    }
                }
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }


        Serializer reqPutSer = new SerializerBuilder().addType(Map.class).addType(RequestPut.class).build();
        Serializer respPutSer = new SerializerBuilder().addType(Boolean.class).addType(ResponsePut.class).build();

        channel.registerHandler( "put", (o, m) -> {
            System.out.println("PUT");
            RequestPut requestPut = reqPutSer.decode(m);
            Boolean b = put(requestPut.getValues());
            return respPutSer.encode(new ResponsePut(b));
        },es);

        Serializer reqGetSer= new SerializerBuilder().addType(Collection.class).addType(RequestGet.class).build();
        Serializer respGetSer =new SerializerBuilder().addType(Map.class).addType(ResponseGet.class).build();

        channel.registerHandler( "get", (o, m) -> {
            System.out.println("GET");
            RequestGet requestGet = reqGetSer.decode(m);
            Map<Long,byte[]> map = get(requestGet.getValues());
            return respGetSer.encode(new ResponseGet(map));
        },es);

        channel.registerHandler("RETRY",  (o, m) -> {
            Tuple t = this.s.decode(m);
            if(t.getId() < this.numberOfTrans){
                if(this.oldTransactions.containsKey(t.getId()))
                    commitRequest(t.getId(),o, t.getRequest());
                else
                    rollbackRequest(o,t.getId());
            }
        },es);

        this.channel.start();
    }

    private Boolean put(Map<Long,byte[]> values) {
        Set<Long> l = values.keySet();
        Long[] array =  l.toArray(new Long[l.size()]);

        return getLocks(array, (transaction, key) -> putRequest(transaction, key, values.get(key)), Tuple.Request.PUT)
                != null;
    }

    private Map<Long, byte[]> get(Collection<Long> gets) {
        Long[] array =  gets.toArray(new Long[gets.size()]);

        Map<Long,byte[]> hashMap = new HashMap<>();

        return getLocks(array,(transaction, key) ->getRequest(transaction,key,hashMap),Tuple.Request.GET)
                != null ? hashMap : null;
    }

    private List<Address> getLocks(Long[] array, BiPredicate<Integer,Long> getLock, Tuple.Request request ){
        int transaction = getNextTransactionId();
        Arrays.sort(array);
        List<Address> workersConfirm = new ArrayList<>();

        try {

            Arrays.stream(array).forEach(l ->
                    this.journal.addSegment(new Tuple(l,null, Tuple.Type.PREPARED,request,transaction)));
            for (Long key : array){
                if(getLock.test(transaction,key)){
                    workersConfirm.add(workers[getWorkerIndex(key)]);
                }
                else{
                    CompletableFuture.allOf(workersConfirm.stream()
                            .map(r -> rollbackRequest(r, transaction))
                            .toArray(CompletableFuture[]::new)).get();
                    return null;
                }
            }

            CompletableFuture.allOf(workersConfirm.stream()
                    .map(address -> commitRequest(transaction, address,request).thenRun(() -> { }))
                    .toArray(CompletableFuture[]::new)).get();

            return workersConfirm;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Boolean getRequest (int transactionId, Long key, Map<Long,byte[]> map)  {
        return preparedRequest(transactionId,key,null,Tuple.Request.GET,(r) ->{
            Tuple t = s.decode(r);
            if (t.getMsg().equals(Tuple.Type.OK)){
                map.put(t.getKey(),t.getValue());
                return true;
            }
            return false;
        });
    }

    private Boolean putRequest (int transactionId, Long key, byte[] value)  {
        return preparedRequest(transactionId,key,value,Tuple.Request.PUT,(r) ->{
            Tuple t = s.decode(r);
            return t.getMsg().equals(Tuple.Type.OK);
        });
    }

    private Boolean preparedRequest (int transactionId, Long key, byte[] value, Tuple.Request request, Predicate<byte []> consumer)  {
        try {
            return channel.sendAndReceive(
                    workers[getWorkerIndex(key)],
                    "PREPARE",
                    s.encode(new Tuple(key, value , Tuple.Type.PREPARED, request, transactionId)),es
                    )
                    .thenApply(consumer::test).get();
        } catch (InterruptedException | ExecutionException e) {
            return  false;
        }
    }

    private CompletableFuture<Void> commitRequest (int transactionId, Address worker, Tuple.Request request) {

        return channel.sendAsync(worker,"CONFIRM",s.encode(
                new Tuple(0,null, Tuple.Type.COMMIT,request,transactionId))).whenComplete((o,e) ->{
                    this.journal.addSegment(new Tuple(0,null, Tuple.Type.COMMIT,request,transactionId));
                    this.oldTransactions.put(transactionId,true);
        } );
    }

    private CompletableFuture<Void> rollbackRequest(Address address, int transactionId){
        return channel.sendAsync(address,"CONFIRM",s.encode(
                new Tuple(0,null, Tuple.Type.ROLLBACK, Tuple.Request.CANCEL, transactionId)));
    }

    private synchronized int getNextTransactionId(){
        return coordinators.length * (numberOfTrans++) + myId;
    }

    private int getWorkerIndex (Long number){
        return (int) (number % workers.length);
    }
}
