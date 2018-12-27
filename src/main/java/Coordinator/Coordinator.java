package Coordinator;

import Serializers.*;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;


public class Coordinator {

    private final Address[] workers;
    private final Address[] coordinators;
    private int myId;
    private final ManagedMessagingService channel;
    private ExecutorService es;
    private Serializer s;

    private int numberOfTrans;

    public Coordinator(Address[] coordinators , Address[] workers, int myId) {
        this.coordinators = coordinators;
        this.workers = workers;
        this.myId = myId;
        this.channel = NettyMessagingService.builder().withAddress(coordinators[myId]).build();
        this.es = Executors.newSingleThreadExecutor();
        this.s = Serializer.builder().addType(Tuple.class).build();

        Serializer reqPutSer = Serializer.builder().addType(RequestPut.class).build();
        Serializer respPutSer = Serializer.builder().addType(ResponsePut.class).build();

        channel.registerHandler( "put", (o, m) -> {
            RequestPut requestPut = reqPutSer.decode(m);
                Boolean b = put(requestPut.getValues());
                channel.sendAsync(o,"responsePut",respPutSer.encode(new ResponsePut(b)));
        },es);


        Serializer reqGetSer= Serializer.builder().addType(RequestGet.class).build();
        Serializer respGetSer = Serializer.builder().addType(ResponseGet.class).build();

        channel.registerHandler( "get", (o, m) -> {
            RequestGet requestGet = reqGetSer.decode(m);
                Map<Long,byte[]> map = get(requestGet.getValues());
                channel.sendAsync(o,"responseGet",respGetSer.encode(new ResponseGet(map)));
        },es);

        this.channel.start();
    }

    private Boolean put(Map<Long,byte[]> values) {
        Long[] array = (Long[]) values.keySet().toArray();

        return getLocks(array, (transaction, key) -> putRequest(transaction, key, values.get(key)), Tuple.Request.PUT)
                != null;
    }

    private Map<Long, byte[]> get(Collection<Long> gets) {
        Long[] array = (Long[]) gets.toArray();
        Map<Long,byte[]> hashMap = new HashMap<>();

        return getLocks(array,(transaction, key) ->getRequest(transaction,key,hashMap),Tuple.Request.GET)
                != null ? hashMap : null;

    }

    private List<Address> getLocks(Long[] array, BiPredicate<Integer,Long> getLock, Tuple.Request request ){
        int transaction = getNextTransactionId();
        Arrays.sort(array);
        List<Address> workersConfirm = new ArrayList<>();

        try {
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
                    "Tuple",
                    s.encode(new Tuple(key, value , Tuple.Type.PREPARED, request, transactionId)),
                    es)
                    .thenApply(consumer::test).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return  false;
    }

    private CompletableFuture<Void> commitRequest (int transactionId, Address worker, Tuple.Request request) {
        return channel.sendAsync(worker,"Tuple",s.encode(
                new Tuple(0,null, Tuple.Type.COMMIT,request,transactionId)));
    }

    private CompletableFuture<Void> rollbackRequest(Address address, int transactionId){
       return channel.sendAsync(address,"Tuple",s.encode(
               new Tuple(0,null, Tuple.Type.ROLLBACK, Tuple.Request.CANCEL, transactionId)));
    }

    private synchronized int getNextTransactionId(){
        return coordinators.length * (numberOfTrans++) + myId;
    }

    private int getWorkerIndex (Long number){
        return (int) (number % workers.length);
    }
}
