package Coordinator;

import Serializers.*;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static Serializers.Tuple.Type.OK;
import static Serializers.Tuple.Type.PREPARED;

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
            try {
                Boolean b = put(requestPut.getValues());
                channel.sendAsync(o,"responsePut",respPutSer.encode(new ResponsePut(b)));
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        },es);


        Serializer reqGetSer= Serializer.builder().addType(RequestGet.class).build();
        Serializer respGetSer = Serializer.builder().addType(ResponseGet.class).build();

        channel.registerHandler( "get", (o, m) -> {
            RequestGet requestGet = reqGetSer.decode(m);
            try {
                Map<Long,byte[]> map = get(requestGet.getValues());
                channel.sendAsync(o,"responseGet",respGetSer.encode(new ResponseGet(map)));
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        },es);
    }

    private Boolean put(Map<Long,byte[]> values) throws ExecutionException, InterruptedException {
        int transaction = getNextTransactionId();
        Long[] array = (Long[]) values.keySet().toArray();
        Arrays.sort(array);
        List<Address> workersConfirm = new ArrayList<>();

        for (Long key : array){
            if(putRequest(transaction,key, values.get(key))){
                workersConfirm.add(workers[getWorkerIndex(key)]);
            }
            else{
                List<CompletableFuture<Void>> futuresCancel = workersConfirm.stream()
                        .map( r -> rollbackRequest(r,transaction))
                        .collect(Collectors.toList());
                CompletableFuture.allOf(futuresCancel.toArray(new CompletableFuture[futuresCancel.size()])).get();
                return false;
            }
        }

        List<CompletableFuture<Void>> futures = values.keySet().stream()
                .map(key -> putCommit(transaction,key).thenRun(() -> {}))
                .collect(Collectors.toList());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        return true;
    }

    private Map<Long, byte[]> get(Collection<Long> gets) throws ExecutionException, InterruptedException {
        Map<Long,byte[]> map = new ConcurrentHashMap<>();
        int transaction = getNextTransactionId();
        Long[] array = (Long[]) gets.toArray();
        Arrays.sort(array);
        List<Address> workersConfirm = new ArrayList<>();

        for (Long key : array){
            if(getRequest(transaction,key)){
                workersConfirm.add(workers[getWorkerIndex(key)]);
            }
            else{
                List<CompletableFuture<Void>> futuresCancel = workersConfirm.stream()
                        .map( r -> rollbackRequest(r,transaction))
                        .collect(Collectors.toList());
                CompletableFuture.allOf(futuresCancel.toArray(new CompletableFuture[futuresCancel.size()])).get();
                return null;
            }
        }

       List<CompletableFuture<byte[]>> futures = gets.stream()
               .map(key -> getCommit(transaction,key).thenApply(r -> map.put(key,r)))
               .collect(Collectors.toList());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();

        /*
        for (Long g : array){
            getCommit(transaction,g).thenApply(r -> map.put(g,r));
        }
        */

        return new HashMap<Long,byte[]>(map);
    }

    private Boolean getRequest (int transactionId, Long key) throws ExecutionException, InterruptedException {
            return request(transactionId, key,  null,  Tuple.Type.PREPARED,  Tuple.Request.GET)
                    .thenApply((r) -> {
                             Tuple t = s.decode(r);
                             return t.getMsg().equals(OK);
                         }).get();
    }

    private Boolean putRequest (int transactionId, Long key, byte[] value) throws ExecutionException, InterruptedException {
        return request(transactionId, key,  value,  Tuple.Type.PREPARED,  Tuple.Request.PUT)
                .thenApply((r) -> {
                    Tuple t = s.decode(r);
                    return t.getMsg().equals(OK);
                }).get();
    }

    private CompletableFuture<byte[]> getCommit (int transactionId, Long key) {
        return request(transactionId, key,  null,  Tuple.Type.COMMIT,  Tuple.Request.GET)
                .thenApply((r) -> {
                    Tuple t = s.decode(r);
                    return t.getValue();
                });
    }

    private CompletableFuture<Void> putCommit (int transactionId, Long key) {
        return channel.sendAsync(workers[getWorkerIndex(key)],"tuple",s.encode(
                new Tuple(key,null,Tuple.Type.COMMIT,Tuple.Request.PUT,transactionId)));
    }

    private CompletableFuture<Void> rollbackRequest(Address address, int transactionId){
       return channel.sendAsync(address,"Tuple",s.encode(new Tuple(0,null, Tuple.Type.ROLLBACK, Tuple.Request.CANCEL, transactionId)));
    }

    private CompletableFuture<byte[]> request(int transactionId, Long key, byte[] value, Tuple.Type type, Tuple.Request request){
        return channel.sendAndReceive(
                workers[getWorkerIndex(key)],
                "Tuple",
                s.encode(new Tuple(key, value , type, request, transactionId)),
                es);
    }

    private CompletableFuture<byte[]> request(Address address, int transactionId, Long key, byte[] value, Tuple.Type type, Tuple.Request request){
        return channel.sendAndReceive(
                address,
                "Tuple",
                s.encode(new Tuple(key, value , type, request, transactionId)),
                es);
    }


    private synchronized int getNextTransactionId(){
        return coordinators.length * (numberOfTrans++) + myId;
    }

    private int getWorkerIndex (Long number){
        return (int) (number % workers.length);
    }
}
