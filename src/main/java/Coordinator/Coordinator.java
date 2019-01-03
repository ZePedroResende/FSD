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
    private final ExecutorService es1;
    private final Serializer s;
    private int numberOfTrans;
    private Map<Integer,Map.Entry<Integer,String>> oldTransactions;
    private Map<Integer,Map.Entry<Integer,String>> oldRollbacks;
    private Journal journal;
    private static final boolean DEBUG = true;

    public Coordinator(Address[] coordinators , Address[] workers, int myId) {
        this.coordinators = coordinators;
        this.workers = workers;
        this.myId = myId;
        this.channel = NettyMessagingService.builder().withAddress(coordinators[myId]).build();
        this.es = Executors.newSingleThreadExecutor();
        this.es1 = Executors.newSingleThreadExecutor();
        this.s = Serializer.builder()
                .addType(Tuple.Request.class)
                .addType(Tuple.Type.class)
                .addType(Tuple.class)
                .addType(CoordinatorTuple.class)
                .build();
        this.oldTransactions = new ConcurrentHashMap<>();
        this.oldRollbacks= new ConcurrentHashMap<>();
        this.journal = new Journal( "coordinator"+ myId, s);


        Serializer reqPutSer = new SerializerBuilder().addType(Map.class).addType(RequestPut.class).build();
        Serializer respPutSer = new SerializerBuilder().addType(Boolean.class).addType(ResponsePut.class).build();

        channel.registerHandler( "put", (o, m) -> {
            System.out.println("PUT");
            RequestPut requestPut = reqPutSer.decode(m);
            int idClient = requestPut.getId();
            Boolean b = put(requestPut.getValues(),idClient,o.toString());


            this.channel.sendAsync(o,
                    "put", respPutSer.encode(new ResponsePut(b,idClient)));
           // return respPutSer.encode(new ResponsePut(b,idClient));

        },es);

        Serializer reqGetSer= new SerializerBuilder().addType(Collection.class).addType(RequestGet.class).build();
        Serializer respGetSer =new SerializerBuilder().addType(Map.class).addType(ResponseGet.class).build();

        channel.registerHandler( "get", (o, m) -> {
            System.out.println("GET");
            RequestGet requestGet = reqGetSer.decode(m);
            int idClient = requestGet.getId();
            Map<Long,byte[]> map = get(requestGet.getValues(),idClient,o.toString());

            this.channel.sendAsync(o,
                    "get", respGetSer.encode(new ResponseGet(map,idClient)));
            //return respGetSer.encode(new ResponseGet(map,idClient));
        },es);

        channel.registerHandler("RETRY",  (o, m) -> {
            Tuple t = this.s.decode(m);
            System.out.println("RETRY id =" + t.getId() );
            if(t.getId() < this.numberOfTrans){
                if(this.oldTransactions.containsKey(t.getId())){
                    Map.Entry<Integer,String> entry = this.oldTransactions.get(t.getId());
                    commitRequest(t.getId(),o, t.getRequest(),entry.getKey(),entry.getValue());
                    if(t.getRequest().equals(Tuple.Request.GET)){
                        this.channel.sendAsync(Address.from(entry.getValue()),
                                "get", respGetSer.encode(new ResponseGet(null,entry.getKey())));
                    } else{
                        this.channel.sendAsync(Address.from(entry.getValue()),
                                "put", respGetSer.encode(new ResponsePut(true,entry.getKey())));
                    }
                }
                else{
                    Map.Entry<Integer,String> entry = this.oldRollbacks.get(t.getId());
                    rollbackRequest(o,t.getId(),entry.getKey(),entry.getValue());
                    if(t.getRequest().equals(Tuple.Request.GET)){
                        this.channel.sendAsync(Address.from(entry.getValue()),
                                "get", respGetSer.encode(new ResponseGet(null,entry.getKey())));
                    } else{
                        this.channel.sendAsync(Address.from(entry.getValue()),
                                "put", respGetSer.encode(new ResponsePut(false,entry.getKey())));
                    }
                }

            }
        },es);

        try {
            this.channel.start().thenCompose((o) -> makeRecover() ).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private Boolean put(Map<Long,byte[]> values, int idClient, String clientAddress) {
        Set<Long> l = values.keySet();
        Long[] array =  l.toArray(new Long[l.size()]);

        return getLocks(array, (transaction, key) -> putRequest(transaction, key, values.get(key)), Tuple.Request.PUT, idClient,clientAddress)
                != null;
    }

    private Map<Long, byte[]> get(Collection<Long> gets, int idClient, String clientAddress) {
        Long[] array =  gets.toArray(new Long[gets.size()]);

        Map<Long,byte[]> hashMap = new HashMap<>();

        return getLocks(array,(transaction, key) ->getRequest(transaction,key,hashMap),Tuple.Request.GET, idClient, clientAddress)
                != null ? hashMap : null;
    }

    private List<Address> getLocks(Long[] array, BiPredicate<Integer,Long> getLock, Tuple.Request request, int idClient, String clientAddress) {
        int transaction = getNextTransactionId();
        Arrays.sort(array);
        List<Address> workersConfirm = new ArrayList<>();

        try {

            Arrays.stream(array).forEach(key ->
                    this.journal.addSegment(new CoordinatorTuple(key,null, Tuple.Type.PREPARED,request,transaction,workers[getWorkerIndex(key)],idClient,clientAddress)));
            for (Long key : array){
                if(getLock.test(transaction,key)){
                    workersConfirm.add(workers[getWorkerIndex(key)]);
                    this.journal.addSegment(new CoordinatorTuple(key,null, Tuple.Type.OK,request,transaction, workers[getWorkerIndex(key)], idClient,clientAddress));
                }
                else{
                    CompletableFuture.allOf(workersConfirm.stream()
                            .map(r -> rollbackRequest(r, transaction,idClient,clientAddress))
                            .toArray(CompletableFuture[]::new)).get();
                    return null;
                }
            }
            CompletableFuture.allOf(workersConfirm.stream()
                    .map(address -> commitRequest(transaction, address,request,idClient,clientAddress).thenRun(() -> { }))
                    .toArray(CompletableFuture[]::new)).get();

            return workersConfirm;
        } catch (InterruptedException | ExecutionException e) {
            //e.printStackTrace();
            return null;
        }
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
                    s.encode(new Tuple(key, value , Tuple.Type.PREPARED, request, transactionId)),es1
                    )
                    .thenApply(consumer::test).get();
        } catch (InterruptedException | ExecutionException e) {
            //e.printStackTrace();
            return  false;
        }
    }

    private CompletableFuture<Void> commitRequest (int transactionId, Address worker, Tuple.Request request, int idClient, String clientAddress) {

        return channel.sendAsync(worker,"CONFIRM",s.encode(
                new Tuple(0,null, Tuple.Type.COMMIT,request,transactionId))).whenComplete((o,e) ->{
                    System.out.println("Commit transaction " + transactionId);
                    this.journal.addSegment(new CoordinatorTuple(0,null, Tuple.Type.COMMIT,request,transactionId,worker,idClient, clientAddress));
                    this.oldTransactions.put(transactionId,new AbstractMap.SimpleEntry<>(idClient,clientAddress));
        } );
    }

    private CompletableFuture<Void> rollbackRequest(Address address, int transactionId, int idClient, String clientAddress) {
        return channel.sendAsync(address,"CONFIRM",s.encode(
                new Tuple(0,null, Tuple.Type.ROLLBACK, Tuple.Request.CANCEL, transactionId))).whenComplete((o,e) ->{
            System.out.println("Rollback transaction " + transactionId);
            this.journal.addSegment(new CoordinatorTuple(0,null, Tuple.Type.ROLLBACK,Tuple.Request.CANCEL,transactionId,address,idClient, clientAddress));
            this.oldRollbacks.put(transactionId,new AbstractMap.SimpleEntry<>(idClient,clientAddress));
        });
    }

    private CompletableFuture<Void> makeRecover(){
        if(DEBUG) System.out.println("[C"+this.myId+"] Start recover");
        CompletableFuture cp = new CompletableFuture();
        try{
            return  cp;
        }finally {

            List< Transaction > list = null;
            try {
                list = journal.getCommitted().get();

                for( Transaction t : list) {
                    CoordinatorTuple tuple = (CoordinatorTuple) t;
                    oldTransactions.put(tuple.getId(), new AbstractMap.SimpleEntry<>(tuple.getIdClient(),tuple.getAddressClient()));
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            list = null;
            try {
                list = journal.getUnconfirmed().get();

                for( Transaction t : list) {
                    CoordinatorTuple tuple = (CoordinatorTuple) t;
                    oldRollbacks.put(tuple.getId(), new AbstractMap.SimpleEntry<>(tuple.getIdClient(),tuple.getAddressClient()));
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            try {
                numberOfTrans = journal.getTransactionId().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            try {
                list = journal.getLastUnconfirmed().get();
                if(list.size() != 0){
                    if(list.get(0).isOk()){
                        for (Transaction t : list) {
                            CoordinatorTuple tuple = (CoordinatorTuple) t;
                            commitRequest(t.getId(),workers[getWorkerIndex( tuple.getKey())],tuple.getRequest(),tuple.getIdClient(),tuple.getAddressClient());
                        }
                    }else{
                        for (Transaction t : list) {
                            CoordinatorTuple tuple = (CoordinatorTuple) t;
                            rollbackRequest(workers[getWorkerIndex( tuple.getKey())],t.getId(),tuple.getIdClient(),tuple.getAddressClient());
                        }
                    }
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            cp.complete(null);
            if(DEBUG) System.out.println("[C"+this.myId+"] Finish recover");
        }
    }

    private synchronized int getNextTransactionId(){
        return coordinators.length * (numberOfTrans++) + myId;
    }

    private int getWorkerIndex (Long number){
        return (int) (number % workers.length);
    }
}
