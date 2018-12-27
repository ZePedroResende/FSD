import Serializers.Tuple;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;


class Worker {

    private final ManagedMessagingService channel;
    private final ExecutorService executorService;
<<<<<<< HEAD

    private Map< Long, byte[] > map;

    private final Log log;
=======
    private final SegmentedJournalWriter<Object> writer;

    private Map<Long, MyLock> locks;
    private Map<Long, byte[]> myHashMap;
    private Map<Integer, List<Tuple>> transactions;
    private Integer id;
>>>>>>> worker asac

    Worker(int myId,  Address myAddr) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////

<<<<<<< HEAD
        this.map = new ConcurrentHashMap<>();
=======
        this.locks = new ConcurrentHashMap<Long, MyLock>();
        this.myHashMap = new ConcurrentHashMap<Long, byte[]>();
        this.transactions = new ConcurrentHashMap<Integer, List<Tuple>>();
        this.id = myId;
>>>>>>> worker asac

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

        this.channel.registerHandler( "Tuple", (BiConsumer<Address, byte[]>) (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            System.out.println("[W"+ myId + "] received \"put\" " + tuple.getKey() + ": " + tuple.getValue() == null ? "null" : new String(tuple.getValue()) + " - transaction " + tuple.getTransId() );

            Tuple.Type msg = tuple.getMsg();
            if(msg.equals(Tuple.Type.PREPARED)){

                locks.get(tuple.getKey())
                        .lock(tuple.getTransId())
                        .thenRun(() -> {
                            channel.sendAsync(o, "Tuple", serializerTuple.encode(
                                    new Tuple(tuple.getKey(),
                                            tuple.getValue(),
                                            Tuple.Type.OK,
                                            tuple.getRequest(),
                                            tuple.getTransId()
                                    )
                                    )
                            );
                        });

                if(transactions.containsKey(tuple.getTransId())){
                    List<Tuple> list = this.transactions.get(tuple.getTransId());
                    list.add(tuple);
                    this.transactions.put(tuple.getTransId(),list);
                }
                else{
                    List<Tuple> list = new ArrayList<>();
                    list.add(tuple);
                    this.transactions.put(tuple.getTransId(), list);
                }

            }

            if(msg.equals(Tuple.Type.ROLLBACK)){

                this.transactions.remove(tuple.getTransId());

            }

            if(msg.equals(Tuple.Type.COMMIT)){

            }

        }, executorService);

    }

    private void recover( SegmentedJournalReader<Object> r ){
        /*
            Temos que melhorar. Falta apagar as que ja nao sao usadas. Falta tambem recuperar as transações que nao
            foram confirmadas nem abortadas. Neste caso temos que dar reply ao coordenador ou entao optar por abortar
            sempre.
         */

        Map< Integer, List<Tuple> > data = new HashMap<>();

        while( r.hasNext() ) {

            Tuple tuple = (Tuple) r.next().entry();

            if(  data.containsKey(tuple.getTransId())  ){

                if( tuple.getMsg().equals( Tuple.Type.COMMIT))
                    commit( data.get( tuple.getTransId()) );
                else
                   data.get( tuple.getTransId() ).add(tuple);

            }else{

                List<Tuple> l = new ArrayList<>();
                l.add( tuple);
                data.put( tuple.getTransId(), l) ;
            }
        }
    }

    private void commit( List<Tuple> list ){

        for( Tuple t : list )
            myHashMap.put( t.getKey(), t.getValue() );

    }

    private void addLog( Tuple t){

        writer.append( t );
        writer.flush();
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
*/
