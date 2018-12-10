import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;



class Worker {

    private final ManagedMessagingService channel;
    private final ExecutorService executorService;
    private final SegmentedJournalWriter<Object> writer;

    private Map<Long, MyLock> locks;

    private Map<Long,MapValue> map;

    private Map<Integer, Tuple> puts;
    private Map<Integer, MapValue> gets;

    Worker(int myId,  Address myAddr) throws ExecutionException, InterruptedException {

        ///////////////// Initiation  /////////////////

        this.map = new ConcurrentHashMap<>();
        this.puts = new ConcurrentHashMap<>();
        this.gets= new ConcurrentHashMap<>();

        Serializer serializerTuple = new SerializerBuilder()
                .addType(Tuple.Type.class)
                .addType(Tuple.class)
                .build();

        SegmentedJournal<Object> j = SegmentedJournal.builder()
                .withName("worker-" + myId )
                .withSerializer( serializerTuple )
                .build();

        this.channel = NettyMessagingService.builder()
                .withAddress( myAddr )
                .build();

        this.executorService = Executors.newCachedThreadPool();


        ///////////////// Recover  /////////////////

        SegmentedJournalReader<Object> reader = j.openReader(0);

        System.out.println("[W" + myId + "] Start recover");

        recover( reader );

        reader.close();

        System.out.println(  mapToString( "[W" + myId +"] [RECOVER] " ));

        System.out.println("[W" + myId + "] Finnish recover");

        ///////////////// Open log File /////////////////

        this.writer = j.writer();

        ///////////////// Handlers  /////////////////

        this.channel.registerHandler( "put", (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            System.out.println("[W"+ myId + "] received \"put\" " + tuple.key + ": " + tuple.value == null ? "null" : new String(tuple.value) + " - transaction " + tuple.transId );

            // add to log file
            addLog(tuple);

            // get value from key
            MapValue value = new MapValue(null);
            MapValue aux =  map.putIfAbsent(  tuple.key,  value );

            if( aux != null) value = aux;

            // get lock
            value.getLockWrite();

            puts.put( tuple.transId, tuple);

            // reply to coordinator
            Tuple t = new Tuple( 0, null, Tuple.Type.OK, tuple.transId);
            channel.sendAsync( o,"reply", serializerTuple.encode(t) );

        }, executorService);

        this.channel.registerHandler( "get", (o, m) -> {

            Tuple tuple = serializerTuple.decode(m);

            System.out.println("[W"+ myId + "] received \"get\" "+  tuple.key + " - transaction " + tuple.transId );

            if( map.containsKey( tuple.key) ){
                // reply "ok"
                MapValue value = map.get(tuple.key);

                Tuple res = new Tuple(tuple.key, value.get() , Tuple.Type.OK, tuple.transId);

                gets.put( tuple.transId, value);
                channel.sendAsync(o, "reply", serializerTuple.encode( res) );

            }else{
                // reply "Rollback"
                Tuple res = new Tuple(tuple.key, null, Tuple.Type.ROLLBACK, tuple.transId);
                channel.sendAsync(o, "reply", serializerTuple.encode( res) );

                System.out.println("[W"+ myId + "] Rollback transaction " + tuple.transId);
            }

        }, executorService);

        this.channel.registerHandler( "reply", (o,m) ->{

            Tuple tuple = serializerTuple.decode(m);

            if(  ! ( gets.containsKey( tuple.transId) || puts.containsKey( tuple.transId))  )
                System.out.println("[W" + myId +"] received reply unknown transaction");

            System.out.println("[W"+ myId +"] "+ tuple.msg + " transaction " + tuple.transId);

            if( gets.containsKey( tuple.transId )){

                MapValue value = gets.get(tuple.transId);
                value.unlockRead();
                gets.remove( tuple.transId);

            }

            if(  puts.containsKey( tuple.transId)) {

                addLog(tuple);

                Tuple keyValue = puts.get( tuple.transId);

                MapValue value = map.get(keyValue.key);

                if ( tuple.msg.equals(Tuple.Type.COMMIT) )
                    value.put( keyValue.value );
                else{
                    // tenho que libertar o lock
                    // e remover caso tenho inserido
                }

                puts.remove( tuple.transId);
            }
        }, executorService);

        this.channel.start().get();
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

            if(  data.containsKey(tuple.transId)  ){

                if( tuple.msg.equals( Tuple.Type.COMMIT))
                    commit( data.get( tuple.transId) );
                else
                   data.get( tuple.transId ).add(tuple);

            }else{

                List<Tuple> l = new ArrayList<>();
                l.add( tuple);
                data.put( tuple.transId, l) ;
            }
        }
    }

    private void commit( List<Tuple> list ){

        for( Tuple t : list )
            map.put( t.key, new MapValue( t.value) );

    }

    private void addLog( Tuple t){

        writer.append( t );
        writer.flush();
    }

    public String mapToString(String info) {
        StringBuilder sb  = new StringBuilder();

        for( Long key : map.keySet()){

            byte[] b = map.get( key).get();
            String value =  b == null ? "null" : new String(b);
            sb.append( info + " " + key + " : " + value +"\n");
        }

        return sb.toString();
    }

}

