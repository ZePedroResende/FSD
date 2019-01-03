package Journal;

import Serializers.CoordinatorTuple;
import Serializers.Transaction;

import Serializers.Tuple;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Journal {

    private SegmentedJournal<Object> j;

    private SegmentedJournalReader<Object> reader;
    private SegmentedJournalWriter<Object> writer;

    public Journal(String fileName, Serializer s) {

        j = SegmentedJournal.builder()
                .withName(fileName)
                .withSerializer(s)
                .build();

        reader = j.openReader(0);
        writer = null;
    }

    public void close(){
        if ( reader != null) reader.close();
        if ( writer != null) writer.close();
        j.close();
    }

    public void addSegment( Transaction t) {

        if (reader != null) {
            reader.close();
            reader = null;
            this.writer = j.writer();
        }

        writer.append(t);
        writer.flush();
    }

    public CompletableFuture< List<Transaction> > getCommitted() {

        CompletableFuture< List<Transaction> > cp = new CompletableFuture<>();

        try{
            return cp;
        }finally {
            cp.complete( filterLog(true));
        }

    }

    public CompletableFuture< List<Transaction> > getUnconfirmed () {

        CompletableFuture<List<Transaction>> cp = new CompletableFuture<>();

        try{
            return cp;
        }finally {
            cp.complete( filterLog(false ) );
        }
    }

    public CompletableFuture< List<Transaction> > getLastUnconfirmed () {

        CompletableFuture<List<Transaction>> cp = new CompletableFuture<>();

        try{
            return cp;
        }finally {
            cp.complete(getLastTransaction());
        }
    }

    public CompletableFuture<Integer> getTransactionId(){
        CompletableFuture<Integer> cp = new CompletableFuture<>();

        try {
            return cp;
        }finally {
            cp.complete(transactionId());
        }
    }

    private List<Transaction> filterLog( boolean flag) {
        // flag == true -> return just committed transactions
        // flag == false -> return just unconfirmed transactions

        List<Transaction> committed = new ArrayList<>();

        if (writer != null) {
            writer.close();
            writer = null;
            reader = j.openReader(0);
        }
        reader = j.openReader(0);

        Map< Integer, List<Transaction> > data = new HashMap<>();

        while (reader.hasNext()) {

            Transaction t = (Transaction) reader.next().entry();

            if ( t.isCommit() ) {

                List<Transaction> list = data.get( t.getId() );
                committed.addAll( list );
                data.remove( t.getId() );
            }else {

                if ( t.isRollback() ){
                    data.remove( t.getId() );
                }else {

                    if ( ! data.containsKey( t.getId() ) )
                        data.put( t.getId(), new ArrayList<>() );

                    data.get( t.getId() ).add(t);
                }
            }
        }

        return  flag ?  committed : data.values().stream()
                                                 .flatMap( List::stream )
                                                 .collect(Collectors.toList());
    }

    private List<Transaction> getLastTransaction(){

        List<Transaction> unconfirmed = new ArrayList<>();
        List<Transaction> aux ;
        Transaction last = null;
        reader = j.openReader(0);

        while (reader.hasNext()) {

            last = (Transaction) reader.next().entry();

        }

        reader = j.openReader(0);
        if(last == null) return unconfirmed;
        if(last.isCommit() ){
            int id = last.getId();
            unconfirmed = filterLog(true);
            unconfirmed.addAll(filterLog(false));
            unconfirmed.removeIf(l -> l.getId() != id );
            unconfirmed.removeIf(l -> l.isPrepare() || l.isRollback() || l.isCommit());
            Transaction finalLast = last;
            unconfirmed.removeIf(l -> ((CoordinatorTuple) l).getAddress().equals(((CoordinatorTuple) finalLast).getAddress()));

            Set<Address> addresses = unconfirmed.stream()
                    .map(l -> ((CoordinatorTuple) l).getAddress())
                    .collect(Collectors.toSet());
            if(addresses.size() > 0){

                int idClient = ((CoordinatorTuple) unconfirmed.get(0)).getIdClient();
                String addressClient = ((CoordinatorTuple) unconfirmed.get(0)).getAddressClient();

                unconfirmed = addresses.stream()
                        .map( address -> new CoordinatorTuple(0,null, Tuple.Type.OK,((CoordinatorTuple) finalLast).getRequest(),id,address,idClient, addressClient))
                        .collect(Collectors.toList());

            }
        } else {

            int id = last.getId();
            unconfirmed = filterLog(false);
            aux = new ArrayList<>(unconfirmed);
            unconfirmed.removeIf(l -> l.getId() != id );
            unconfirmed.removeIf(l -> l.isPrepare());
            aux.removeIf(l -> l.isOk());

            Transaction finalLast = last;
            if(unconfirmed.size() != aux.size()) unconfirmed = aux;

            Set<Address> addresses = unconfirmed.stream()
                    .map(l -> ((CoordinatorTuple) l).getAddress())
                    .collect(Collectors.toSet());

            if(addresses.size() > 0){
                int idClient = ((CoordinatorTuple) unconfirmed.get(0)).getIdClient();
                String addressClient = ((CoordinatorTuple) unconfirmed.get(0)).getAddressClient();

                unconfirmed = addresses.stream()
                        .map( address -> new CoordinatorTuple(0,null, Tuple.Type.ROLLBACK,Tuple.Request.CANCEL,id , address, idClient, addressClient))
                        .collect(Collectors.toList());
            }

        }

        return unconfirmed;
    }

    private int transactionId(){
        Transaction last = null;
        reader = j.openReader(0);

        while (reader.hasNext()) {

            last = (Transaction) reader.next().entry();

        }

        if(last == null) return 0;
        return last.getId() +1;
    }
}

