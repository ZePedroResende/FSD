package Journal;

import Serializers.Transaction;

import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
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

    private List<Transaction> filterLog( boolean flag) {
        // flag == true -> return just committed transactions
        // flag == false -> return just unconfirmed transactions

        List<Transaction> committed = new ArrayList<>();

        if (writer != null) {
            writer.close();
            writer = null;
            reader = j.openReader(0);
        }

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

        if (writer != null) {
            writer.close();
            writer = null;
            reader = j.openReader(0);
        }

        Transaction last = (Transaction) j.openReader(j.maxEntrySize()).getCurrentEntry();
        if(last.isOk() || last.isPrepare()){
            int id = last.getId();
            unconfirmed = filterLog(false);
            unconfirmed.removeIf(l -> l.getId() != id );
            unconfirmed.removeIf(l -> l.isOk());
        }

        return unconfirmed;
    }
}

