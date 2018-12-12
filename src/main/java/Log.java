import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

class Log{

    private final SegmentedJournal<Object> journal;
    private SegmentedJournalWriter<Object> writer;

    Log( String filename, Serializer serializer ) {

        this.journal = SegmentedJournal.builder()
                .withName( filename )
                .withSerializer( serializer )
                .build();
    }

    public void recover(Consumer<Transaction> handler) {

        if( writer != null )
            writer.close();

        SegmentedJournalReader<Object> reader = journal.openReader(0);

        Map< Integer, List<Transaction> > data = new HashMap<>();

        while( reader.hasNext() ) {

            Transaction trans = (Transaction) reader.next().entry();

            if(  data.containsKey( trans.getId() )  ){

                if( trans.isCommit() ){

                    for( Transaction t : data.get( trans.getId()) )
                        handler.accept( t );
                }
                else
                    data.get(trans.getId() ).add( trans );

            }else{

                List<Transaction> l = new ArrayList<>();
                l.add( trans );
                data.put( trans.getId(), l ) ;
            }
        }

        reader.close();

        this.writer = journal.writer();
    }

    public void addLog( Transaction t) {

        if( writer == null )
            writer = journal.writer();

        writer.append( t );
        writer.flush();
    }
}
