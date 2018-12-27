package Test;

import Journal.Journal;

import Serializers.Transaction;
import Serializers.Tuple;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;

class JournalTest {

    private static final String file = "test";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Serializer s = new SerializerBuilder()
                .addType(Tuple.Type.class)
                .addType(Tuple.Request.class)
                .addType(Tuple.class)
                .build();

        Journal j = new Journal(file, s);

        int count = 0;

        List<Transaction> committed = j.getCommitted().get();
        j.close();
        j = new Journal(file, s);
        List<Transaction> unconfirmed = j.getUnconfirmed().get();
        j.close();
        j = new Journal(file, s);

        if( committed.size() != 0)
            System.out.println("Erro: teste 1 - " + committed.toString());
        else count ++;

        if( unconfirmed.size() != 0)
            System.out.println("Erro: teste 2 - " + unconfirmed.toString());
        else count ++;

        j.addSegment(new Tuple(1L, null, Tuple.Type.PREPARED, Tuple.Request.GET, 1));
        j.addSegment(new Tuple(2L, null, Tuple.Type.PREPARED, Tuple.Request.GET, 1));
        j.addSegment(new Tuple(5L, null, Tuple.Type.PREPARED, Tuple.Request.GET, 2));
        j.addSegment(new Tuple(9L, null, Tuple.Type.PREPARED, Tuple.Request.GET, 2));
        j.addSegment(new Tuple(3L, null, Tuple.Type.PREPARED, Tuple.Request.GET, 3));
        j.addSegment(new Tuple(4L, null, Tuple.Type.PREPARED, Tuple.Request.GET, 3));

        j.addSegment(new Tuple(1L, null, Tuple.Type.COMMIT,   Tuple.Request.GET, 1));
        j.addSegment(new Tuple(2L, null, Tuple.Type.ROLLBACK, Tuple.Request.GET, 2));

        committed = j.getCommitted().get();
        j.close();
        j = new Journal(file, s);
        unconfirmed = j.getUnconfirmed().get();
        j.close();

        if( committed.size() != 2)
            System.out.println("Erro: teste 3 - " + committed.toString() );
        else count ++;

        if( unconfirmed.size() != 2)
            System.out.println("Erro: teste 4 - " + unconfirmed.toString() );
        else count ++;

        System.out.println( count + "/4 testes corretos! :D") ;

        new File("test-1.log").delete();

    }

}

