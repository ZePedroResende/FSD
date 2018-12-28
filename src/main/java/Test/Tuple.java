package Test;

public class Tuple {

    public static void main(String[] args){
        Serializers.Tuple t1 = new Serializers.Tuple( 1, null, Serializers.Tuple.Type.PREPARED, Serializers.Tuple.Request.GET, 1);
        Serializers.Tuple t2 = new Serializers.Tuple( 1, "ola".getBytes(), Serializers.Tuple.Type.PREPARED, Serializers.Tuple.Request.PUT, 2);
        Serializers.Tuple t3 = new Serializers.Tuple( 1, "ola".getBytes(), Serializers.Tuple.Type.ROLLBACK, Serializers.Tuple.Request.CANCEL, 3);
        Serializers.Tuple t4 = new Serializers.Tuple( 1, "ola".getBytes(), Serializers.Tuple.Type.COMMIT, Serializers.Tuple.Request.GET, 4);
        Serializers.Tuple t5 = new Serializers.Tuple( 1, "ola".getBytes(), Serializers.Tuple.Type.OK, Serializers.Tuple.Request.GET, 5);

        System.out.println( t1.toString() );
        System.out.println( t2.toString() );
        System.out.println( t3.toString() );
        System.out.println( t4.toString() );
        System.out.println( t5.toString() );
    }
}
