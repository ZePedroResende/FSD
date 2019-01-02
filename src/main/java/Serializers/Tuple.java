package Serializers;


public class Tuple implements Transaction {

    public enum Type {
        ROLLBACK,
        COMMIT,
        OK,
        PREPARED
    }

    public enum Request {
        GET,
        PUT,
        CANCEL
    }

    final   long  key;
    final   byte[]  value;
    final   Type  msg;
    final   Request request;
    final   int    transId;


    public Tuple(long key, byte[] value, Type msg, Request request, int transId) {

        this.key = key;
        this.value = value;
        this.msg = msg;
        this.request = request;
        this.transId = transId;
    }

    public Tuple( Tuple origin, byte[] newValue, Type mewMsg) {
         this.key =  origin.getKey();
         this.value = newValue;
         this.msg = mewMsg;
         this.request = origin.getRequest();
         this.transId = origin.getId();
    }

    @Override
    public int getId() {
        return this.transId;
    }

    @Override
    public boolean isCommit() {
        return Type.COMMIT.equals( this.msg );
    }

    @Override
    public boolean isRollback() {
        return Type.ROLLBACK.equals( this.msg );
    }

    @Override
    public boolean isPrepare() {
        return Type.PREPARED.equals( this.msg );
    }

    @Override
    public boolean isOk() {
        return Type.OK.equals( this.msg );
    }


    public long getKey() {
        return key;
    }

    public Request getRequest() {
        return request;
    }

    public byte[] getValue() {
        return value;
    }

    public Type getMsg() {
        return msg;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append( "Id: " ).append( transId)
                                .append(" - ")
                                .append( msg);
        if( ! msg.equals(Type.ROLLBACK) && ! msg.equals(Type.COMMIT) ){

            sb.append("  ").append( request );

            if( request.equals(Request.GET) && msg.equals(Type.PREPARED))
                sb.append(" " + key);
            if( request.equals( Request.PUT) || ( request.equals(Request.GET) && msg.equals(Type.OK)) )
                sb.append(" " + key).append(" -> ").append( value != null ? new String( value): "NULL" );
        }

        return sb.toString();
    }


}
