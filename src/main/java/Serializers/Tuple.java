package Serializers;


public class Tuple implements Transaction {

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

}
