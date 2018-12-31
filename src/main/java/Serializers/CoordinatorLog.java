package Serializers;

public class CoordinatorLog implements Transaction {
    private final   long  key;
    private final   byte[]  value;
    private final   Type  msg;
    private final   Request request;
    private final   int    transId;

    public CoordinatorLog(long key, byte[] value, Type msg, Request request, int transId) {
        this.key = key;
        this.value = value;
        this.msg = msg;
        this.request = request;
        this.transId = transId;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public boolean isCommit() {
        return false;
    }

    @Override
    public boolean isRollback() {
        return false;
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

}
