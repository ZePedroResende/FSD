public class Tuple implements Transaction{

    @Override
    public int getId() {
        return transId;
    }

    @Override
    public boolean isCommit() {
        return msg.equals(Type.COMMIT);
    }

    public enum Type {
        ROLLBACK,
        COMMIT,
        OK,
        PREPARED
    }

    final   long  key;
    final   byte[]  value;
    final   Type  msg;
    final   int    transId;


    public Tuple(long key, byte[] value, Type msg, int transId) {

        this.key = key;
        this.value = value;
        this.msg = msg;
        this.transId = transId;
    }
}
