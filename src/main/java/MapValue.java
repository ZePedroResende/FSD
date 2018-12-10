import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*

Class StampedLock

 */
public class MapValue {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private byte[] valeu;

    MapValue(byte[] value){
        this.valeu = value;
    }

    public void getLockWrite(){
        w.lock();
    }

    public void getLockWrite(){
        w.lock();
    }

    public void put( byte[] valeu){
        this.valeu = valeu;
        w.unlock();
    }

    public byte[] get(){
        r.lock();
        return valeu;
    }

    public void unlockRead(){
        r.unlock();
    }
}
