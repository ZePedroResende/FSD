package Test;

import API.API;
import API.Middleware;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class APITest extends Thread {

    private static final int NUMCLIENTS = 1;

    public static void main(String[] args) {
        /*
         */

    }


    private  Middleware md;

    APITest( String port){
        this.md = new Middleware(port);
    }

    @Override
    public void run() {
        while (true){
            try {
                test1();
                Thread.sleep(100);
            } catch (Exception e) { System.out.println(e); }

        }
    }

    private int test1() throws ExecutionException, InterruptedException {
        /* Just test a put and a get */

        Map<Long, byte[]> map = new HashMap<>();
        map.put(1L, "Ola".getBytes());
        map.put(2L, "Mundo".getBytes());
        map.put(3L, "Lindo".getBytes());

        if (!md.put(map).get())
            return 1;

        List<Long> l = new ArrayList<>();
        l.add(1L);
        l.add(2L);
        l.add(3L);

        return map.equals( md.get(l).get() ) ? 0 : 2;
    }

    private int test2(){
        return 0;
    }

}
