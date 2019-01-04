package Test;

import API.API;
import API.Middleware;
import Coordinator.Coordinator;
import Worker.Worker;
import Config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class APITest implements Runnable {


    private  Middleware md;

    public APITest( String port){
        try {
            this.md = new Middleware(Integer.parseInt(port) , Config.loadConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (true){
                if (test1() == 0) {
                    System.out.println("SUCESS");
                } else {
                    System.out.println("FAILURE");
                }
                ;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private int test1() {
        /* Just test a put and a get */

        Map<Long, byte[]> map = new HashMap<>();
        map.put(1L, "Ola".getBytes());
        map.put(2L, "Mundo".getBytes());
        map.put(3L, "Lindo".getBytes());

        try {
            if (!md.put(map).get()){
                System.out.println("PUT FALHOU");
                return 1;
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        List<Long> l = new ArrayList<>();
        l.add(1L);
        l.add(2L);
        l.add(3L);
        try {
          return  map.entrySet().stream()
                    .map(a -> new String(a.getValue()))
                    .collect(Collectors.toSet())
                    .equals(
                            md.get(l).get().entrySet().stream()
                                    .map(a -> new String(a.getValue())).collect(Collectors.toSet())) ? 0: 2;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return 3;
    }

    private int test2(){
        return 0;
    }

}

class Coordenador implements Runnable {


    private  Coordinator c;

    public Coordenador(int i){
        try {
            this.c = new Coordinator(i , Config.loadConfig());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
    }

}


class Wor implements Runnable {


    private  Worker w;

    public Wor(int i){
        try {
            this.w = new Worker(i , Config.loadConfig());
        } catch (ExecutionException | InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
    }

}

class RunMiddleware{


    public static void main(String[] args) {
        /*
         */

        Config c = null;

        try {
            c = Config.loadConfig();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int NUMCLIENTS = 1;
        //if(c == null) c = Config.defaultConfig();


        Thread clients[] = new Thread[NUMCLIENTS];
        Thread worker[] = new Thread[c.getNumWorkers()];
        Thread coordinator[] = new Thread[c.getNumCoordinators()];

        for(int i =0; i<c.getNumWorkers(); i++){
            worker[i] = new Thread(new Wor(i));
        }

        for(int i =0; i<c.getNumWorkers(); i++){
            worker[i].start();
        }

        for(int i =0; i<c.getNumCoordinators(); i++){
            coordinator[i] = new Thread(new Coordenador(i));
        }

        for(int i =0; i<c.getNumCoordinators(); i++){
            coordinator[i].start();
        }

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(int i =0; i<NUMCLIENTS; i++){
            clients[i] = new Thread(new APITest(String.format("33%03d", i)));
        }

        for(int i =0; i<NUMCLIENTS; i++){
            clients[i].start();
        }


    }
}
