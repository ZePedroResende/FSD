package Config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


public class Config {
    /*
     ATENCAO
     Tem que se decidir o que vamos meter no ficheiro para meter as
     variaveis todas direitas. Depois é so gerar os gets
     */
    
    private boolean debug;
    private int NumCoordinators;
    private int NumWorkers;


    private Config(boolean debug, int numCoordinators, int numWorkers) {
        this.debug = debug;
        NumCoordinators = numCoordinators;
        NumWorkers = numWorkers;
    }

    public static final String filePath = "./src/main/java/config.json";

    public static Config loadConfig() throws IOException {

        Gson gson = new GsonBuilder().create();

        Path path = new File(filePath ).toPath();

        Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);

        return gson.fromJson(reader,  Config.class);
    }

    public String toString(){

        StringBuilder sb = new StringBuilder();
        sb.append( "Debug = ").append(debug).append("\n");
        sb.append( "NumCoordinators = ").append(NumCoordinators).append("\n");
        sb.append( "NumWorkers = ").append(NumWorkers).append("\n");
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        Config c = Config.loadConfig();
        System.out.println(c.toString());
    }
}
