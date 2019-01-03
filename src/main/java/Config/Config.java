package Config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


public class Config {

    public static final String filePath = "./src/main/java/config.json";

    public static Config loadConfig() throws IOException {

        Gson gson = new GsonBuilder().create();

        Path path = new File(filePath ).toPath();

        Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);

        return gson.fromJson(reader,  Config.class);
    }

    private final boolean debugMode;
    private final int numCoordinators;
    private final int numWorkers;


    public Config(boolean debugMode, int numCoordinators, int numWorkers) {
        this.debugMode = debugMode;
        this.numCoordinators = numCoordinators;
        this.numWorkers = numWorkers;
    }

    public boolean getDebugMode() {
        return debugMode;
    }

    public int getNumCoordinators() {
        return numCoordinators;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public static String getFilePath() {
        return filePath;
    }

    public String toString(){

        StringBuilder sb = new StringBuilder();
        sb.append( "Debug = ").append(debugMode).append("\n");
        sb.append( "NumCoordinators = ").append(numCoordinators).append("\n");
        sb.append( "NumWorkers = ").append(numWorkers).append("\n");
        return sb.toString();
    }

}
