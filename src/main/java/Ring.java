import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

public class Ring {
    public static void main(String[] args) throws IOException {
        String localHostAddress = "127.0.0.1:";
        ArrayList<String> ports = new ArrayList<>();
        int nodeCount = readNodeCount();
        for (int i = 0; i < nodeCount; i++) {
            ports.add(String.valueOf(55000 + i));
        }
        for (int i = 0; i < nodeCount; i++) {
            new Node(localHostAddress + ports.get(i % nodeCount), localHostAddress + ports.get((i + 1) % nodeCount), i == 0);
        }
    }

    private static int readNodeCount() throws IOException {
        Properties nodeProps = new Properties();
        nodeProps.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread()
                .getContextClassLoader()
                .getResource("config.properties"))
                .getPath()));
        return Integer.parseInt(nodeProps.getProperty("nodeCount"));
    }
}
