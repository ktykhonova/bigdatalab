package workingWithCassandraDB;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class CassandraConnector {

    public static final String KEYSPACE = "uservisitsrecords";
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 9042;

    public CassandraConnector() {
        connect(HOST, PORT);
    }

    private Cluster cluster;

    private Session session;

    public void connect(String node, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect(KEYSPACE);
    }

    public Session getSession() {

        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
}