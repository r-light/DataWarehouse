package HbaseToMysql;

import java.io.IOException;
import java.sql.SQLException;

public class AggregateMain {
    public static void main(String arg[]) throws IOException, SQLException, ClassNotFoundException {
        SparkOperation so = new SparkOperation();
        so.Init();
        so.execute();
    }
}
