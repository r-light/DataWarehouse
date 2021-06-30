package ExportOutdate;

import java.io.IOException;
import java.text.ParseException;

public class ExportMain {
    public static void main(String arg[]) throws IOException, ParseException {
        HBaseOperation hbo = new HBaseOperation();
        hbo.Init();
        hbo.execute();
    }
}
