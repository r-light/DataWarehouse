package ExportOutdate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HBaseOperation {
    private Configuration hconf;
    private HTable hTable;
    private Date t;
    private SimpleDateFormat df;
    private ResultScanner rs;

    private String file_name;
    private FileWriter fileWriter;

    public void Init() throws IOException {
        hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.quorum", "WSR,GSWB,XB,ZYF");
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        hTable = new HTable(hconf, "userbehavior");
        rs = hTable.getScanner(new Scan());

        t = new Date();
        df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        file_name = "/backup/outdated_data_"+df.format(t)+".txt";
        fileWriter = new FileWriter(file_name, true);
    }

    private List<byte[]> QueryAll() throws IOException, ParseException {
        List<byte[]> outdat_row = new ArrayList<byte[]>();;

        for(Result r:rs){
            byte[] row = r.getRow();
            byte[] time = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("BehaviorTime"));
            if(isOutdate(time.toString())){
                outdat_row.add(row);
            }
        }
        return outdat_row;
    }

    private boolean isOutdate(String behaviorTime) throws ParseException {
        Date t2 = df.parse(behaviorTime);
        long diff = t.getTime() - t2.getTime();
        double days = diff / (1000.0 * 60.0 * 60.0 * 24.0);

        if(days > 365)
            return true;
        else
            return false;
    }

    private void ExportOutdate(List<byte[]> delete_list) throws IOException {
        for(byte[] row:delete_list){
            Get get = new Get(row);
            Result r = hTable.get(get);

            String id = Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("Uid")));
            int behavior = Bytes.toInt(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("Behavior")));
            String aid = Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("Aid")));
            String behavior_time = Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("BehaviorTime")));
            String record = id + " " + behavior + " " + aid + " " + behavior_time;
            writeToFile(record);

            Delete d = new Delete(row);
            hTable.delete(d);
        }
    }

    private void writeToFile(String record) throws IOException {
        fileWriter.write(record);
        fileWriter.write("\r\n");
    }

    public void execute() throws IOException, ParseException {
        List<byte[]> outdate = QueryAll();
        ExportOutdate(outdate);
    }
}
