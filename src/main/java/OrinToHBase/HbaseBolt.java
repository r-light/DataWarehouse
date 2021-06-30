package OrinToHBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HbaseBolt extends BaseBasicBolt {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(HbaseBolt.class);
    private Connection connection = null;
    private HBaseAdmin hBaseAdmin = null;
    private Table table = null;
    private String tableName;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "172.31.17.200,172.31.17.26,172.31.17.29");
        conf.set("hbase.master", "172.31.17.200:60000");
        try {
            connection = ConnectionFactory.createConnection(conf);
            hBaseAdmin = (HBaseAdmin) connection.getAdmin();

        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line = tuple.getStringByField("msg");
        String datas[] = line.split(",");
        getTableName(datas[0]);

        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = insertHbase(datas[0], datas[datas.length-1], datas);  // 插入数据至HBase中
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void getTableName(String datatype){
        if(datatype == "0")
            tableName = "articleInfo";
        else if(datatype == "1")
            tableName = "userBasic";
        else if(datatype == "2")
            tableName = "userBehavior";
        else if(datatype == "3")
            tableName = "userEdu";
        else if(datatype == "4")
            tableName = "userInterest";
        else if(datatype == "5")
            tableName = "userSkill";
        else;
    }

    private Put insertHbase(String datatype, String count, String datas[]){
        Put put = new Put(Bytes.toBytes(String.valueOf(count)));
        if(datatype == "0"){
            put.addColumn("cf".getBytes(), "Aid".getBytes(), datas[1].getBytes());
            put.addColumn("cf".getBytes(), "DiggCount".getBytes(), datas[2].getBytes());
            put.addColumn("cf".getBytes(), "BuryCount".getBytes(),  datas[3].getBytes());
            put.addColumn("cf".getBytes(), "ViewCount".getBytes(),  datas[4].getBytes());
            put.addColumn("cf".getBytes(), "CommentCount".getBytes(), datas[5].getBytes());
            put.addColumn("cf".getBytes(), "Type".getBytes(), datas[6].getBytes());
            put.addColumn("cf".getBytes(), "IsTop".getBytes(),  datas[7].getBytes());
            put.addColumn("cf".getBytes(), "Status".getBytes(),  datas[8].getBytes());
        }
        else if(datatype == "1"){
            put.addColumn("cf".getBytes(), "Uid".getBytes(), datas[1].getBytes());
            put.addColumn("cf".getBytes(), "Gender".getBytes(), datas[2].getBytes());
            put.addColumn("cf".getBytes(), "Status".getBytes(),  datas[3].getBytes());
            put.addColumn("cf".getBytes(), "FollowNum".getBytes(),  datas[4].getBytes());
            put.addColumn("cf".getBytes(), "FansNum".getBytes(), datas[5].getBytes());
            put.addColumn("cf".getBytes(), "FriendNum".getBytes(), datas[6].getBytes());
        }
        else if(datatype == "2"){
            put.addColumn("cf".getBytes(), "Uid".getBytes(), datas[1].getBytes());
            put.addColumn("cf".getBytes(), "Behavior".getBytes(), datas[2].getBytes());
            put.addColumn("cf".getBytes(), "Aid".getBytes(),  datas[3].getBytes());
            put.addColumn("cf".getBytes(), "BehaviorTime".getBytes(),  datas[4].getBytes());
        }
        else if(datatype == "3"){
            put.addColumn("cf".getBytes(), "Uid".getBytes(), datas[1].getBytes());
            put.addColumn("cf".getBytes(), "Degree".getBytes(), datas[2].getBytes());
            put.addColumn("cf".getBytes(), "SchoolName".getBytes(),  datas[3].getBytes());
            put.addColumn("cf".getBytes(), "MajorStr".getBytes(),  datas[4].getBytes());
        }
        else if(datatype == "4"){
            put.addColumn("cf".getBytes(), "Uid".getBytes(), datas[1].getBytes());
            put.addColumn("cf".getBytes(), "InterestName".getBytes(), datas[2].getBytes());
        }
        else if(datatype == "5"){
            put.addColumn("cf".getBytes(), "Uid".getBytes(), datas[1].getBytes());
            put.addColumn("cf".getBytes(), "SkillName".getBytes(), datas[2].getBytes());
        }
        else;

        return put;
    }
}