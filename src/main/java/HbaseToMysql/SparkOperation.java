package HbaseToMysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.execution.columnar.INT;
import org.apache.spark.sql.execution.columnar.STRING;
import org.ini4j.Wini;
import scala.Tuple2;
import scala.collection.Seq;

import java.beans.Transient;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class SparkOperation implements Serializable {
    private transient Configuration configuration;
    private transient JavaSparkContext sc;
    private transient Configuration hconf;
    private transient Connection conn;

    private static final int degreeMax = 15;

    private String FAMILY;
    private String table_userB;
    private String COLUM_ID;
    private String table_userE;
    private String COLUM_Deg;
    private String table_userI;
    private String COLUM_Int;
    private String table_behavior;
    private String COLUM_Beh;
    private String COLUM_BT;

    private PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, String> userIDStore;
    private PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, String> userInterestStore;
    private PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, Integer> userEducationStore;
    private PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, UserAggre> userBehaviorStore;
    private Function2<UserAggre, UserAggre, UserAggre> aggreteUserBehavior;

    private JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDDPair;
    private JavaPairRDD<ImmutableBytesWritable, Result> InterRDDPair;
    private JavaPairRDD<ImmutableBytesWritable, Result> EduRDDPair;
    private JavaPairRDD<ImmutableBytesWritable, Result> BehaviorRDDPair;

    public String getFamily(String tableName) {
        return tableName.toLowerCase();
    }

    public void Init() throws SQLException, ClassNotFoundException, IOException {
        /*
        Configuration做为Hadoop的一个基础功能承担着重要的责任，为Yarn、HSFS、MapReduce、NFS、调度器等提供参数的配置、配置文件的分布式传输(实现了Writable接口)等重要功能。
        在hadoop-common-project子工程中，它的实现子类有：HdfsConfiguration、YarnConfiguration、JobConf、NfsConfiguration、FairSchedulerConfiguration等。
         */

        configuration = new Configuration();
        table_userB = "userBasic";
        COLUM_ID = "Uid";
        table_userE = "userEdu";
        COLUM_Deg = "Degree";
        table_userI = "userInterest";
        COLUM_Int = "InterestName";
        table_behavior = "userBehavior";
        COLUM_Beh = "Behavior";
        COLUM_BT = "BehaviorTime";

        SparkInit();
        HBaseInit();
        System.out.println("Init finished");
    }

    private void SparkInit(){
        // sparkConf: Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
        // setAppName: Set a name for your application. Shown in the Spark web UI.
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkDataFromHbase")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.setMaster("spark://cluster1:7077")
                .registerKryoClasses(new Class[]{ImmutableBytesWritable.class, Result.class, Configuration.class});
        // Create a JavaSparkContext that loads settings from system properties (for instance, when launching with ./bin/spark-submit).
        sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
    }

    private void HBaseInit(){
        //Creates a Configuration with HBase resources
        hconf = HBaseConfiguration.create(configuration);
        hconf.set("hbase.zookeeper.quorum", "cluster1,cluster2,cluster3");
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    private void MySqlInit() throws SQLException, ClassNotFoundException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://172.31.17.26:3306/lab" +
                            "?useUnicode=true&autoReconnect=true&failOverReadOnly=false",
                    "root",
                    "cluster");
        }
        catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }
    }

    public JavaPairRDD<Integer, String> extractTableUserB() throws IOException {
        List<String> col = new ArrayList<String>();
        //COLUM_ID=Uid
        col.add(COLUM_ID);
        FAMILY = getFamily(table_userB);
        hBaseRDDPair = select(table_userB, FAMILY, col);
        userIDStore = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, String>() {
            private static final long serialVersionUID = -2437063503351644147L;
            @Override
            public Tuple2<Integer, String> call(
                    Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                //取列的值(uid)
                byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ID));
                if (o1 == null || o1.length == 0)
                    return null;
                String id = new String(o1);
                int key = Integer.parseInt(id.substring(1));
                return new Tuple2<Integer, String>(key, id);
            }
        };

        return hBaseRDDPair.mapToPair(userIDStore);
    }

    public JavaPairRDD<Integer, Integer> extractTableUserE() throws IOException {
        List<String> col = new ArrayList<String>();
        //COLUM_ID=Uid
        col.add(COLUM_ID);
        //COLUM_Deg=Degree
        col.add(COLUM_Deg);
        FAMILY = getFamily(table_userE);
        EduRDDPair = select(table_userE, FAMILY, col);
        userEducationStore = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, Integer>() {
            private static final long serialVersionUID = -2437063503351644147L;
            @Override
            public Tuple2<Integer, Integer> call(
                    Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                //取列UID的值
                byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ID));
                //取列DEGREE的值
                byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_Deg));
                if (o1 == null || o1.length == 0)
                    return null;
                String id = new String(o1);
                int degree = Integer.parseInt(new String(o2));
                if (degree > degreeMax)
                    degree = -1;
                int key = Integer.parseInt(id.substring(1));
                return new Tuple2<>(key, degree);
            }
        };
        return EduRDDPair.mapToPair(userEducationStore);
    }

    public JavaPairRDD<Integer, String> extractTableUserI() throws IOException {
        List<String> col = new ArrayList<String>();
        //COLUM_ID=Uid
        col.add(COLUM_ID);
        //COLUM_ID=InterestName
        col.add(COLUM_Int);
        FAMILY = getFamily(table_userI);
        InterRDDPair = select(table_userI, FAMILY, col);
        userInterestStore = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, String>() {
            private static final long serialVersionUID = -2437063503351644147L;
            @Override
            public Tuple2<Integer, String> call(
                    Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ID));
                byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_Int));
                if (o1 == null || o1.length == 0)
                    return null;
                String id = new String(o1);
                String interest = "null";
                if (o2.length != 0)
                    interest = new String(o2);
                int key = Integer.parseInt(id.substring(1));
                return new Tuple2<>(key, interest);
            }
        };

        return InterRDDPair.mapToPair(userInterestStore);
    }

    public JavaPairRDD<String, UserAggre> extractTableUserBehavior() throws IOException {
        List<String> col = new ArrayList<String>();
        col.add(COLUM_ID);
        col.add(COLUM_Beh);
        col.add(COLUM_BT);

        FAMILY = getFamily(table_behavior);
        BehaviorRDDPair = select(table_behavior, FAMILY, col);
        userBehaviorStore = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, UserAggre>() {
                private static final long serialVersionUID = -2437063503351644147L;
                @Override
                public Tuple2<String, UserAggre> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ID));
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_Beh));
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_BT));
                    if (o1 == null || o1.length == 0)
                        return null;
                    String id = new String(o1);
                    int behavior = Integer.parseInt(new String(o2));
                    String behavior_time = new String(o3);
                    UserAggre temp = new UserAggre(id);
                    switch (behavior) {
                        case 0:
                            temp.setPUBLISH(1);
                            break;
                        case 1:
                            temp.setVIEW(1);
                            break;
                        case 2:
                            temp.setCOMMENT(1);
                            break;
                    }
                    temp.setLAST_TIME(behavior_time);
                    return new Tuple2<>(id, temp);
                }
            };

        return BehaviorRDDPair.mapToPair(userBehaviorStore);

    }
    public void execute() throws IOException, SQLException, ClassNotFoundException {
        // 提取用户的基础信息，即UID
        System.out.println("before extractTableUserB");
        JavaPairRDD<Integer, String> resultBasic = extractTableUserB();
        System.out.println("after extractTableUserB num: " + resultBasic.count());

        resultBasic = resultBasic.sortByKey();
        resultBasic.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                if (tuple2._1 == null || tuple2._2 == null)
                    System.out.println(tuple2._1 + " " + tuple2._2);
            }
        });

        // 提取用户的教育信息
        System.out.println("before extractTableUserE");
        JavaPairRDD<Integer, Integer> resultEDU = extractTableUserE();
        System.out.println("after extractTableUserE num: " + resultEDU.count());

        resultEDU = resultEDU.sortByKey();
        resultEDU.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                if (tuple2._1 == null || tuple2._2 == null)
                    System.out.println(tuple2._1 + " " + tuple2._2);
            }
        });

        // 提取用户的兴趣
        System.out.println("before extractTableUserI");
        JavaPairRDD<Integer, String> resultInter = extractTableUserI();
        System.out.println("after extractTableUserI num: " + resultInter.count());

        resultInter = resultInter.sortByKey();
        resultInter.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                if (tuple2._1 == null || tuple2._2 == null)
                    System.out.println(tuple2._1 + " " + tuple2._2);
            }
        });

        // 提取用户的行为，即浏览评论等行为
        System.out.println("before extractTableUserBehavior");
        JavaPairRDD<String, UserAggre> resultBehave = extractTableUserBehavior();
        System.out.println("after extractTableUserBehavior num: " + resultBehave.count());

        // 聚合用户的行为，同一个id
        System.out.println("before aggreteUserBehavior");
        aggreteUserBehavior = new Function2<UserAggre, UserAggre, UserAggre>() {
            @Override
            public UserAggre call(UserAggre userAggre, UserAggre userAggre2) throws Exception {
                UserAggre temp = new UserAggre(userAggre.getID());
                temp.setPUBLISH(userAggre.getPUBLISH() + userAggre2.getPUBLISH());
                temp.setCOMMENT(userAggre.getCOMMENT() + userAggre2.getCOMMENT());
                temp.setVIEW(userAggre.getVIEW() + userAggre2.getVIEW());
                String t1 = userAggre.getLAST_TIME();
                String t2 = userAggre2.getLAST_TIME();
                long long1 = Long.parseLong(t1.replaceAll("[-\\s.:]",""));
                long long2 = Long.parseLong(t2.replaceAll("[-\\s.:]",""));
                temp.setLAST_TIME(long1 > long2 ? t1 : t2);
                return temp;
            }
        };
        
        // 聚合用户的教育背景和uid
        JavaPairRDD<String, UserAggre> upload = resultBehave.reduceByKey(aggreteUserBehavior);
        System.out.println("after aggreteUserBehavior num: " + upload.count());

        final List<Tuple2<String, UserAggre>> aggreList = new ArrayList<>();
        System.out.println("before aggreteAll aggreList " + aggreList.size());

        final Map<String, UserAggre> aggreMap = upload.collectAsMap();
        System.out.println("before aggreteAll aggreMap " + aggreMap.size());

        final Map<Integer, String> basicMap = resultBasic.collectAsMap();
        final Map<Integer, Integer> eduMap = resultEDU.collectAsMap();
        final Map<Integer, String> interMap = resultInter.collectAsMap();

        for(Map.Entry<Integer, String> entry : basicMap.entrySet()){
            String ID = entry.getValue();
            int id = entry.getKey();
            UserAggre userAggre = aggreMap.get(ID);
            if (userAggre == null)  continue;
            if (eduMap.containsKey(id))
                userAggre.setEDU(eduMap.get(id));
            else
                userAggre.setEDU(-1);
            if (interMap.containsKey(id))
                userAggre.setINTER(interMap.get(id));
            aggreList.add(new Tuple2<>(ID, userAggre));
        }

        // 插入mysql
        System.out.println("after aggreteAll: aggreList " + aggreList.size());
        upload = sc.parallelizePairs(aggreList);

        MySqlInit();
        VoidFunction<Tuple2<String, UserAggre>> InsertMysql = new VoidFunction<Tuple2<String, UserAggre>>() {
            @Override
            public void call(Tuple2<String, UserAggre> tuple2) throws Exception {
                String ID = tuple2._2.getID();
                int EDU = tuple2._2.getEDU();
                String INT = tuple2._2.getINTER();
                int PUBLISH = tuple2._2.getPUBLISH();
                int VIEW = tuple2._2.getVIEW();
                int COMMENT = tuple2._2.getCOMMENT();
                String LAST_TIME = tuple2._2.getLAST_TIME();

                String sql = "INSERT into UserInfo(Uid, Degree, InterestName, Publish, View, Comment, LastTime) VALUES(?,?,?,?,?,?,?)";
                while (conn == null) {
                    System.out.println("conn null");
                    MySqlInit();
                }
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setString(1, ID);
                ps.setInt(2, EDU);
                ps.setString(3, INT);
                ps.setInt(4, PUBLISH);
                ps.setInt(5, VIEW);
                ps.setInt(6, COMMENT);
                ps.setString(7, LAST_TIME);
                ps.executeUpdate();
            }
        };
        upload.foreach(InsertMysql);
    }

    private JavaPairRDD<ImmutableBytesWritable, Result> select(String tableName, String family, List<String> column) throws IOException {
        //通过设置conf.set(TableInputFormat.INPUT_TABLE, tableName);设定HBase的输入表
        hconf.set(TableInputFormat.INPUT_TABLE, tableName);

        Scan scan = new Scan();
        /*指定需要的family或column ，如果没有调用任何addFamily或Column，会返回所有的columns;
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));*/
        scan.addFamily(Bytes.toBytes(family));

        for(String col:column)
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));

        /*Google Protocol Buffers 是一种轻便高效的结构化数据存储格式，可以用于结构化数据串行化，或者说序列化。
        它很适合做数据存储或 RPC 数据交换格式。可用于通讯协议、数据存储等领域的语言无关、平台无关、可扩展的序列化结构数据格式。
        目前提供了 C++、Java、Python 三种语言的 API。*/
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        //Base64是一种能将任意Binary资料用64种字元组合成字串的方法，而这个Binary资料和字串资料彼此之间是可以互相转换的，十分方便。在实际应用上
        String scanToString = Base64.encodeBytes(proto.toByteArray());
        //设定对HBase输入表的scan方式；
        hconf.set(TableInputFormat.SCAN, scanToString);

        return sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
    }
}
