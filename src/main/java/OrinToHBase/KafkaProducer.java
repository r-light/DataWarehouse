package OrinToHBase;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaProducer {
    private String file_path;   //数据源路径
    private List<String> content;   //暂存本次发送的日志内容
    File file;  //数据源文件
    BufferedReader buf;
    private String previous_readTime;   //上一次抓取数据的时间
    private int count = 0;
    private String datatype;

    private String topic = "my_kafka";   //topic名称
    private Producer<String, String> producer;
    private Properties properties;

    public KafkaProducer(String topic, String file_path)throws FileNotFoundException{
        this.topic = topic;
        this.file_path = file_path;
        previous_readTime = "";
        file = new File(file_path);
        buf = new BufferedReader(new FileReader(file));
        content = new ArrayList<String>();
        count = 0;
        getDataType();
        createProducer();
    }

    private void createProducer(){
        properties = new Properties();
        properties.put("zookeeper.connect", "172.31.17.200:2181,172.31.17.26:2181,172.31.17.29:2181");//声明zk
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "172.31.17.200:9092");// 声明kafka broker
//        properties.put("bootstrap.servers", "172.31.17.200:9092");
        producer = new Producer<String, String>(new ProducerConfig(properties));
//        producer = new KafkaProducer<>(props); 
    }

    private int readFileContent() throws IOException {
        int loop_flag = 1;

        while(loop_flag == 1){
            String line = buf.readLine();
            if(line == null)
                return -1;  //数据读取完毕，返回-1
            count++;
            String datas[] = line.split("\001");
            String temp = datatype;
            for (int i = 0; i < datas.length; i++){
                temp = temp + "," + datas[i];
            }
            temp = temp + "," + count;
            content.add(temp);

            if (previous_readTime.compareTo(datas[3]) != 0) {
                loop_flag = 0;
                previous_readTime = datas[3];
            }
        }
        return 1;
    }

    public void execute() throws IOException {
        int run_flag = 1;
        System.out.println("A producer started");
        long startTime =  System.currentTimeMillis();
        while(run_flag == 1){
            run_flag = readFileContent();   //读取文件数据

            for(String sendMes : content){  //发送消息缓冲池的消息
            	System.out.println(sendMes);
            	KeyedMessage msg = new KeyedMessage<String, String>(topic, count+"", sendMes);
            	System.out.println(msg);
                producer.send(msg);
            	
            }

            content.clear();

        }
        long endTime =  System.currentTimeMillis();
        double usedTime = (endTime-startTime)/1000.0;
        producer.send(new KeyedMessage<String, String>(topic, (count)+"","end"));

        System.out.println("This producer ran "+usedTime+" s");
    }

    private void getDataType(){
        if(file_path.contains("articleInfo"))
            datatype = "0";
        else if(file_path.contains("userBasic"))
            datatype = "1";
        else if(file_path.contains("userBehavior"))
            datatype = "2";
        else if(file_path.contains("userEdu"))
            datatype = "3";
        else if(file_path.contains("userInterest"))
            datatype = "4";
        else if(file_path.contains("userSkill"))
            datatype = "5";
        else;
    }

}
