package OrinToHBase;

import java.io.FileNotFoundException;
import java.io.IOException;

public class ProducerMain {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String base_path = "C:\\Users\\Jiao\\Downloads\\Compressed\\Datasets_3\\";
		String articleInfo = "articleInfo\\articleInfo";
		String userBasic = "userBasic\\userBasic";
		String userBehavior = "userBehavior\\userBehavior";
		String userEdu = "\\userEdu\\userEdu";
		String userInterest = "\\userInterest\\userInterest";
		String userSkill = "\\userSkill\\userSkill";

		KafkaProducer kp_1 = new KafkaProducer("test", base_path + articleInfo);
		kp_1.execute();

		KafkaProducer kp_2 = new KafkaProducer("test", base_path +userBasic);
		kp_2.execute();

		KafkaProducer kp_3 = new KafkaProducer("test", base_path + userBehavior);
		kp_3.execute();

		KafkaProducer kp_4 = new KafkaProducer("test", base_path +userEdu);
		kp_4.execute();

		KafkaProducer kp_5 = new KafkaProducer("test", base_path + userInterest);
		kp_5.execute();

		KafkaProducer kp_6 = new KafkaProducer("test", base_path + userSkill);
		kp_6.execute();
	}

}
