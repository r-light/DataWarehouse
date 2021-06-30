package RecommendAlgo;

public class RecEngine {
    // 读取用户基础信息
    public static String readBasicData(){
        return "user info";
    }

    // 读取行为数据
    public static String readBehaviorData(){
        return "user behavior";
    }

    // 计算特征向量
     public static String calculateFeature(){
        return "features";
     }

     //获得推荐的初始结果
     public static String recommandRes(){
        return "results";
     }

     // 过滤初始的结果
     public static String filter(){
        return "results";
     }

     // 对过滤后的结果进行排序
     public static String sort(){
        return "results";
     }

     //推荐引擎主程序
     public static void main(String[] args){
        // 首先读取数据，并进行处理
         readBasicData();
         readBehaviorData();

         //接着计算特征，给下一步计算推荐结果准备
         calculateFeature();

         //计算推荐结果，并进行过滤和排序，得到最终结果
         recommandRes();
         filter();
         sort();
     }



}
