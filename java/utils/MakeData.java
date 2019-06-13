package utils;


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Random;
import java.util.UUID;

/**
 * 模拟数据程序
 *
 * @author Administrator
 */
public class MakeData {


    /**
     * 模拟数据
     *
     * @param sc
     * @param sqlContext
     */

    public static void main(String[] args) throws Exception {

        for(int x=20;x<50;x++) {
            String out = null;
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("d:/test/"+x+".txt"));
            String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                    "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};

            //生成的数据日期为当天日期，设置搜索参数时应注意搜索时间范围
            String date = DateUtils.getTodayDate();
            String[] actions = new String[]{"search", "click", "order", "pay"};
            Random random = new Random();

            for (int i = 0; i < 10000; i++) {
                long userid = random.nextInt(100);

                for (int j = 0; j < 10; j++) {
                    String sessionid = UUID.randomUUID().toString().replace("-", "");
                    String baseActionTime = date + " " + random.nextInt(23);

                    for (int k = 0; k < random.nextInt(100); k++) {
                        long pageid = random.nextInt(10);
                        String actionTime = baseActionTime + ":" + StringUtils.fulfuill(
                                String.valueOf(random.nextInt(59))) + ":"
                                + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                        String searchKeyword = null;
                        Long clickCategoryId = null;
                        Long clickProductId = null;
                        String orderCategoryIds = null;
                        String orderProductIds = null;
                        String payCategoryIds = null;
                        String payProductIds = null;

                        String action = actions[random.nextInt(4)];
                        if ("search".equals(action)) {
                            searchKeyword = searchKeywords[random.nextInt(10)];
                        } else if ("click".equals(action)) {
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                            clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        } else if ("order".equals(action)) {
                            orderCategoryIds = String.valueOf(random.nextInt(100));
                            orderProductIds = String.valueOf(random.nextInt(100));
                        } else if ("pay".equals(action)) {
                            payCategoryIds = String.valueOf(random.nextInt(100));
                            payProductIds = String.valueOf(random.nextInt(100));
                        }

                        out = date + "\001" + userid + "\001" + sessionid + "\001" + pageid + "\001" + actionTime + "\001" + searchKeyword + "\001" +
                                clickCategoryId + "\001" + clickProductId + "\001" +
                                orderCategoryIds + "\001" + orderProductIds + "\001" +
                                payCategoryIds + "\001" + payProductIds + "\n";
                        bufferedWriter.write(out);
                    }
                }
                bufferedWriter.flush();
            }
        }
    }
}
