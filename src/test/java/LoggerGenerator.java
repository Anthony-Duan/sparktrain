import org.apache.log4j.Logger;

/**
 * @ Description: 模拟日志产生
 * @ Date: Created in 12:01 2018/4/6
 * @ Author: Anthony_Duan
 */
public class LoggerGenerator {

    public static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws InterruptedException {

        int index = 0;
        while (true) {
            Thread.sleep(1000);
            logger.info("value:"+index++);
        }

    }

}
