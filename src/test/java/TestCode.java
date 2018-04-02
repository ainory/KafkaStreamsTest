import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.math.BigDecimal;
import java.text.DecimalFormat;

/**
 * @author ainory on 2018. 3. 22..
 */
public class TestCode {

    public static void main(String[] args) {


//        System.out.println(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"));
//        System.out.println(DateFormatUtils.format(getStartTime(60), "yyyy-MM-dd HH:mm:ss"));

        /*DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##0.000");


        BigDecimal a = new BigDecimal( 1521700200649L * 0.001);*/

        int i = 1;
        float f = 1.0f;
        long l = 100L;
        double d= 1.0d;

        Number number  = new Integer(i);

//        System.out.println(NumberUtils.);


//        System.out.println(a.);

    }


    public static long getStartTime( int prevSec) {
        int itv = prevSec*1000;
        long ct = System.currentTimeMillis() - (prevSec*1000);
        long nct = ((ct/itv)*itv)+itv;
        return nct;
    }

    public static long getStartTime(long l, int prevSec) {
        int itv = prevSec*1000;
        long ct = l - (prevSec*1000);
        long nct = ((ct/itv)*itv)+itv;
        return nct;
    }
}
