import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAdjusters;

public class UseParquet {
    public static void main(String[] args) {
        String parqAppLoaded = "D:\\Coding\\Java\\RTB_MEGA_FOLDER\\app_loaded.parquet";
        String parqRegistered = "D:\\Coding\\Java\\RTB_MEGA_FOLDER\\registered.parquet";

        SparkSession sparkSes = SparkSession.builder()
                .appName("RTB_Test")
                .getOrCreate();

        Dataset<Row> appLoadDS = sparkSes.read().parquet(parqAppLoaded);
        Dataset<Row> regDS = sparkSes.read().parquet(parqRegistered);

        Long registered = regDS.count();

        Dataset<Row> joinDS = regDS.join(appLoadDS, "email")
            .select(
                    regDS.col("time").as("regTime"),
                    appLoadDS.col("time").as("loadTime"),
                    regDS.col("email")
            );

        joinDS = joinDS.filter((Row row) -> {
                Timestamp regTime = row.getTimestamp(0);
                Timestamp loadTime = row.getTimestamp(1);
                Timestamp nextWeekStart = nextMon(regTime);
                Timestamp nextWeekEnd = nextMon(nextWeekStart);

                return loadTime.after(nextWeekStart) && loadTime.before(nextWeekEnd);
            })
        .select("email")
        .distinct();

        Long specialUsers = joinDS.count();
        System.out.println(Math.round(((float)specialUsers/(float)registered)*100) + "% among registered users are special");
    }

    public static Timestamp nextMon(Timestamp t){
        LocalDate ld = t
                .toLocalDateTime()
                .toLocalDate()
                .with(TemporalAdjusters.next( DayOfWeek.MONDAY ) );

        LocalDateTime ldt = LocalDateTime.of(ld, LocalTime.MIDNIGHT);

        return Timestamp.valueOf(ldt);
    }
}
