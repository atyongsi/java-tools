import org.example.postgresql.PostgresqlToJson;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by atyongsi@163.com on 2020/8/25
 * Description:Have Not Yet
 */
public class TestPg2Pg {
    public static void main(String[] args) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("datax-generate-configs/src/main/resource/read"));
        String[] readInfo = br.readLine().split("\\|");
        BufferedReader bw = new BufferedReader(new FileReader("datax-generate-configs/src/main/resource/write"));
        String[] writeInfo = bw.readLine().split("\\|");

        String json = PostgresqlToJson.postgreSqlToJson(readInfo, writeInfo, true, 3);

        System.out.println(json);

    }
}
