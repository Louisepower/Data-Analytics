package comgooglecloud;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableId;
import org.threeten.bp.Duration;

import java.io.*;
import java.util.Calendar;

public class QuickstartSample {
  public static void main(String... args) throws Exception {

   // BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    GoogleCredentials credentials;
    File credentialsPath = new File("/Users/louise/SoftFiles/application_default_credentials.json");//请修改为json授权文件存放路径
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
    System.out.println(credentialsPath);
    BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    /**
     * 将BQ中Query结果保存到表格
     */
    int y,m,d,h,mi,s;
    Calendar cal=Calendar.getInstance();
    y=cal.get(Calendar.YEAR);
    m=cal.get(Calendar.MONTH)+1;
    d=cal.get(Calendar.DATE);
    h=cal.get(Calendar.HOUR_OF_DAY);
    mi=cal.get(Calendar.MINUTE);
    s=cal.get(Calendar.SECOND);
    System.out.println("执行开始时间是"+y+"年"+m+"月"+d+"日"+h+"时"+mi+"分"+s+"秒");
    String datasetId = "TM_test";

    File file = new File("/Users/louise/SQL4");//SQL文件路径
    String files[];
    files=file.list();
    int num = files.length;
    System.out.println("tables下文件夹个数："+num);

    for(int i=0;i<files.length;i++)
    {
      //从文件中读取Query 语句
      System.out.println(files[i]);
      String Tablename =files[i].replaceAll(".txt", "");
      String tableId =y+"_"+m+"_"+d+"_"+h+"_"+mi+"_"+s+"_"+ Tablename ;

      File Queryfiles = new File(file +"/"+files[i]);;
      System.out.println(txt2String(Queryfiles));

      //Query to Table
      runQueryPermanentTable(datasetId, tableId, Queryfiles);
      System.out.println("Query to Table is Done!");

      //Export to Cloud
      String cloudStoragePath = "gs://louise_test_data/"+ tableId +".csv";
      extractSingle(datasetId ,tableId,"CSV",cloudStoragePath);
      System.out.println("Export is Done!");

      //Delete Table from Bigquery
      deleteTable(datasetId, tableId);
      System.out.println("Delete is Done!");

    }

  }


  /**
   * 读取txt文件的内容
   * @param file 想要读取的文件对象
   * @return 返回文件内容
   */
  public static String txt2String(File file){
    StringBuilder result = new StringBuilder();
    try{
      BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
      String s = null;
      while((s = br.readLine())!=null){//使用readLine方法，一次读一行
        result.append(System.lineSeparator()+s);
      }
      br.close();
    }catch(Exception e){
      e.printStackTrace();
    }
    return result.toString();
  }


  /**
   * Save Query Result To Table.
   */
  public static void runQueryPermanentTable(String destinationDataset, String destinationTable, File file)
          throws InterruptedException {
     BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    String query = txt2String(file);
    QueryJobConfiguration queryConfig =

            QueryJobConfiguration.newBuilder(query)
                    .setUseLegacySql(true)
                    .setDestinationTable(TableId.of(destinationDataset, destinationTable))
                    .setAllowLargeResults(true)
                    .build();
                   bigquery.query(queryConfig).iterateAll();
    }
  /**
   * Extracting data to single Google Cloud Storage file.
   */
  public static void extractSingle(String datasetId,String tableId,String format, String gcsUrl) {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    TableId tableId1 = TableId.of(datasetId,tableId);
    Table table = bigquery.getTable(tableId1);
    Job job = table.extract(format, gcsUrl);
    try {
      Job completedJob =
              job.waitFor(
                      RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                      RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob != null && completedJob.getStatus().getError() == null) {
        // Job completed successfully
      } else {
        // Handle error case
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  /**
   * Delete table in Bigquery
   */
  public static boolean deleteTable(String datasetName, String tableName) {
    // [START ]
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    boolean deleted = bigquery.delete(datasetName, tableName);
    if (deleted) {
      int y,m,d,h,mi,s;
      Calendar cal=Calendar.getInstance();
      y=cal.get(Calendar.YEAR);
      m=cal.get(Calendar.MONTH)+1;
      d=cal.get(Calendar.DATE);
      h=cal.get(Calendar.HOUR_OF_DAY);
      mi=cal.get(Calendar.MINUTE);
      s=cal.get(Calendar.SECOND);
      System.out.println("执行完成时间是"+y+"年"+m+"月"+d+"日"+h+"时"+mi+"分"+s+"秒");
    } else {
      // the table was not found
    }
    // [END ]
    return deleted;
  }
}

