package ManagementAPISample;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;

import com.google.api.client.util.DateTime;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.AnalyticsScopes;
import com.google.api.services.analytics.model.*;
import com.jcraft.jsch.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.io.*;
import java.security.GeneralSecurityException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Thread.sleep;


/**
 * A simple example of how to access the Google Analytics API using a service
 * account.
 */
public class Upload2Ga {

    private static final String APPLICATION_NAME = "Hello Analytics";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
//  private static final String KEY_FILE_LOCATION = "/home/cupshe/jsonkey/truemetrics-222507-2bed10a82266.json";
    private static final String KEY_FILE_LOCATION = "/Users/louise/cupshe/truemetrics-222507-2bed10a82266.json";
    public static void main(String[] args) {
        /** 日志 **/
        Log logger = LogFactory.getLog(Upload2Ga.class);

        /**获取当前日期*/
        Date d = new Date();
        DateTime TodaydateTime = new DateTime(d.getTime());
        logger.info("当前时间：" + TodaydateTime);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        /** 配置sftp 路径 **/
        String remotePath = "/home/cupshe/data";

        /** 配置文件路径 **/
        String TodayFilePath = "/home/cupshe/data/ga_import_product_" + sdf.format(d) + ".csv";
//      String localPath = "/home/cupshe/importdata/ga_import_product_" + sdf.format(d) + ".csv";
        String localPath = "/Users/louise/cupshe/importdata/ga_import_product_" + sdf.format(d) + ".csv";

        /** 获取GA待上传文件 **/
        File file = new File(localPath);

        /** 配置cupshe GA账户信息 **/
//        String AccountId = "60224307" ;
//        String WebPropertyId = "UA-60224307-1" ;
//        String CustomDataSourceId = "0d91XyLyTya9kw56M-Hw7A" ;


        /** 触脉测试 GA账户信息 **/
           String AccountId = "28338210";
           String WebPropertyId = "UA-28338210-5";
           String CustomDataSourceId = "KfCciZXLQV6v2X5C3ZWoTQ";


        /** 定义程序执行成功验证文件xxxx_xx_xx_UploadFinish.label 存储路径 */
//         String resultfilepath = "/home/cupshe/importdata/output" ;
        String resultfilepath = "/Users/louise/cupshe/importdata/output" ; //请修改为存放完成标志文件XXXXX_finish.label的路径

        /**当前日期验证文件名称*/
        String fileName = sdf.format(d) +"_UploadFinish.label";
        File resultfile = new File(resultfilepath,fileName);

        /** 判断今日验证文件 **/
        logger.info("验证"+fileName+"文件是否存在");
        if(resultfile.exists()){
            logger.info("今日上传已完成，结束进程");
            System.exit(0) ;
        }
        else {
            logger.info("今日任务未执行，开始执行......");
           try {
               /**链接Sftp**/

               ManagementAPISample.SftpUtil ftp = new ManagementAPISample.SftpUtil();
               ftp.connect();

               /** 获取Sftp 文件list **/
               List<String> fileList = ftp.listFiles(remotePath);
               logger.info("当前Sftp文件列表信息：" + fileList);

               Boolean sftpfileIsExit = ftp.isExistDir(TodayFilePath);

               if(sftpfileIsExit){
                   /**下载今日数据文件**/
                   ftp.download(remotePath, TodayFilePath, localPath);
                   //logger.info("获取今日Sftp文件,本地文件名：" + localPath);

                   /** 删除Sftp中的文件 **/
                   //ftp.delete(remotePath, TodayFilePath);

                   logger.info("已获取今日Sftp文件到本地,Sftp文件名：" + TodayFilePath);
                   /** 断开Sftp连接 **/
                   ftp.disconnect();
               }
               else{
                   logger.info("今日Sftp数据文件"+ TodayFilePath +"不存在，结束进程，等待下次执行！" );
                   /** 断开Sftp连接 **/
                   ftp.disconnect();
                   System.exit(0) ;
               }

               /** 调用GA待上传文件 **/

               InputStreamContent mediaContent = new InputStreamContent("application/octet-stream", new FileInputStream(file));
               mediaContent.setLength(file.length());

               /** 创建 analytics 实例**/
               Analytics analytics = initializeAnalytic();


               /** 删除历史文件：判读历史文件的日期进行删除 **/
               logger.info("获取并删除 GA历史上传文件......");
               try {
                   Uploads uploads = analytics.management().uploads().list(AccountId,
                           WebPropertyId, CustomDataSourceId).execute();
                   //列举已经上传的文件
                   logger.info(" 列举已经上传的历史文件......");
                   if( uploads.getTotalResults()== 0){
                       logger.info("TotalResults ="+uploads.getTotalResults()+"; 暂无历史文件");
                   }
                   else{
                   for (Upload upload : uploads.getItems()) {
                       logger.info("Uploads Id            = " + upload.getId());
                       logger.info("Custom Data UploadTime = " + upload.getUploadTime());
                       logger.info("Upload Status         = " + upload.getStatus() + "\n");
                       SimpleDateFormat dataformat = new SimpleDateFormat("yyyy-MM-dd");
                       DateTime dataSourceTime = upload.getUploadTime();
                       logger.info("History dataSourceTime = " + dataSourceTime);

                       if (dataSourceTime.getValue() < TodaydateTime.getValue()) {
                           logger.info("历史文件时间：" + dataSourceTime + ";\n" + "当前时间" + TodaydateTime + "; \n" + "是否小于当前日期：" + (dataSourceTime.getValue() < TodaydateTime.getValue()));
                           // Construct a list of file ids to delete.
                           List<String> filesToDelete = Arrays.asList(upload.getId());
                           // Construct the body of the Delete Request and set the file ids.
                           AnalyticsDataimportDeleteUploadDataRequest body = new AnalyticsDataimportDeleteUploadDataRequest();
                           body.setCustomDataImportUids(filesToDelete);

                           // This request deletes three uploaded files for the authorized user.
                           try {
                               logger.info("删除GA历史上传文件：" + upload.getId());
                               analytics.management().uploads().deleteUploadData(AccountId, WebPropertyId, CustomDataSourceId, body).execute();
                           } catch (GoogleJsonResponseException e) {
                               System.err.println("There was a service error: "
                                       + e.getDetails().getCode() + " : "
                                       + e.getDetails().getMessage());
                           }
                       }
                   }}
               } catch (GoogleJsonResponseException e) {
                   System.err.println("There was a service error: "
                           + e.getDetails().getCode() + " : "
                           + e.getDetails().getMessage());
               }
               /** 上传今日新文件 **/
               logger.info("开始GA上传：上传今日产品文件......");
               try {
                   Upload upload = analytics.management().uploads().uploadData(AccountId, WebPropertyId, CustomDataSourceId, mediaContent).execute();
                   logger.info("Today Uploads Id            = " + upload.getId());
                   logger.info("Today Custom Data UploadTime = " + upload.getUploadTime());
                   logger.info("Today Custom Data UploadStatus = " + upload.getStatus());
                   /** 验证上传状态 **/
                   if(upload.getId() != null){
                       logger.info("上传成功......等待GA验证上传状态......当前状态："+ upload.getStatus() );
                       if (upload.getStatus().matches("PENDING") ){
                           do{
                               logger.info("当前上传状态：" + upload.getStatus() + ",等待3S");
                               sleep(3000);
                               Upload currentStatus = analytics.management().uploads().get(AccountId, WebPropertyId, CustomDataSourceId, upload.getId()).execute();
                               logger.info("3S后上传状态：" + currentStatus.getStatus() );
                           }
                           while(analytics.management().uploads().get(AccountId, WebPropertyId, CustomDataSourceId, upload.getId()).execute().getStatus().matches("PENDING"));
                       }
                       /**
                        * 执行完成创建UploadFinish.label文件
                        */
                           logger.info( "当前上传状态："+analytics.management().uploads().get(AccountId, WebPropertyId, CustomDataSourceId, upload.getId()).execute().getStatus() + ", 执行完成创建"+ fileName +"文件......");
                           Finish(resultfilepath);
                           logger.info("任务执行完毕，结束进程");
                           System.exit(0) ;
                   }
                   else{
                       logger.error("GA上传ID为空，上传失败......");
                   }
                   /** 发送邮件提醒 **/
               } catch (GoogleJsonResponseException e) {
                   System.err.println("There was a service error: "
                           + e.getDetails().getCode() + " : "
                           + e.getDetails().getMessage());
               }
           } catch (Exception e) {
               e.printStackTrace();
           }
       }
    }
    /**
     * Initializes an Analytics service object.
     *
     * @return An authorized Analytics service object.
     * @throws IOException
     * @throws GeneralSecurityException
     */
    private static Analytics initializeAnalytic() throws GeneralSecurityException, IOException {

        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();


        GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(KEY_FILE_LOCATION)).createScoped(AnalyticsScopes.all());

        // Construct the Analytics service object.
        return new Analytics.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME).build();
    }
    /**
     * 成功文件程序
     *
     */

    public static void Finish( String resultfilepath ){
        Log logger = LogFactory.getLog(Upload2Ga.class);
        File filePath = new File(resultfilepath);

        /** 判断该文件夹是否已存在*/
        if(!filePath.exists()){
            //不存在
            filePath.mkdirs();
        }
        /** fileName表示创建的文件名*/
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");//设置日期格式
        String fileName = df.format(new Date()) +"_UploadFinish.label";
        // System.out.println(fileName);// new Date()为获取当前系统时间
        File file = new File(filePath,fileName);

        /** 判断文件是否已存在*/
        if(!file.exists()){
            //不存在
            try{
                file.createNewFile();
                logger.info("验证文件不存在，创建验证文件:"+df.format(new Date())+"_UploadFinish.label");
            }catch(IOException e){
                e.printStackTrace();
                logger.error(e);
            }
        }else {
            logger.info("已存在标志文件:"+df.format(new Date())+"_UploadFinish.label");
        }
    }


}
