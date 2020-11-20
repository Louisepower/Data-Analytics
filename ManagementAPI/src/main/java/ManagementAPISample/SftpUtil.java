package ManagementAPISample;

import com.jcraft.jsch.*;
import org.eclipse.jetty.server.session.JDBCSessionManager;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;


import com.jcraft.jsch.ChannelSftp;

public class SftpUtil {

 private String host = "39.100.120.159";
 private String username="cupshe";
 private String password="q1w2e3r4";
 private int port = 22;
 private ChannelSftp sftp = null;


 /**
  * connect server via sftp
  */
 public void connect() {
  try {
   if(sftp != null){
    System.out.println("sftp is not null");
   }
   JSch jsch = new JSch();
   jsch.getSession(username, host, port);
   Session sshSession = jsch.getSession(username, host, port);
   System.out.println("Session created.");
   sshSession.setPassword(password);
   Properties sshConfig = new Properties();
   sshConfig.put("StrictHostKeyChecking", "no");
   sshSession.setConfig(sshConfig);
   sshSession.connect();
   System.out.println("Session connected.");
   System.out.println("Opening Channel.");
   Channel channel = sshSession.openChannel("sftp");
   channel.connect();
   sftp = (ChannelSftp) channel;
   System.out.println("Connected to " + host + ".");
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
 /**
  * Disconnect with server
  */
 public void disconnect() {
  if(this.sftp != null){
   if(this.sftp.isConnected()){
    this.sftp.disconnect();
   }else if(this.sftp.isClosed()){
    System.out.println("sftp is closed already");
   }
  }
 }

 /**
  * 判断文件是否存在
  *
  * **/

 public boolean isExistDir(String path){
  boolean  isExist=false;
  try {
   SftpATTRS sftpATTRS = sftp.lstat(path);
   isExist = true;

  } catch (Exception e) {
   if (e.getMessage().toLowerCase().equals("no such file")) {
    isExist = false;
   }
  }
  return isExist;
 }

 /**
  * 删除文件
  *
  * @param directory
  *            要删除文件所在目录
  * @param deleteFile
  *            要删除的文件
  */
 public void delete(String directory, String deleteFile) {
  try {
   sftp.cd(directory);
   sftp.rm(deleteFile);
  } catch (Exception e) {
   e.printStackTrace();
  }
 }


 public void download(String directory, String downloadFile,String saveFile) {
  try {
   sftp.cd(directory);
   File file = new File(saveFile);
   sftp.get(downloadFile, new FileOutputStream(file));
  } catch (Exception e) {
   e.printStackTrace();
  }
 }

 /**
  * 列出目录下的文件
  *
  * @param directory
  *            要列出的目录
  */
 public Vector listFiles(String directory)
         throws SftpException {
  return sftp.ls(directory);

 }
 /**
  * get all the files need to be upload or download
  * @param filepath
  * @return
  */
 public List<String> getFileEntryList(String filepath){
  ArrayList<String> fileList = new ArrayList<String>();
  InputStream in = null;
  try {

   while(filepath != null){
    fileList.add(filepath);
   }
   in.close();
  } catch (FileNotFoundException e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
  } catch (IOException e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
  }finally{
   if(in != null){
    in = null;
   }
  }

  return fileList;
 }

}


