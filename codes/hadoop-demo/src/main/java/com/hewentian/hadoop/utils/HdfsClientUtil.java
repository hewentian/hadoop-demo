package com.hewentian.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

/**
 * <p>
 * <b>HdfsClientUtil</b> 是 hdfs工具类
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-12-09 13:59:18
 * @since JDK 1.8
 */
public class HdfsClientUtil {
    private static Logger log = Logger.getLogger(HdfsClientUtil.class);

    private static FileSystem fileSystem = null;

    static {
        // new Configuration() 的时候，它就会去加载jar包中的hdfs-default.xml
        // 然后再加载classpath下的hdfs-site.xml
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop-host-master:9000");
        conf.set("dfs.replication", "2");

        try {
            // 最后的hadoop表示的是hadoop集群安装的Linux用户
            // 不提供用户名也可以，但是要在 hdfs-site.xml 中将 dfs.permissions.enabled 设为 false
            fileSystem = FileSystem.get(new URI("hdfs://hadoop-host-master:9000"), conf, "hadoop");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 关闭操作
     */
    public static void closeFileSystm() {
        try {
            fileSystem.close();
            fileSystem = null;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 创建目录
     *
     * @param pathString
     * @throws IOException
     */
    public static void mkdirs(String pathString) throws IOException {
        fileSystem.mkdirs(new Path(pathString));
    }

    /**
     * 删除目录
     *
     * @param path
     * @param b    如果目录非空，这里必须为true
     * @throws IOException
     */
    public static void delete(String path, boolean b) throws IOException {
        fileSystem.delete(new Path(path), b);
    }

    /**
     * 重命名文件或目录
     *
     * @param oldPath
     * @param newPath
     * @throws IOException
     */
    public static void rename(String oldPath, String newPath) throws IOException {
        fileSystem.rename(new Path(oldPath), new Path(newPath));
    }

    /**
     * 将文件上传到HDFS
     *
     * @param srcFile  要上传的文件
     * @param hdfsFile 目标文件位置
     * @throws IOException
     */
    public static void putFileToHdsf(String srcFile, String hdfsFile) throws IOException {
        fileSystem.copyFromLocalFile(new Path(srcFile), new Path(hdfsFile));
    }

    /**
     * 将HDFS的文件下载的本地
     *
     * @param hdfsFile HDFS中的文件路径
     * @param srcFile  本地文件路径
     * @throws IOException
     */
    public static void getFileFromHdfs(String hdfsFile, String srcFile) throws IOException {
        fileSystem.copyToLocalFile(new Path(hdfsFile), new Path(srcFile));
    }

    /**
     * 列出目录详情
     *
     * @param path
     * @param recursive
     * @throws IOException
     */
    public static void listFiles(String path, boolean recursive) throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(path), recursive);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("\n\n--------------------");
            System.out.println(fileStatus.getPath().toUri().getPath());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-lentgh: " + bl.getLength() + ", block-offset: " + bl.getOffset());

                String[] hosts = bl.getHosts();
                System.out.println("hosts: ");
                for (String host : hosts) {
                    System.out.print(host + ", ");
                }
            }
        }
    }

    public static void listStatus(String path) throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path(path));

        for (FileStatus fs : listStatus) {
            if (fs.isFile()) {
                System.out.println("f--    " + fs.getPath().getName());
            } else if (fs.isDirectory()) {
                System.out.println("d--    " + fs.getPath().getName());
            }
        }
    }

    /**
     * 将文件上传到HDFS，通过流的方式
     *
     * @param srcFile  要上传的文件
     * @param hdfsFile 目标文件位置
     * @throws IOException
     */
    public static void putFileToHdsfStream(String srcFile, String hdfsFile) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(srcFile);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(hdfsFile), true);


        try {
            // org.apache.hadoop.io.IOUtils
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4096);
        } catch (IOException e) {
            throw e;
        } finally {
            fsDataOutputStream.close();
            fileInputStream.close();
        }
    }

    /**
     * 将HDFS的文件下载的本地，通过流的方式
     *
     * @param hdfsFile HDFS中的文件路径
     * @param srcFile  本地文件路径
     * @throws IOException
     */
    public static void getFileFromHdfsStream(String hdfsFile, String srcFile) throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(hdfsFile));
        FileOutputStream fileOutputStream = new FileOutputStream(new File(srcFile));

        try {
            // org.apache.hadoop.io.IOUtils
            IOUtils.copyBytes(fsDataInputStream, fileOutputStream, 4096);
        } catch (IOException e) {
            throw e;
        } finally {
            fsDataInputStream.close();
            fileOutputStream.close();
        }
    }

    /**
     * 显示文件内容
     *
     * @param path
     * @throws IOException
     */
    public static void catFile(String path) throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path));

        try {
            // org.apache.hadoop.io.IOUtils
            IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
        } catch (IOException e) {
            throw e;
        } finally {
            fsDataInputStream.close();
        }
    }

    public static void check(String pathStr) throws IOException {
        Path path = new Path(pathStr);

        if (!fileSystem.exists(path)) {
            System.out.println(pathStr + " not exists");
        }

        if (fileSystem.isFile(path)) {
            System.out.println("is file");
        } else if (fileSystem.isDirectory(path)) {
            System.out.println("is directory");
        }
    }

    /**
     * 输出所有配置信息
     */
    public static void showAllConf() {
        Configuration conf = fileSystem.getConf();
        Iterator<Map.Entry<String, String>> it = conf.iterator();

        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }

    /**
     * hdfs之间文件的复制
     *
     * @param srcFile  hdfs上面的源文件
     * @param destFile hdfs上面的目标文件
     * @throws IOException
     */
    public static void copyFileBwtweenHdfs(String srcFile, String destFile) throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(srcFile));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(destFile));

        try {
            IOUtils.copyBytes(fsDataInputStream, fsDataOutputStream, 1024 * 1024 * 64, false);
        } catch (IOException e) {
            throw e;
        } finally {
            fsDataInputStream.close();
            fsDataOutputStream.close();
        }
    }

    /**
     * 通过URL方式来读取HDFS文件内容
     *
     * @param urlStr
     * @throws Exception
     */
    public static void catFileByURL(String urlStr) throws Exception {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL(urlStr);
        InputStream inputStream = url.openStream();

        try {
            IOUtils.copyBytes(inputStream, System.out, 4096, true);
        } catch (Exception e) {
            throw e;
        } finally {
            inputStream.close();
        }
    }

    public static void main(String[] args) {
        try {
//            mkdirs("/hewentian");
//            rename("/hewentian","/tim");
//            delete("/tim",true);
//            putFileToHdsf("/home/hewentian/Downloads/apache-maven-3.3.9-bin.tar.gz","/hewentian/");
//            putFileToHdsf("/home/hewentian/ProjectD/hadoopCluster/hadoop-2.7.3/README.txt","/hewentian/README.txt");
//    getFileFromHdfs("/hewentian/apache-maven-3.3.9-bin.tar.gz","/home/hewentian/Downloads/tmp2/");
//            listFiles("/hewentian",true);
//            listStatus("/hewentian");
//            putFileToHdsfStream("/home/hewentian/ProjectD/hadoopCluster/hadoop-2.7.3/NOTICE.txt","/hewentian/NOTICE.txt");
//            delete("/hewentian/NOTICE.txt",true);
//            getFileFromHdfsStream("/hewentian/apache-maven-3.3.9-bin.tar.gz", "/home/hewentian/Downloads/tmp2/apache-maven-3.3.9-bin.tar.gz");
//            catFile("/hewentian/README.txt");
//            check("/test");
//            check("/hewentian");
//            check("/hewentian/README.txt");
//showAllConf();
//        copyFileBwtweenHdfs("/hewentian/README.txt","/hewentian/README2.txt");
            catFileByURL("hdfs://hadoop-host-master:9000/hewentian/README.txt");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            closeFileSystm();
        }
    }
}
