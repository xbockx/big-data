package com.linbo.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description
 * @Author xbockx
 * @Date 11/20/2021
 */
public class HDFSClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration cfg = new Configuration();
        cfg.set("dfs.replication", "2");
        // 用户
        String user = "linbo";
        // 1. 获取客户端对象
        fs = FileSystem.get(uri, cfg, user);
    }

    @After
    public void close() throws IOException {
        // 3. 关闭资源
        fs.close();
    }

    /**
     * 创建目录
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException {
        // 2. 创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));
    }

    /**
     * 上传
     * 参数优先级：hdfs-default->hdfs-site->resources下配置->代码配置
     */
    @Test
    public void testPut() throws IOException {
        String filePath = "D:/temp/sunwukong.txt";
        File file = new File(filePath);
        file.createNewFile();
        fs.copyFromLocalFile(true, true, new Path(filePath), new Path("/xiyou/huaguoshan"));
    }

    /**
     * 下载
     */
    @Test
    public void testGet() throws IOException {
        fs.copyToLocalFile(true, new Path("/xiyou/huaguoshan"), new Path("D:/temp/huaguoshan"), false);
    }

    /**
     * 删除
     */
    @Test
    public void testRm() throws IOException {
        // 删除文件
        fs.delete(new Path("/xiyou/huaguoshan1"), false);
        // 删除空目录
        fs.delete(new Path("/xiyou"), false);
        // 删除非空目录
        fs.delete(new Path("/xiyou"), true);
    }

    /**
     * 获取文件详细信息
     */
    @Test
    public void testFileDetail() throws IOException {
        final RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            System.out.println("==================");
            final LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath());
            System.out.println(next.getPermission());
            System.out.println(next.getLen());
            System.out.println(next.getGroup());
            System.out.println(next.getOwner());
        }
    }

}
