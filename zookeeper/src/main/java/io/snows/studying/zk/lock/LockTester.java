package io.snows.studying.zk.lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Random;

public class LockTester {
    CuratorFramework client;

    public LockTester(String uri) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(uri, retryPolicy);
        client.start();
        //client.create().forPath("/lock/lck00", "testing".getBytes());
    }

    public void run() throws Exception {
        InterProcessMutex lock = new InterProcessMutex(client, "/lock3/lck00");
        Path tf = Paths.get("/tmp/t01.txt");
        int rid = new Random().nextInt();
        while (true) {
            lock.acquire();
            String data;
            try {
                data = new String(Files.readAllBytes(tf));
            } catch (IOException e) {
                data = "0";
            }
            long value = Long.parseLong(data);
            value++;
            Files.write(tf, ("" + value).getBytes(), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
            Files.write(Paths.get("/tmp/t01.log"), (rid + "," + value + "\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            lock.release();
            Thread.sleep(100);
        }

    }

    public static void main(String[] args) {
        try {
            new LockTester("127.0.0.1:2181").run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
