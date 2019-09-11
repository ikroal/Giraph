package org.apache.giraph.custom;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.io.formats.*;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * @author ikroal
 * @date 2019-05-10
 * @time: 09:31
 * @version: 1.0.0
 */
public class CustomRunner {

    /**
     * 输入路径
     */
    private static final String INPUT_PATH = "giraph-core/src/main/resources/input/graph_data.txt";

    /**
     * 输出路径
     */
    private static final String OUTPUT_PATH = "giraph-core/src/main/resources/output/shortestPath";

    public static void main(String[] args) throws Exception {
        GiraphConfiguration conf = new GiraphConfiguration(new Configuration());
        conf.setComputationClass(Shortestpath.class);
        //设置输入和输出格式
        conf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        //设置本地运行模式
        conf.setLocalTestMode(true);
        //设置 worker 配置
        conf.setWorkerConfiguration(1, 1, 100);
        //可选，如果要学习 Checkpoint 机制应该设置
        conf.setCheckpointFrequency(4);
        GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);

        GiraphJob job = new GiraphJob(conf, Shortestpath.class.getSimpleName());
        //设置输入和输出路径
        GiraphTextInputFormat.setVertexInputPath(conf, new Path(INPUT_PATH));
        GiraphTextOutputFormat.setOutputPath(job.getInternalJob(), new Path(OUTPUT_PATH));
        //删除之前的输出
        deletePath(OUTPUT_PATH, true);
        job.run(true);
    }

    /**
     * 用于删除输出目录
     *
     * @param path 目录路径
     */
    public static void deletePath(String path, boolean isDirectory) {
        File file = new File(path);
        if (file.exists()) {
            //本地目录递归删除
            if (isDirectory) {
                File[] subFiles = file.listFiles();
                for (File subFile : subFiles) {
                    if (subFile.isFile()) {
                        subFile.delete();
                    } else {
                        deletePath(subFile.getPath(), true);
                    }
                }
            }
            file.delete();
        }
    }
}
