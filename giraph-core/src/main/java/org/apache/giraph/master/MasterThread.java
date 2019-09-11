/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.master;

import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.giraph.counters.GiraphTimers;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.apache.giraph.conf.GiraphConstants.SPLIT_MASTER_WORKER;
import static org.apache.giraph.conf.GiraphConstants.USE_SUPERSTEP_COUNTERS;

/**
 * Master thread that will coordinate the activities of the tasks.  It runs
 * on all task processes, however, will only execute its algorithm if it knows
 * it is the "leader" from ZooKeeper.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class MasterThread<I extends WritableComparable, V extends Writable,
    E extends Writable> extends Thread {
  /** Counter group name for the Giraph timers */
  public static final String GIRAPH_TIMERS_COUNTER_GROUP_NAME = "Giraph Timers";
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(MasterThread.class);
  /** Reference to shared BspService */
  private CentralizedServiceMaster<I, V, E> bspServiceMaster = null;
  /** Context (for counters) */
  private final Context context;
  /** Use superstep counters? */
  private final boolean superstepCounterOn;
  /** Are master and worker split or not? */
  private final boolean splitMasterWorker;
  /** Setup seconds */
  private double setupSecs = 0d;
  /** Superstep timer (in seconds) map */
  private final Map<Long, Double> superstepSecsMap =
      new TreeMap<Long, Double>();

  /**
   * Constructor.
   *
   * @param bspServiceMaster Master that already exists and setup() has
   *        been called.
   * @param context Context from the Mapper.
   */
  public MasterThread(CentralizedServiceMaster<I, V, E> bspServiceMaster,
      Context context) {
    super(MasterThread.class.getName());
    this.bspServiceMaster = bspServiceMaster;
    this.context = context;
    //计数统计信息相关
    GiraphTimers.init(context);
    //是否使用超步计数
    superstepCounterOn = USE_SUPERSTEP_COUNTERS.get(context.getConfiguration());
    //是否区分 master 和 worker
    splitMasterWorker = SPLIT_MASTER_WORKER.get(context.getConfiguration());
  }

  /**
   * The master algorithm.  The algorithm should be able to withstand
   * failures and resume as necessary since the master may switch during a
   * job.
   */
  @Override
  public void run() {
    // Algorithm:
    // 1. Become the master
    // 2. If desired, restart from a manual checkpoint
    // 3. Run all supersteps until complete
    try {
      long startMillis = System.currentTimeMillis();
      long initializeMillis = 0;
      long endMillis = 0;
      //设置，根据 restartSuperstep 配置决定是否需要修改 Superstep
      bspServiceMaster.setup();
      //初始化 Superstep 状态
      SuperstepState superstepState = SuperstepState.INITIAL;

      if (bspServiceMaster.becomeMaster()) {
        // First call to checkWorkers waits for all pending resources.
        // If these resources are still available at subsequent calls it just
        // reads zookeeper for the list of healthy workers.
        bspServiceMaster.checkWorkers();
        initializeMillis = System.currentTimeMillis();
        GiraphTimers.getInstance().getInitializeMs().increment(
            initializeMillis - startMillis);
        // Attempt to create InputSplits if necessary. Bail out if that fails.
        //创建数据分片，在 worker 进行请求时发送给 worker
        if (bspServiceMaster.getRestartedSuperstep() !=
            BspService.UNSET_SUPERSTEP ||
            (bspServiceMaster.createMappingInputSplits() != -1 &&
                bspServiceMaster.createVertexInputSplits() != -1 &&
                bspServiceMaster.createEdgeInputSplits() != -1)) {
          long setupMillis = System.currentTimeMillis() - initializeMillis;
          GiraphTimers.getInstance().getSetupMs().increment(setupMillis);
          setupSecs = setupMillis / 1000.0d;
          //当前超步的状态，初始为 false
          while (!superstepState.isExecutionComplete()) {
            long startSuperstepMillis = System.currentTimeMillis();
            long cachedSuperstep = bspServiceMaster.getSuperstep();
            // If master and worker are running together, worker will call reset
            if (splitMasterWorker) {
              GiraphMetrics.get().resetSuperstepMetrics(cachedSuperstep);
            }
            /**
             * 从 {@link org.apache.giraph.conf.GiraphClasses} 中获取到用户设置的
             * computationClass
             */
            Class<? extends Computation> computationClass =
                bspServiceMaster.getMasterCompute().getComputation();
            //重要的调用，分配分区，建立 master 与 worker 的连接，部分 aggregator 操作
            // 返回超步状态，写检查点
            superstepState = bspServiceMaster.coordinateSuperstep();
            long superstepMillis = System.currentTimeMillis() -
                startSuperstepMillis;
            superstepSecsMap.put(cachedSuperstep,
                superstepMillis / 1000.0d);
            if (LOG.isInfoEnabled()) {
              LOG.info("masterThread: Coordination of superstep " +
                  cachedSuperstep + " took " +
                  superstepMillis / 1000.0d +
                  " seconds ended with state " + superstepState +
                  " and is now on superstep " +
                  bspServiceMaster.getSuperstep());
            }
            //是否使用超步计数器
            if (superstepCounterOn) {
              String computationName = (computationClass == null) ?
                  null : computationClass.getSimpleName();
              GiraphTimers.getInstance().getSuperstepMs(cachedSuperstep,
                  computationName).increment(superstepMillis);
            }

            //超步结束之后调用
            bspServiceMaster.postSuperstep();

            // If a worker failed, restart from a known good superstep
            //需要重启
            if (superstepState == SuperstepState.WORKER_FAILURE) {
              //从上次 checkpoint 设置部分参数
              bspServiceMaster.restartFromCheckpoint(
                  bspServiceMaster.getLastGoodCheckpoint());
            }
            endMillis = System.currentTimeMillis();
          }
          bspServiceMaster.setJobState(ApplicationState.FINISHED, -1, -1);
        }
      }
      //等待 Job 完成之后删除文件、关闭连接
      bspServiceMaster.cleanup(superstepState);
      if (!superstepSecsMap.isEmpty()) {
        GiraphTimers.getInstance().getShutdownMs().
          increment(System.currentTimeMillis() - endMillis);
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Took " + setupSecs + " seconds.");
        }
        for (Entry<Long, Double> entry : superstepSecsMap.entrySet()) {
          if (LOG.isInfoEnabled()) {
            if (entry.getKey().longValue() ==
                BspService.INPUT_SUPERSTEP) {
              LOG.info("input superstep: Took " +
                  entry.getValue() + " seconds.");
            } else {
              LOG.info("superstep " + entry.getKey() + ": Took " +
                  entry.getValue() + " seconds.");
            }
          }
          context.progress();
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("shutdown: Took " +
              (System.currentTimeMillis() - endMillis) /
              1000.0d + " seconds.");
          LOG.info("total: Took " +
              ((System.currentTimeMillis() - initializeMillis) /
              1000.0d) + " seconds.");
        }
        GiraphTimers.getInstance().getTotalMs().
          increment(System.currentTimeMillis() - initializeMillis);
      }
      //Job 完成之后回调
      bspServiceMaster.postApplication();
      // CHECKSTYLE: stop IllegalCatchCheck
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatchCheck
      LOG.error("masterThread: Master algorithm failed with " +
          e.getClass().getSimpleName(), e);
      //Job 失败的时候回调
      bspServiceMaster.failureCleanup(e);
      throw new IllegalStateException(e);
    }
  }
}
