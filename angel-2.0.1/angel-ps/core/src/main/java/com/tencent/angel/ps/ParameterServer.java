/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ps;

import com.google.protobuf.ServiceException;
import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.model.PSMatricesLoadContext;
import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.output.format.IntFloatElement;
import com.tencent.angel.model.output.format.SnapshotFormat;
import com.tencent.angel.plugin.AngelServiceLoader;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.PSAttemptIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.*;
import com.tencent.angel.ps.client.MasterClient;
import com.tencent.angel.ps.client.PSLocationManager;
import com.tencent.angel.ps.clock.ClockVectorManager;
import com.tencent.angel.ps.io.PSModelIOExecutor;
import com.tencent.angel.ps.io.load.PSModelLoader;
import com.tencent.angel.ps.io.load.SnapshotRecover;
import com.tencent.angel.ps.io.save.PSModelSaver;
import com.tencent.angel.ps.io.save.SnapshotDumper;
import com.tencent.angel.ps.meta.PSMatrixMetaManager;
import com.tencent.angel.ps.server.control.ParameterServerService;
import com.tencent.angel.ps.server.data.MatrixTransportServer;
import com.tencent.angel.ps.server.data.PSFailedReport;
import com.tencent.angel.ps.server.data.RunningContext;
import com.tencent.angel.ps.server.data.WorkerPool;
import com.tencent.angel.ps.storage.MatrixStorageManager;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.utils.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parameter server,hold and manage individual parameters that divided by {@link com.tencent.angel.master.AngelApplicationMaster}.
 */
public class ParameterServer {
  private static final Log LOG = LogFactory.getLog(ParameterServer.class);

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Application configuration
   */
  private final Configuration conf;

  /**
   * Master location
   */
  private final Location masterLocation;

  /**
   * PS Attempt id
   */
  private final PSAttemptId attemptId;

  /**
   * PS Attempt id proto
   */
  private final PSAttemptIdProto attemptIdProto;

  private final AtomicBoolean stopped;

  /**
   * PS Attempt index
   */
  private final int attemptIndex;

  /**
   * PS RPC server for control message
   */
  private volatile ParameterServerService psServerService;

  /**
   * PS RPC server for data
   */
  private volatile MatrixTransportServer matrixTransportServer;

  /**
   * Heartbeat thread
   */
  private volatile Thread heartbeatThread;

  /**
   * Location manager
   */
  private volatile PSLocationManager locationManager;

  /**
   * Matrix data storage
   */
  private volatile MatrixStorageManager matrixStorageManager;

  /**
   * Matrix meta manager
   */
  private volatile PSMatrixMetaManager matrixMetaManager;

  /**
   * Matrix clock vector manager
   */
  private volatile ClockVectorManager clockVectorManager;

  private volatile PSModelIOExecutor ioExecutor;

  /**
   * Matrix saver
   */
  private volatile PSModelSaver saver;

  /**
   * Matrix saver
   */
  private volatile PSModelLoader loader;

  /**
   * Matrix snapshot dumper
   */
  private volatile SnapshotDumper snapshotDumper;

  /**
   * Master RPC client
   */
  private volatile MasterClient master;

  /**
   * HA update pusher TODO
   */
  // private volatile PS2PSPusherImpl ps2PSPusher;

  /**
   * The RPC handlers for matrix data
   */
  private volatile WorkerPool workerPool;

  private volatile RunningContext runningContext;

  private final PSFailedReport psFailedReport;

  private static final AtomicInteger runningWorkerGroupNum = new AtomicInteger(0);
  private static final AtomicInteger runningWorkerNum = new AtomicInteger(0);
  private static final AtomicInteger runningTaskNum = new AtomicInteger(0);

  public static int getRunningWorkerGroupNum() {
    return runningWorkerGroupNum.get();
  }

  public static int getRunningWorkerNum() {
    return runningWorkerNum.get();
  }

  public static int getRunningTaskNum() {
    return runningTaskNum.get();
  }

  public static void setRunningWorkerGroupNum(int num) {
    runningWorkerGroupNum.set(num);
  }

  public static void setRunningWorkerNum(int num) {
    runningWorkerNum.set(num);
  }

  public static void setRunningTaskNum(int num) {
    runningTaskNum.set(num);
  }

  /**
   * Create a new Parameter server.
   *
   * @param serverIndex   the server index
   * @param attemptIndex  the attempt index
   * @param appMasterHost the app master host
   * @param appMasterPort the app master port
   * @param conf          the conf
   */
  public ParameterServer(int serverIndex, int attemptIndex, String appMasterHost, int appMasterPort,
    Configuration conf) {
    this.attemptId = new PSAttemptId(new ParameterServerId(serverIndex), attemptIndex);
    this.attemptIdProto = ProtobufUtil.convertToIdProto(attemptId);
    this.attemptIndex = attemptIndex;
    this.conf = conf;
    this.masterLocation = new Location(appMasterHost, appMasterPort);
    this.stopped = new AtomicBoolean(false);
    this.psFailedReport = new PSFailedReport();
    this.context = new PSContext(this);
  }

  /* new code */
  public void print_ParameterServer(){
    LOG.info("print_ParameterServer");
    LOG.info("");
    LOG.info("");
    matrixMetaManager.print_PSMatrixMetaManager();
    LOG.info("");
    LOG.info("");
    clockVectorManager.print_ClockVectorManager();
    LOG.info("");
    LOG.info("");
    matrixStorageManager.print_MatrixStorageManager();
    LOG.info("");
    LOG.info("");
    locationManager.print_PSLocationManager();
    LOG.info("");
    LOG.info("");
  }

  /* code end */


  /**
   * Gets matrix partition manager.
   *
   * @return the matrix partition manager
   */
  public MatrixStorageManager getMatrixStorageManager() {
    return matrixStorageManager;
  }

  /**
   * Get matrix meta manager
   *
   * @return
   */
  public PSMatrixMetaManager getMatrixMetaManager() {
    return matrixMetaManager;
  }

  /**
   * Get matrix clock vector manager
   *
   * @return
   */
  public ClockVectorManager getClockVectorManager() {
    return clockVectorManager;
  }

  /**
   * Stop parameter server.
   *
   * @param exitCode the exit code
   */
  public void stop(int exitCode) {
    LOG.info("stop*****************************"); //////
    LOG.info("stop ps rpcServer!");
    if (psServerService != null) {
      psServerService.stop();
      psServerService = null;
    }
    LOG.info("stop heartbeat thread!");
    if (!stopped.getAndSet(true)) {
      if (heartbeatThread != null) {
        heartbeatThread.interrupt();
        try {
          heartbeatThread.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping heartbeatThread.");
        }
        heartbeatThread = null;
      }

      if (matrixTransportServer != null) {
        try {
          matrixTransportServer.stop();
        } catch (InterruptedException e) {
          LOG.warn("stop matrixTransportServer interrupted.");
        }
        matrixTransportServer = null;
      }

      if (snapshotDumper != null) {
        snapshotDumper.stop();
        snapshotDumper = null;
      }

      if (master != null) {
        master.stop();
        master = null;
      }

      // TODO
      /*
      if(ps2PSPusher != null) {
        ps2PSPusher.stop();
        ps2PSPusher = null;
      }
      */

      if (workerPool != null) {
        workerPool.stop();
        workerPool = null;
      }

      if (clockVectorManager != null) {
        clockVectorManager.stop();
        clockVectorManager = null;
      }

      if (ioExecutor != null) {
        ioExecutor.stop();
        ioExecutor = null;
      }

      if (runningContext != null) {
        runningContext.stop();
        runningContext = null;
      }

      AngelServiceLoader.stopService();
      exit(exitCode);
    }
  }

  private void exit(int code) {
    LOG.info("exit*****************************"); //////
    AngelDeployMode deployMode = context.getDeployMode();
    if (deployMode == AngelDeployMode.YARN) {
      System.exit(code);
    }
  }

  public static void main(String[] argv) {
    LOG.info("Starting Parameter Server");
    int serverIndex = Integer.valueOf(System.getenv(AngelEnvironment.PARAMETERSERVER_ID.name()));
    String appMasterHost = System.getenv(AngelEnvironment.LISTEN_ADDR.name());
    int appMasterPort = Integer.valueOf(System.getenv(AngelEnvironment.LISTEN_PORT.name()));

    int attemptIndex = Integer.valueOf(System.getenv(AngelEnvironment.PS_ATTEMPT_ID.name()));

    Configuration conf = new Configuration();
    conf.addResource(AngelConf.ANGEL_JOB_CONF_FILE);

    String user = System.getenv(ApplicationConstants.Environment.USER.name());
    UserGroupInformation.setConfiguration(conf);

    String runningMode =
      conf.get(AngelConf.ANGEL_RUNNING_MODE, AngelConf.DEFAULT_ANGEL_RUNNING_MODE);
    if (runningMode.equals(RunningMode.ANGEL_PS_WORKER.toString())) {
      LOG.debug("AngelEnvironment.TASK_NUMBER.name()=" + AngelEnvironment.TASK_NUMBER.name());
      conf.set(AngelConf.ANGEL_TASK_ACTUAL_NUM, System.getenv(AngelEnvironment.TASK_NUMBER.name()));
    }

    final ParameterServer psServer =
      new ParameterServer(serverIndex, attemptIndex, appMasterHost, appMasterPort, conf);

    try {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      UserGroupInformation psUGI = UserGroupInformation
        .createRemoteUser(System.getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      psUGI.addCredentials(credentials);

      psUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override public Object run() throws Exception {
          psServer.initialize();
          psServer.start();
          return null;
        }
      });
    } catch (Throwable x) {
      LOG.fatal("Start PS failed ", x);
      psServer.failed(x.getMessage());
    }
    LOG.info("Starting Parameter Server successfully.");
  }

  /**
   * Gets host address.
   *
   * @return the host address
   * @throws UnknownHostException
   */
  public String getHostAddress() throws UnknownHostException {
    return psServerService.getHostAddress();
  }

  /**
   * Gets port.
   *
   * @return the port
   */
  public int getPort() {
    return psServerService.getPort();
  }

  /**
   * Gets server id.
   *
   * @return the server id
   */
  public ParameterServerId getServerId() {
    return attemptId.getPsId();
  }

  /**
   * Gets ps attempt id.
   *
   * @return the ps attempt id
   */
  public PSAttemptId getPSAttemptId() {
    return attemptId;
  }

  /**
   * Gets master location.
   *
   * @return the master location
   */
  public Location getMasterLocation() {
    return locationManager.getMasterLocation();
  }

  /**
   * Gets conf.
   *
   * @return the conf
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Initialize.
   *
   * @throws IOException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public void initialize() throws IOException, InstantiationException, IllegalAccessException {
    LOG.info("Initialize a parameter server");
    ServerRow.maxLockWaitTimeMs = conf.getInt(AngelConf.ANGEL_PS_MAX_LOCK_WAITTIME_MS,
      AngelConf.DEFAULT_ANGEL_PS_MAX_LOCK_WAITTIME_MS);
    ServerRow.useAdaptiveStorage = conf.getBoolean(AngelConf.ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE,
      AngelConf.DEFAULT_ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE);
    ServerRow.sparseToDenseFactor = conf.getFloat(AngelConf.ANGEL_PS_SPARSE_TO_DENSE_FACTOR,
      AngelConf.DEFAULT_ANGEL_PS_SPARSE_TO_DENSE_FACTOR);

    locationManager = new PSLocationManager(context);
    locationManager.setMasterLocation(masterLocation);

    runningContext = new RunningContext(context);
    workerPool = new WorkerPool(context, runningContext);
    workerPool.init();

    ioExecutor = new PSModelIOExecutor(context);
    ioExecutor.init();

    matrixStorageManager = new MatrixStorageManager(context);
    int taskNum = conf.getInt(AngelConf.ANGEL_TASK_ACTUAL_NUM, 1);
    clockVectorManager = new ClockVectorManager(taskNum, context);
    clockVectorManager.init();
    matrixMetaManager = new PSMatrixMetaManager(context);

    master = new MasterClient(context);
    master.init();

    psServerService = new ParameterServerService(context);
    psServerService.start();
    matrixTransportServer = new MatrixTransportServer(getPort() + 1, context);

    saver = new PSModelSaver(context);
    loader = new PSModelLoader(context);

    int replicNum = conf.getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
      AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);

    // TODO
    if (replicNum > 1) {
      /*
      boolean useEventPush = false;//conf.getBoolean(AngelConf.ANGEL_PS_HA_USE_EVENT_PUSH, AngelConf.DEFAULT_ANGEL_PS_HA_USE_EVENT_PUSH);
      if(useEventPush) {
        boolean sync = conf.getBoolean(AngelConf.ANGEL_PS_HA_PUSH_SYNC, AngelConf.DEFAULT_ANGEL_PS_HA_PUSH_SYNC);
        if(sync) {
          ps2PSPusher = new SyncEventPusher(context);
        } else {
          ps2PSPusher = new AsyncEventPusher(context);
        }
      } else {
        ps2PSPusher = new PeriodPusher(context);
      }
      ps2PSPusher.init();
      */
    } else {
      snapshotDumper = new SnapshotDumper(context);
    }
  }

  private void startHeartbeat() {
    final int heartbeatInterval = conf.getInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS,
      AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS);
    LOG.info("Starting HeartbeatThread, interval is " + heartbeatInterval + " ms");
    heartbeatThread = new Thread(() -> {
      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        //LOG.info("begin stopped.get() = " + stopped.get()); //////
        //LOG.info("begin Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
        try {
          //LOG.info("0 => stopped.get() = " + stopped.get()); //////
          //LOG.info("0 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
          Thread.sleep(heartbeatInterval);
          //LOG.info("1 => stopped.get() = " + stopped.get()); //////
          //LOG.info("1 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
        } catch (InterruptedException e) {
          //LOG.info("2 => stopped.get() = " + stopped.get()); //////
          //LOG.info("2 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
          if (!stopped.get()) {
            //LOG.info("3 => stopped.get() = " + stopped.get()); //////
            //LOG.info("3 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
            LOG.warn("Allocated thread interrupted. Returning.", e);
          }
          //LOG.info("4 => stopped.get() = " + stopped.get()); //////
          //LOG.info("4 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
          return;
        }
        //LOG.info("5 => stopped.get() = " + stopped.get()); //////
        //LOG.info("5 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
        try {
          //LOG.info("6 => stopped.get() = " + stopped.get()); //////
          //LOG.info("6 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
          if (!stopped.get()) {
            //LOG.info("7 => stopped.get() = " + stopped.get()); //////
            //LOG.info("7 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
            heartbeat();
          }
          //LOG.info("8 => stopped.get() = " + stopped.get()); //////
          //LOG.info("8 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
        } catch (YarnRuntimeException e) {
          //LOG.info("9 => stopped.get() = " + stopped.get()); //////
          //LOG.info("9 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
          LOG.error("Error communicating with AM: " + e.getMessage(), e);
          //LOG.info("10 => stopped.get() = " + stopped.get()); //////
          //LOG.info("10 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
          return;
        } catch (Exception e) {
          //LOG.info("11 => stopped.get() = " + stopped.get()); //////
          //LOG.info("11 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
          LOG.error("ERROR IN CONTACTING RM. ", e);
          //LOG.info("12 => stopped.get() = " + stopped.get()); //////
          //LOG.info("12 Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
        }
        //LOG.info("end stopped.get() = " + stopped.get()); //////
        //LOG.info("end Thread.currentThread().isInterrupted() = " + Thread.currentThread().isInterrupted()); //////
      }
    });
    heartbeatThread.setName("heartbeatThread");
    heartbeatThread.start();
  }

  private void register() {
    LOG.info("register*****************************"); //////
    try {
      master.register();
      LOG.info("Register to AppMaster successfully");
    } catch (Throwable e) {
      // to exit
      LOG.error("ps register to AppMaster failed: ", e);
      stop(-1);
    }
  }

  private List<MatrixReportProto> buildMatrixReports() {
    MatrixReportProto.Builder matrixBuilder = MatrixReportProto.newBuilder();
    PartReportProto.Builder partBuilder = PartReportProto.newBuilder();
    List<MatrixReportProto> ret = new ArrayList<>();

    for (MatrixMeta matrix : matrixMetaManager.getMatrixMetas().values()) {
      matrixBuilder.setMatrixId(matrix.getId()).setMatrixName(matrix.getName());
      if (context.getPartReplication() > 1) {
        for (PartitionMeta part : matrix.getPartitionMetas().values()) {
          partBuilder.setPartId(part.getPartId()).setStatus(
            context.getMatrixStorageManager().getPart(matrix.getId(), part.getPartId()).getState()
              .getNumber());
          matrixBuilder.addPartReports(partBuilder.build());
        }
      }
      ret.add(matrixBuilder.build());
      matrixBuilder.clear();
    }
    return ret;
  }

  private void heartbeat() {
    LOG.info("heartbeat*****************************"); //////
    PSReportRequest.Builder builder = PSReportRequest.newBuilder();
    builder.setPsAttemptId(attemptIdProto);
    Pair.Builder pairBuilder = Pair.newBuilder();
    pairBuilder.setKey("key");
    pairBuilder.setValue("value");
    builder.addMetrics(pairBuilder.build());
    builder.addAllMatrixReports(buildMatrixReports());

    PSReportResponse ret;
    PSReportRequest request = builder.build();
    LOG.debug("ps hb = " + request);
    try {
      ret = master.psReport(request);
      /* new code */
      LOG.info("heartbeat() in ParameterServer.java");
      int status = ret.getPsStatus();
      if (status == -1){
        remove_save();
        done();
        return;
      }else if (status == 1){
        LOG.info("status == 1");
        List<MatrixMeta> matrixMetas_idle = ProtobufUtil.convertToMatricesMeta(ret.getNeedIdleMatricesList());
        load_removedPS(matrixMetas_idle);
      }
      LOG.info("status from heartbeat = " + status);
      LOG.info("ret.getPsCommand() = " + ret.getPsCommand());
      /* code end */
      switch (ret.getPsCommand()) {
        case PSCOMMAND_REGISTER:
          try {
            register();
          } catch (Exception x) {
            LOG.error("register failed: ", x);
            stop(-1);
          }
          break;

        case PSCOMMAND_SHUTDOWN:
          LOG.error("shutdown command come from appmaster, exit now!!");
          stop(-1);
          break;

        default:
          break;
      }

      LOG.debug("ps hb ret = " + ret);

      LOG.info("ps hb ret = " + ret); //////
      if (ret.hasNeedSaveMatrices()) {
        /* old code */
        // saver.save(ProtobufUtil.convert(ret.getNeedSaveMatrices()));
        /* new code */
        LOG.info("ret.hasNeedSaveMatrices()");
        PSMatricesSaveContext saveContext = ProtobufUtil.convert(ret.getNeedSaveMatrices());
        saveContext.print_PSMatricesSaveContext();
        saver.save(saveContext);
        /* code end */
      }

      if (ret.hasNeedLoadMatrices()) {
        LOG.info("ret.hasNeedLoadMatrices()"); //////
        loader.load(ProtobufUtil.convert(ret.getNeedLoadMatrices()));
      }
      syncMatrices(ret.getNeedCreateMatricesList(), ret.getNeedReleaseMatrixIdsList(),
        ret.getNeedRecoverPartsList());
    } catch (Throwable e) {
      LOG.error("send heartbeat to appmaster failed ", e);
      stop(-1);
    }
  }

  private void syncMatrices(List<MLProtos.MatrixMetaProto> needCreateMatrices,
    List<Integer> needReleaseMatrices, List<RecoverPartKeyProto> needRecoverParts)
    throws Exception {
    LOG.info("syncMatrices......"); //////
    if (!needCreateMatrices.isEmpty()) {
      /* old code
      createMatrices(ProtobufUtil.convertToMatricesMeta(needCreateMatrices));
      /* new code */
      LOG.info("createMatrices......"); ///////
      List<MatrixMeta> matricesMeta = ProtobufUtil.convertToMatricesMeta(needCreateMatrices);
      createMatrices(matricesMeta);
      for (int i = 0; i < matricesMeta.size(); i++){
        LOG.info("matrixMeta " + i);
        LOG.info("      MatrixContext_ToString = " + matricesMeta.get(i).getMatrixContext().toString());
        LOG.info("      MatrixPartitionsMeta:");
        for (Map.Entry<Integer, PartitionMeta> entry : matricesMeta.get(i).getPartitionMetas().entrySet()){
          LOG.info("          PartitionInteger " + entry.getKey());
          LOG.info("          PartitionKey_ToString = " + entry.getValue().getPartitionKey().toString());
          LOG.info("          PartitionMeta_ToString = " + entry.getValue().toString());
        }
      }
    /* code end */
    }
    if (!needReleaseMatrices.isEmpty()) {
      LOG.info("releaseMatrices......"); //////
      releaseMatrices(needReleaseMatrices);
    }

    // TODO
    /*
    if(needCreateMatrices.isEmpty() && needReleaseMatrices.isEmpty()
      && !needRecoverParts.isEmpty() && (ps2PSPusher != null)) {
      LOG.info("need recover parts:" + needRecoverParts);
      int size = needRecoverParts.size();
      for(int i = 0; i < size; i++) {
        // TODO
        //ps2PSPusher.recover(ProtobufUtil.convert(needRecoverParts.get(i)));
      }
    }
    //context.getSnapshotManager().processRecovery();
    */
  }

  private void createMatrices(List<MatrixMeta> matrixMetas) throws Exception {
    matrixMetaManager.addMatrices(matrixMetas);
    clockVectorManager.addMatrices(matrixMetas);
    if (context.getPartReplication() == 1) {
      clockVectorManager.adjustClocks(master.getTaskMatrixClocks());
    }
    matrixStorageManager.addMatrices(matrixMetas);
    initMatricesData(matrixMetas);
  }

  /* new code */
  private void createMatrices_idle(List<MatrixMeta> matrixMetas_idle) throws IOException {
    LOG.info("before createMatrices_idle......");
    print_ParameterServer();
    matrixMetaManager.addMatrices_idle(matrixMetas_idle);
    clockVectorManager.addMatrices_idle(matrixMetas_idle);
    matrixStorageManager.addMatrices_idle(matrixMetas_idle);
    LOG.info("after createMatrices_idle......");
    print_ParameterServer();
  }

  /* code end */

  private void initMatricesData(final List<MatrixMeta> matrixMetas) throws IOException {
    LOG.info("initMatricesData(final List<MatrixMeta> matrixMetas)......"); //////
    if (context.getPartReplication() > 1 && context.getPSAttemptId().getIndex() > 0) {
      return;
    }
    LOG.info("no return in initMatricesData(...)..."); //////
    // Recover PS from snapshot or load path
    if (context.getPSAttemptId().getIndex() > 1) {
      LOG.info("Recover PS from snapshot or load path......."); //////
      int matrixNum = matrixMetas.size();
      List<PSMatrixLoadContext> matrixLoadContexts = new ArrayList<>(matrixMetas.size());
      SnapshotRecover recover = new SnapshotRecover(context);
      for (int i = 0; i < matrixNum; i++) {
        // First check snapshot
        Path inputPath = null;
        try {
          inputPath = recover.getSnapshotPath(matrixMetas.get(i).getId());
        } catch (IOException e) {
          LOG.error("Get snapshot path failed, ", e);
        }

        // Check load path setting
        if (inputPath == null) {
          String loadPathStr = matrixMetas.get(i).getAttribute(MatrixConf.MATRIX_LOAD_PATH);
          if (loadPathStr != null) {
            inputPath = new Path(loadPathStr, matrixMetas.get(i).getName());
          }
        }

        if (inputPath != null) {
          matrixLoadContexts.add(new PSMatrixLoadContext(matrixMetas.get(i).getId(),
            new Path(inputPath.toString(), matrixMetas.get(i).getName()).toString(),
            new ArrayList<>(matrixMetas.get(i).getPartitionMetas().keySet()),
            SnapshotFormat.class.getName()));
        }
      }

      if (!matrixLoadContexts.isEmpty()) {
        context.getIOExecutors().load(new PSMatricesLoadContext(-1, -1, matrixLoadContexts));
      }
    }
  }

  private void releaseMatrices(List<Integer> matrixIds) {
    if (!matrixIds.isEmpty()) {
      matrixMetaManager.removeMatrices(matrixIds);
      clockVectorManager.removeMatrices(matrixIds);
      clearMatricesData(matrixIds);
    }
  }

  private void clearMatricesData(List<Integer> matrixIds) {
    matrixStorageManager.removeMatrices(matrixIds);
  }

  /**
   * Start parameter server services.
   *
   * @throws IOException the io exception
   */
  public void start() throws Exception {
    LOG.info("start*****************************"); //////
    if (snapshotDumper != null) {
      snapshotDumper.start();
    }
    master.start();

    // TODO
    // if(ps2PSPusher != null) {
    // ps2PSPusher.start();
    // }

    workerPool.start();
    ioExecutor.start();
    matrixTransportServer.start();
    clockVectorManager.start();
    runningContext.start();

    // > 0 means it is not the first PSAttempt
    LOG.info("getAttemptIndex() = " + getAttemptIndex()); //////
    if (getAttemptIndex() > 0) {
      LOG.info("PS " + getServerId() + " running attempt " + getAttemptIndex()
        + " load matrices from snapshot if need");
      List<MatrixMeta> matrixMetas = master.getMatricesMeta();
      if (!matrixMetas.isEmpty()) {
        createMatrices(matrixMetas);
      }
    }

    register();
    startHeartbeat();
    AngelServiceLoader.startServiceIfNeed(this, getConf());
    LOG.info("end of start()"); //////
  }

  /* new code */
  public void load_removedPS(List<MatrixMeta> matrixMetas_idle) throws IOException {
    LOG.info("load_removedPS**************************");
    if (matrixMetas_idle != null) {
      LOG.info("matrixMetas_idle size = " + matrixMetas_idle.size());
      // only care about public Map<Integer, PartitionMeta> partitionMetas_idle in MatrixMeta
      for (int i = 0; i < matrixMetas_idle.size(); i++) {
        LOG.info("matrixId = " + matrixMetas_idle.get(i).getMatrixContext().getMatrixId());
        for (Map.Entry<Integer, PartitionMeta> entry: matrixMetas_idle.get(i).partitionMetas_idle.entrySet()){
          LOG.info("PartitionMeta_idle toString = " + entry.getValue().toString());
        }
      }
      createMatrices_idle(matrixMetas_idle);
    }
  }


  public void remove_save() throws ServiceException {
    LOG.info("remove_save*****************************");
    List<MatrixMeta> matrixMetas = master.psRemove(attemptIdProto);
    LOG.info("matrixMetas.size() = " + matrixMetas.size());
    List<String> savedFiles = save(matrixMetas);
    master.psSave(attemptIdProto);
    // read_test(savedFiles);
  }

  public List<String> save(List<MatrixMeta> matrixMetas){
    List<String> savedFiles = new ArrayList<>();
    int rowindex = 0;
    FileSystem fs = null;
    for (int i = 0 ; i< matrixMetas.size(); i++){
      MatrixMeta matrixMeta = matrixMetas.get(i);
      ServerMatrix serverMatrix = context.getMatrixStorageManager().getMatrix(matrixMeta.getId());
      if (serverMatrix != null){
        for (Map.Entry<Integer, Map<Integer, PartitionMeta>> entry: matrixMeta.partitionMetas_repartition.entrySet()){
          int prePartitionId = entry.getKey();
          ServerPartition serverPartition = serverMatrix.getPartition(prePartitionId);
          int baseCol = (int) context.getMatrixMetaManager().getPartMeta(matrixMeta.getId(), prePartitionId).getStartCol();
          LOG.info("baseCol = " + baseCol);
          ServerIntFloatRow row = (ServerIntFloatRow) serverPartition.getRow(rowindex);
          IntFloatVector vector = (IntFloatVector) row.getSplit();
          if (vector.isDense()){
            float[] data = vector.getStorage().getValues();
            for (Map.Entry<Integer, PartitionMeta> entry2: entry.getValue().entrySet()){
              PartitionMeta partitionMeta = entry2.getValue();
              String savePath_final = partitionMeta.savePath;
              int startIndex = (int) partitionMeta.getStartCol() - baseCol;
              int endIndex = (int) partitionMeta.getEndCol() - baseCol;
              LOG.info("startIndex = " + startIndex);
              LOG.info("endIndex = " + endIndex);
              if (startIndex >= 0 && startIndex <= endIndex && endIndex <= data.length){
                savedFiles.add(savePath_final);
                Path saveFilePath = new Path(savePath_final);
                try {
                  fs = saveFilePath.getFileSystem(conf);
                  LOG.info("fs class = " + fs.getClass());
                  // Path tmpDestFile = HdfsUtil.toTmpPath(saveFilePath);
                  // FSDataOutputStream out = fs.create(tmpDestFile);
                  FSDataOutputStream out = fs.create(saveFilePath);
                  IntFloatElement element = new IntFloatElement();
                  String sep = ",";
                  for (int j = startIndex; j < endIndex; j++) {
                    element.rowId = row.getRowId();
                    element.colId = baseCol + j;
                    element.value = data[j];
                    out.writeBytes(
                            String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
                                    .valueOf(element.value) + "\n");
                  }
                  out.flush();
                  out.close();
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            }
          }
        }
      }
    }
    try {
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return savedFiles;
  }

  public void read_test(List<String> savedFiles) {
    LOG.info("savedFiles size = " + savedFiles.size());
    FileSystem fs = null;

    for (int i = 0; i < savedFiles.size(); i++) {
      String readFile = savedFiles.get(i);
      LOG.info("readFile = " + readFile);
      Path readFilePath = new Path(readFile);
      try {
        fs = readFilePath.getFileSystem(conf);
        LOG.info("fs class = " + fs.getClass());
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        FSDataInputStream input = fs.open(readFilePath);
        for (int j = 0; j < 10; j++) {
          String line = input.readLine();
          String[] kv = line.split(",");
          LOG.info("rowId = " + Integer.valueOf(kv[0]));
          LOG.info("colId = " + Integer.valueOf(kv[1]));
          LOG.info("value = " + Float.valueOf(kv[2]));
        }
        input.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    try {
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  /* code end */

  /**
   * Done, will notify master and exit
   */
  public void done() {
    LOG.info("done*****************************"); //////
    try {
      master.done();
      LOG.info("send done message to master success");
    } catch (ServiceException e) {
      LOG.error("send done message to master failed ", e);
    } finally {
      stop(0);
    }
  }

  /**
   * Failed, will notify master and exit
   *
   * @param errorLog the error log
   */
  public void failed(String errorLog) {
    LOG.info("failed*****************************"); //////
    try {
      master.failed(errorLog);
      LOG.info("send failed message to master success");
    } catch (ServiceException e) {
      LOG.error("send failed message to master failed ", e);
    } finally {
      stop(-1);
    }
  }

  /**
   * Gets parameter server service.
   *
   * @return the ps server service
   */
  public ParameterServerService getPsService() {
    return psServerService;
  }

  /**
   * Gets rpc client to master
   *
   * @return MasterProtocol rpc client to master
   */
  public MasterClient getMaster() {
    return master;
  }

  /**
   * Get attempt index
   *
   * @return attempt index
   */
  public int getAttemptIndex() {
    return attemptIndex;
  }

  /**
   * Get location manager
   *
   * @return location manager
   */
  public PSLocationManager getLocationManager() {
    return locationManager;
  }

  /**
   * TODO
   * Get PS 2 PS update pusher
   * @return PS 2 PS update pusher
   */
  // public PS2PSPusherImpl getPs2PSPusher() {
  // return ps2PSPusher;
  // }

  /**
   * Get RPC worker pool
   *
   * @return RPC worker pool
   */
  public WorkerPool getWorkerPool() {
    return workerPool;
  }

  /**
   * Get File Read/Writer executors
   *
   * @return File Read/Writer executors
   */
  public PSModelIOExecutor getPSModelIOExecutor() {
    return ioExecutor;
  }

  /**
   * Get Snapshot dumper
   *
   * @return Snapshot dumper
   */
  public SnapshotDumper getSnapshotDumper() {
    return snapshotDumper;
  }

  /**
   * Get PS failed information reporter
   *
   * @return PS failed information reporter
   */
  public PSFailedReport getPSFailedReport() {
    return psFailedReport;
  }

  /**
   * Get PS running context
   *
   * @return PS running context
   */
  public RunningContext getRunningContext() {
    return runningContext;
  }
}
