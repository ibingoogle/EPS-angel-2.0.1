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


package com.tencent.angel.ps.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.MasterProtocol;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.model.PSMatricesLoadResult;
import com.tencent.angel.model.PSMatricesSaveResult;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.*;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.ParameterServer;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.PSLocation;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Master RPC client
 */
public class MasterClient {
  private static final Log LOG = LogFactory.getLog(ParameterServer.class);
  /**
   * Master rpc protocol
   */
  private volatile MasterProtocol masterProxy;

  private volatile TConnection connection;

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Create MasterClient
   *
   * @param context PS context
   */
  public MasterClient(PSContext context) {
    this.context = context;
  }

  /**
   * Init
   */
  public void init() {
    connection = TConnectionManager.getConnection(context.getConf());
    Location masterLoc = context.getLocationManager().getMasterLocation();
    try {
      masterProxy = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());
    } catch (Throwable e) {
      LOG.fatal("Connect to master failed! PS is to exit now!", e);
      context.getPs().failed("Connect to master failed!");
    }
  }

  /**
   * Start
   */
  public void start() {

  }

  /**
   * Stop
   */
  public void stop() {
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        LOG.error("close connection falied ", e);
      }
    }
  }

  /**
   * Get task clocks for all matrices from Master
   *
   * @return task clocks for all matrices from Master
   * @throws ServiceException
   */
  public Int2ObjectOpenHashMap<Int2IntOpenHashMap> getTaskMatrixClocks() throws ServiceException {
    /* new code */
    LOG.info("Int2ObjectOpenHashMap<Int2IntOpenHashMap> getTaskMatrixClocks()......");
    /* code end */

    GetTaskMatrixClockResponse response =
      masterProxy.getTaskMatrixClocks(null, GetTaskMatrixClockRequest.newBuilder().build());
    Int2ObjectOpenHashMap<Int2IntOpenHashMap> taskIdToMatrixClocksMap =
      new Int2ObjectOpenHashMap<>(response.getTaskMatrixClocksCount());

    List<TaskMatrixClock> taskMatrixClocks = response.getTaskMatrixClocksList();
    int size = taskMatrixClocks.size();
    int matrixNum;
    LOG.info("taskMatrixClocks.size() = " + size); //////
    for (int i = 0; i < size; i++) {
      LOG.info("size index = " + i); //////
      Int2IntOpenHashMap matrixIdToClockMap =
        new Int2IntOpenHashMap(taskMatrixClocks.get(i).getMatrixClocksCount());
      LOG.info("taskIndex = " + taskMatrixClocks.get(i).getTaskId().getTaskIndex()); //////
      LOG.info("matrixIdToClockMap_ToString = " + matrixIdToClockMap.toString()); //////
      taskIdToMatrixClocksMap
        .put(taskMatrixClocks.get(i).getTaskId().getTaskIndex(), matrixIdToClockMap);
      List<MatrixClock> matrixClocks = taskMatrixClocks.get(i).getMatrixClocksList();
      matrixNum = matrixClocks.size();
      for (int j = 0; j < matrixNum; j++) {
        LOG.info("      MatrixId = " + matrixClocks.get(j).getMatrixId()); //////
        LOG.info("      Clock = " + matrixClocks.get(j).getClock()); //////
        matrixIdToClockMap.put(matrixClocks.get(j).getMatrixId(), matrixClocks.get(j).getClock());
      }
    }

    return taskIdToMatrixClocksMap;
  }

  /**
   * Report PS run over successfully to Master
   *
   * @throws ServiceException
   */
  public void done() throws ServiceException {
    masterProxy.psDone(null, PSDoneRequest.newBuilder()
      .setPsAttemptId(ProtobufUtil.convertToIdProto(context.getPSAttemptId())).build());
  }

  /**
   * Report PS run failed to Master
   *
   * @param errorLog failed message
   * @throws ServiceException
   */
  public void failed(String errorLog) throws ServiceException {
    masterProxy.psError(null, PSErrorRequest.newBuilder()
      .setPsAttemptId(ProtobufUtil.convertToIdProto(context.getPSAttemptId())).setMsg(errorLog)
      .build());
  }

  /**
   * Register to Master
   *
   * @throws IOException
   * @throws ServiceException
   */
  public void register() throws IOException, ServiceException {
    PSRegisterRequest.Builder regBuilder = PSRegisterRequest.newBuilder();
    regBuilder.setPsAttemptId(ProtobufUtil.convertToIdProto(context.getPSAttemptId()));
    try {
      Location location =
        new Location(InetAddress.getLocalHost().getHostAddress(), context.getPsService().getPort());
      regBuilder.setLocation(ProtobufUtil.convertLocation(location));
    } catch (UnknownHostException eop) {
      LOG.error("UnknownHostException: " + eop);
      throw new IOException(eop);
    }

    masterProxy.psRegister(null, regBuilder.build());
  }

  /**
   * Heartbeat to Master
   *
   * @param request heartbeat message
   * @return heartbeat response
   * @throws ServiceException
   */
  public PSReportResponse psReport(PSReportRequest request) throws ServiceException {
    return masterProxy.psReport(null, request);
  }

  /* new code */
  public PSRemoveResponse psRemove(PSRemoveRequest request) throws ServiceException {
    return masterProxy.psRemove(null, request);
  }
  /* code end */

  /**
   * Get a ps location from master
   *
   * @param serverId server id
   * @return PS location
   * @throws ServiceException
   */
  public Location getPsLocation(ParameterServerId serverId) throws ServiceException {
    MLProtos.GetPSLocationReponse response = masterProxy.getPSLocation(null,
      MLProtos.GetPSLocationRequest.newBuilder().setPsId(ProtobufUtil.convertToIdProto(serverId))
        .build());
    return ProtobufUtil.convertToLocation(response.getPsLocation());
  }

  /**
   * Get the stored pss and the locations for a matrix partition
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @return the stored pss and the locations
   * @throws ServiceException
   */
  public PartitionLocation getPartLocation(int matrixId, int partId) throws ServiceException {
    MLProtos.GetPartLocationResponse response = masterProxy.getPartLocation(null,
      MLProtos.GetPartLocationRequest.newBuilder().setMatrixId(matrixId).setPartId(partId).build());
    List<MLProtos.PSLocationProto> psLocsProto = response.getLocationsList();

    int size = psLocsProto.size();
    List<PSLocation> psLocs = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      psLocs.add(new PSLocation(ProtobufUtil.convertToId(psLocsProto.get(i).getPsId()),
        ProtobufUtil.convertToLocation(psLocsProto.get(i))));
    }
    return new PartitionLocation(psLocs);
  }

  /**
   * Get the stored pss for a matrix partition
   *
   * @param matrixId    matrix id
   * @param partitionId partition id
   * @return the stored pss
   * @throws ServiceException
   */
  public List<ParameterServerId> getStoredPss(int matrixId, int partitionId)
    throws ServiceException {
    List<MLProtos.PSIdProto> psIdProtos = masterProxy.getStoredPss(null,
      MLProtos.GetStoredPssRequest.newBuilder().setMatrixId(matrixId).setPartId(partitionId)
        .build()).getPsIdsList();
    int size = psIdProtos.size();
    List<ParameterServerId> psIds = new ArrayList<>(psIdProtos.size());
    for (int i = 0; i < size; i++) {
      psIds.add(ProtobufUtil.convertToId(psIdProtos.get(i)));
    }
    return psIds;
  }

  /**
   * Get current iteration
   *
   * @return current iteration
   * @throws ServiceException
   */
  public int getIteration() throws ServiceException {
    return masterProxy.getIteration(null, GetIterationRequest.newBuilder().build()).getIteration();
  }

  /**
   * Get matrices meta for this ps
   *
   * @return
   * @throws ServiceException
   * @throws ClassNotFoundException
   */
  public List<MatrixMeta> getMatricesMeta() throws ServiceException, ClassNotFoundException {
    /* new code */
    LOG.info("List<MatrixMeta> getMatricesMeta()......");
    /* code end */

    GetPSMatricesResponse response = masterProxy.getPSMatricesMeta(null,
      GetPSMatricesMetaRequest.newBuilder()
        .setPsId(ProtobufUtil.convertToIdProto(context.getPSAttemptId().getPsId())).build());
    List<MLProtos.MatrixMetaProto> matricesMataProto = response.getMatricesMetaList();
    int size = matricesMataProto.size();
    List<MatrixMeta> matricesMeta = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      matricesMeta.add(ProtobufUtil.convertToMatrixMeta(matricesMataProto.get(i)));
    }
    /* new code */
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

    return matricesMeta;
  }

  /**
   * Notify save result
   *
   * @param result save result
   * @throws ServiceException
   */
  public void saveFinish(PSMatricesSaveResult result) throws ServiceException {
    masterProxy.saveFinish(null, SaveFinishRequest.newBuilder()
      .setPsAttemptId(ProtobufUtil.convertToIdProto(context.getPSAttemptId()))
      .setResult(ProtobufUtil.convert(result)).build());
  }

  /**
   * Notify load result
   *
   * @param result load result
   * @throws ServiceException
   */
  public void loadFinish(PSMatricesLoadResult result) throws ServiceException {
    masterProxy.loadFinish(null, LoadFinishRequest.newBuilder()
      .setPsAttemptId(ProtobufUtil.convertToIdProto(context.getPSAttemptId()))
      .setResult(ProtobufUtil.convert(result)).build());
  }

  /**
   * Notify master save start
   *
   * @param requestId    save request id
   * @param subRequestId save sub-request id
   * @throws ServiceException
   */
  public void saveStart(int requestId, int subRequestId) throws ServiceException {
    masterProxy.saveStart(null, SaveStartRequest.newBuilder()
      .setPsAttemptId(ProtobufUtil.convertToIdProto(context.getPSAttemptId()))
      .setRequestId(requestId).setSubRequestId(subRequestId).build());
  }

  /**
   * Notify master load start
   *
   * @param requestId    load request id
   * @param subRequestId load sub-request id
   * @throws ServiceException
   */
  public void loadStart(int requestId, int subRequestId) throws ServiceException {
    masterProxy.loadStart(null, LoadStartRequest.newBuilder()
      .setPsAttemptId(ProtobufUtil.convertToIdProto(context.getPSAttemptId()))
      .setRequestId(requestId).setSubRequestId(subRequestId).build());
  }
}
