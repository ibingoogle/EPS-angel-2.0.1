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


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.client.local.AngelLocalClient;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.matrix.RowBasedMatrix;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.server.data.response.GetClocksResponse;
import com.tencent.angel.ps.server.data.response.RemoveWorkerResponse;//////
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.cache.MatricesCache;
import com.tencent.angel.psagent.matrix.oplog.cache.*;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportInterface;
import com.tencent.angel.psagent.task.TaskContext;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The adapter between user requests and actual rpc requests. Because a matrix is generally
 * distributed in multiple parameter servers, so an application request generally corresponds to
 * multiple rpc requests. The adapter can split the application request to sub-requests(rpc
 * requests) and merge the results of them, then return the final result.
 */
public class UserRequestAdapter {
  private static final Log LOG = LogFactory.getLog(UserRequestAdapter.class);
  /**
   * matrix id to the lock for GET_ROWS request map
   */
  private final ConcurrentHashMap<Integer, ReentrantLock> locks;

  /**
   * result cache for GET_ROWS requests
   */
  private final ConcurrentHashMap<RowIndex, GetRowsResult> resultsMap;

  /**
   * matrix id to fetching rows indexes map, use to distinct requests to same rows
   */
  private final ConcurrentHashMap<Integer, IntOpenHashSet> fetchingRowSets;

  /**
   * matrix id -> (row index -> the number of row splits the row contains), se to determine whether
   * all splits for a row are all fetched
   */
  private final Map<Integer, Int2IntOpenHashMap> matrixToRowSplitSizeCache;

  /**
   * Distinct get row request set
   */
  private final ConcurrentHashMap<GetRowRequest, Integer> getRowSubrespons;

  /**
   * Request id to sub request result cache map
   */
  private final ConcurrentHashMap<Integer, PartitionResponseCache> requestIdToSubresponsMap;

  /**
   * Request id to request map
   */
  private final ConcurrentHashMap<Integer, UserRequest> requests;

  /**
   * Request id to result map
   */
  private final ConcurrentHashMap<Integer, FutureResult> requestIdToResultMap;

  /**
   * Sub response merge worker pool
   */
  private volatile ForkJoinPool workerPool;


  /**
   * stop the merge dispatcher and all workers
   */
  private final AtomicBoolean stopped;

  private final int partNumThreshold = 50;

  private final int colNumThreshold = 10000000;


  /**
   * Create a new UserRequestAdapter.
   */
  public UserRequestAdapter() {
    locks = new ConcurrentHashMap<>();
    resultsMap = new ConcurrentHashMap<>();
    fetchingRowSets = new ConcurrentHashMap<>();
    matrixToRowSplitSizeCache = new HashMap<>();
    getRowSubrespons = new ConcurrentHashMap<>();

    requestIdToSubresponsMap = new ConcurrentHashMap<>();
    requests = new ConcurrentHashMap<>();
    requestIdToResultMap = new ConcurrentHashMap<>();

    stopped = new AtomicBoolean(false);
  }

  /**
   * Start the sub-request results merge dispatcher.
   */
  public void start() {
    workerPool = new ForkJoinPool(16);
  }

  /**
   * Stop the merge dispatcher and all workers.
   */
  public void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }

    if (workerPool != null) {
      workerPool.shutdownNow();
      workerPool = null;
    }
  }

  private void checkParams(int matrixId) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if(matrixMeta == null) {
      throw new AngelException("can not find matrix " + matrixId);
    }
  }

  private void checkParams(int matrixId, int rowId) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if(matrixMeta == null) {
      throw new AngelException("can not find matrix " + matrixId);
    }
    int rowNum = matrixMeta.getRowNum();
    if(rowId < 0 || rowId >= rowNum) {
      throw new AngelException("not valid row id, row id is in range[0," + rowNum + ")");
    }
  }

  private void checkParams(int matrixId, int [] rowIds) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if(matrixMeta == null) {
      throw new AngelException("can not find matrix " + matrixId);
    }

    if(rowIds == null || rowIds.length == 0) {
      throw new AngelException("row ids is empty");
    }

    int rowNum = matrixMeta.getRowNum();
    for(int rowId : rowIds) {
      if(rowId < 0 || rowId >= rowNum) {
        throw new AngelException("not valid row id, row id is in range[0," + rowNum + ")");
      }
    }
  }

  /* new code */
  public void removeWorker(int taskIndex){
    ParameterServerId[] serverIds =
            PSAgentContext.get().getLocationManager().getPsIds();
    Map<ParameterServerId, Future> psIdToResultMap =
            new HashMap<>(serverIds.length);
    // MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    MatrixTransportInterface matrixClient =
            PSAgentContext.get().getMatrixTransportClient();
    // Send remove worker request to every ps
    for (int i = 0; i < serverIds.length; i++) {
      LOG.info("Send remove worker request to every ps; serverId = " + serverIds[i]);
      try {
        psIdToResultMap.put(serverIds[i], matrixClient.removeWorker(serverIds[i], taskIndex));
      } catch (Exception e) {
        LOG.error("remove worker failed from server " + serverIds[i] + " failed, ", e);
      }
    }
    // Wait the responses
    try {
      for (Entry<ParameterServerId, Future> resultEntry : psIdToResultMap.entrySet()) {
        RemoveWorkerResponse response = (RemoveWorkerResponse) resultEntry.getValue().get();
        if (response.getResponseType() == com.tencent.angel.ps.server.data.response.ResponseType.SUCCESS) {
          int numRestWorkers = response.numRestWorkers;
          LOG.info("serverId = " + resultEntry.getKey().getIndex() + ", num of rest workers = " + numRestWorkers);
        }
      }
    } catch (Exception e) {
      LOG.error("remove worker failed, ", e);
    }
  }
  /* code end*/

  public Vector getRow(int matrixId, int rowIndex, int clock)
    throws InterruptedException, ExecutionException {
    LOG.debug("start to getRow request, matrix=" + matrixId + ", rowIndex=" + rowIndex + ", clock="
      + clock);

    LOG.info("start to getRow request, matrix=" + matrixId + ", rowIndex=" + rowIndex + ", clock="
            + clock); //////
    checkParams(matrixId, rowIndex);
    long startTs = System.currentTimeMillis();

    // Get partitions for this row
    List<PartitionKey> partList =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIndex);
    /* new code */
    LOG.info("partList size = " + partList.size());
    LOG.info("rowIndex = " + rowIndex);
    LOG.info("clock = " + clock);
    /* code end */
    GetRowRequest request = new GetRowRequest(matrixId, rowIndex, clock);
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);

    // Distinct get row requests
    FutureResult<Vector> result;
    Integer requestId = getRowSubrespons.get(request);
    if (requestId != null) {
      result = requestIdToResultMap.get(requestId);
    } else {
      result = null;
    }

    // Need get from ps or storage/cache
    if (result == null) {
      LOG.info("result == null ......"); //////
      // Switch to new request id, send a new request
      try {
        requestId = request.getRequestId();
        result = new FutureResult<>();
        GetRowPipelineCache responseCache =
          new GetRowPipelineCache(partList.size(), meta.getRowType());
        requests.put(requestId, request);
        requestIdToResultMap.put(requestId, result);
        requestIdToSubresponsMap.put(requestId, responseCache);
        getRowSubrespons.put(request, requestId);

        // First get this row from matrix storage
        //MatrixStorage matrixStorage =
        //  PSAgentContext.get().getMatrixStorageManager().getMatrixStoage(matrixId);
        //TVector row = matrixStorage.getRow(rowIndex);
        //if (row != null && row.getClock() >= clock) {
        //  result.set(row);
        //  return row;
        //}

        // Get row splits of this row from the matrix cache first
        MatricesCache matricesCache = PSAgentContext.get().getMatricesCache();
        MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
        int size = partList.size();
        for (int i = 0; i < size; i++) {
          LOG.info("get Row => i = " + i); //////
          ServerRow rowSplit = matricesCache.getRowSplit(matrixId, partList.get(i), rowIndex);
          /* new code */
          if (rowSplit != null){
            LOG.info("rowSplit != null");
            LOG.info("rowSplit.getClock() = " + rowSplit.getClock());
            LOG.info("clock = " + clock);
          }else {
            LOG.info("rowSplit == null");
          }
          /* code end */
          if (rowSplit != null && rowSplit.getClock() >= clock) {
            LOG.info("rowSplit != null && rowSplit.getClock() >= clock"); /////
            notifyResponse(requestId, rowSplit);
            //responseCache.addSubResponse(rowSplit);
          } else {
            // If the row split does not exist in cache, get it from parameter server
            matrixClient.getRowSplit(requestId, partList.get(i), rowIndex, clock);
          }
        }

        // Wait the final result
        Vector row = result.get();
        LOG.debug("get row use time=" + (System.currentTimeMillis() - startTs));

        /* new code */
        LOG.info("row.getSize() = " + row.getSize());
        LOG.info("row.getRowId = " + row.getRowId());
        LOG.info("get row use time=" + (System.currentTimeMillis() - startTs));
        /* code end */
        // Put it to the matrix cache
        // matrixStorage.addRow(rowIndex, row);
        return row;
      } finally {
        requests.remove(requestId);
        requestIdToResultMap.remove(requestId);
        requestIdToSubresponsMap.remove(requestId);
        getRowSubrespons.remove(request);
      }
    } else {
      LOG.info("result != null ......"); //////
      // Just wait result
      return result.get();
    }
  }

  /**
   * Update matrix use a udf.
   *
   * @param updateFunc update udf function
   * @return Future<VoidResult> update future result
   */
  public Future<VoidResult> update(UpdateFunc updateFunc) {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    UpdateParam param = updateFunc.getParam();

    // Split the param use matrix partitions
    List<PartitionUpdateParam> partParams = param.split();


    /* old code */
    // int size = partParams.size();
    /* new code */
    int active_partParams_size = 0;
    for (int i = 0; i < partParams.size(); i++){
      if (partParams.get(i).getPartKey().status){
        active_partParams_size++;
        LOG.info("partitionId in PartitionUpdateParam = " + partParams.get(i).getPartKey().getPartitionId());
      }
    }
    LOG.info("active_partParams_size = " + active_partParams_size); // synchronization operations
    /* code end */


    UpdatePSFRequest request = new UpdatePSFRequest(updateFunc);
    /* old code */
    // UpdaterResponseCache cache = new UpdaterResponseCache(size);
    /* new code */
    UpdaterResponseCache cache = new UpdaterResponseCache(active_partParams_size);
    /* code end */
    FutureResult<VoidResult> result = new FutureResult<>();
    int requestId = request.getRequestId();

    requests.put(requestId, request);
    requestIdToSubresponsMap.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);

    // Send request to PSS
    /* old code
    for (int i = 0; i < size; i++) {
      matrixClient.update(request.getRequestId(), updateFunc, partParams.get(i));
    }
    */
    /* new code */
    for (int i = 0; i < partParams.size(); i++) {
      if (partParams.get(i).getPartKey().status) {
        matrixClient.update(request.getRequestId(), updateFunc, partParams.get(i));
      }else {
        LOG.info("update in worker 0 => removed partitionId = " + partParams.get(i).getPartKey().getPartitionId());
      }
    }
    /* code end */

    return result;
  }

  /**
   * Flush the matrix oplog to parameter servers.
   *
   * @param matrixId    matrix id
   * @param taskContext task context
   * @param matrixOpLog matrix oplog
   * @param updateClock true means we should update the clock value after update matrix
   * @return Future<VoidResult> flush future result
   */
  public Future<VoidResult> flush(int matrixId, TaskContext taskContext, MatrixOpLog matrixOpLog,
    boolean updateClock) {
    checkParams(matrixId);
    if (!updateClock && (matrixOpLog == null)) {
      FutureResult<VoidResult> ret = new FutureResult<VoidResult>();
      ret.set(new VoidResult(ResponseType.SUCCESS));
      return ret;
    }

    LOG.info("matrixOpLog = " + matrixOpLog + ", updateClock = " + updateClock);//////

    Map<PartitionKey, List<RowUpdateSplit>> psUpdateData =
      new HashMap<PartitionKey, List<RowUpdateSplit>>();
    FlushRequest request =
      new FlushRequest(taskContext.getMatrixClock(matrixId), taskContext.getIndex(), matrixId,
        matrixOpLog, updateClock);

    long startTs = System.currentTimeMillis();
    // Split the matrix oplog according to the matrix partitions
    if (matrixOpLog != null) {
      matrixOpLog.split(psUpdateData);
    }
    LOG.debug("split use time=" + (System.currentTimeMillis() - startTs));

    // If need update clock, we should send requests to all partitions
    if (updateClock) {
      fillPartRequestForClock(matrixId, psUpdateData, taskContext);
    }

    /* old code
    FlushResponseCache cache = new FlushResponseCache(psUpdateData.size());
    /* new code */
    int active_psUpdateData_size = 0;
    for (Map.Entry<PartitionKey, List<RowUpdateSplit>> entry :psUpdateData.entrySet()){
      if (entry.getKey().status) active_psUpdateData_size++;
    }
    LOG.info("active_psUpdateData_size = " + active_psUpdateData_size); // clock operation
    FlushResponseCache cache = new FlushResponseCache(active_psUpdateData_size);
    /* code end */


    FutureResult<VoidResult> result = new FutureResult<>();
    int requestId = request.getRequestId();
    requestIdToSubresponsMap.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    // Send request to PSS
    LOG.info("Flush the matrix oplog to parameter servers.");//////
    plus(requestId, matrixId, psUpdateData, taskContext, updateClock);
    return result;
  }

  private void fillPartRequestForClock(int matrixId,
    Map<PartitionKey, List<RowUpdateSplit>> psUpdateData, TaskContext taskContext) {
    List<PartitionKey> partitions =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = partitions.size();
    LOG.info("partitions.size() in fillPartRequestForClock = " + size); //////
    for (int i = 0; i < size; i++) {
      if (!psUpdateData.containsKey(partitions.get(i))) {
        LOG.info("partitionId = " + partitions.get(i).getPartitionId()); //////
        LOG.info("partitionKey status" + partitions.get(i).status); //////
        psUpdateData.put(partitions.get(i), new ArrayList<>());
      }
    }
  }

  private void plus(int requestId, int matrixId,
    Map<PartitionKey, List<RowUpdateSplit>> psUpdateData, TaskContext taskContext,
    boolean updateClock) {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();

    int clock;
    if (taskContext != null) {
      clock = taskContext.getMatrixClock(matrixId);
    } else {
      clock = -1;
    }

    int i = 0;//////

    for (Entry<PartitionKey, List<RowUpdateSplit>> partUpdateEntry : psUpdateData.entrySet()) {
      /* old code
      matrixClient.update(requestId, matrixId, partUpdateEntry.getKey(),
        new RowSplitsUpdateItem(partUpdateEntry.getValue()), taskContext, clock, updateClock,
        UpdateOp.PLUS);
      */
      /*new code*/
      PartitionKey Pkey = partUpdateEntry.getKey();
      if (Pkey.status) {
        matrixClient.update(requestId, matrixId, Pkey,
                new RowSplitsUpdateItem(partUpdateEntry.getValue()), taskContext, clock, updateClock,
                UpdateOp.PLUS);
        LOG.info(i);
        LOG.info("go to matrixClient.update(*) with clock = " + clock + " updateClock = " + updateClock);
        LOG.info("requestId = " + requestId);
        LOG.info("matrixId = " + matrixId);
        LOG.info("PartitionKey in plus = " + Pkey.toString());
        i++;
      }else {
        LOG.info("clock => removed partitionKey index = " + Pkey.getPartitionId());
      }
      /*code end*/
    }
  }

  /**
   * Get rows use pipeline mode.
   *
   * @param result       result cache
   * @param rowIndex     the indexes of rows that need to fetch from ps
   * @param rpcBatchSize how many rows to be fetched in a rpc
   * @param clock        clock value
   * @return result cache
   */
  public GetRowsResult getRowsFlow(GetRowsResult result, RowIndex rowIndex, int rpcBatchSize,
    int clock) {
    LOG.debug("get rows request, rowIndex=" + rowIndex);
    checkParams(rowIndex.getMatrixId());
    if (rpcBatchSize == -1) {
      rpcBatchSize = chooseRpcBatchSize(rowIndex);
    }

    // Filter the rowIds which are fetching now
    ReentrantLock lock = getLock(rowIndex.getMatrixId());
    RowIndex needFetchRows = null;
    try {
      lock.lock();
      resultsMap.put(rowIndex, result);

      if (!fetchingRowSets.containsKey(rowIndex.getMatrixId())) {
        fetchingRowSets.put(rowIndex.getMatrixId(), new IntOpenHashSet());
      }

      if (!matrixToRowSplitSizeCache.containsKey(rowIndex.getMatrixId())) {
        matrixToRowSplitSizeCache.put(rowIndex.getMatrixId(), new Int2IntOpenHashMap());
      }

      needFetchRows = findNewRows(rowIndex);
    } finally {
      lock.unlock();
    }

    // Send the rowIndex to rpc dispatcher and return immediately
    if (needFetchRows.getRowsNumber() > 0) {
      dispatchGetRows(needFetchRows, rpcBatchSize, clock);
    }
    return resultsMap.get(rowIndex);
  }

  /**
   * Get elements of the row use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId    row id
   * @param indices  elements indices
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector> get(int matrixId, int rowId, int[] indices) throws AngelException {
    return get(new IntIndexGetRowRequest(matrixId, rowId, indices, null));
  }

  /**
   * Get elements of the row use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId    row id
   * @param indices  elements indices
   * @param func     element init function
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector> get(int matrixId, int rowId, int[] indices, InitFunc func)
    throws AngelException {
    return get(new IntIndexGetRowRequest(matrixId, rowId, indices, func));
  }

  /**
   * Get elements of the row use long indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId    row id
   * @param indices  elements indices
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector> get(int matrixId, int rowId, long[] indices) throws AngelException {
    return get(new LongIndexGetRowRequest(matrixId, rowId, indices, null));
  }

  /**
   * Get elements of the row use long indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId    row id
   * @param indices  elements indices
   * @param func     element init function
   * @return the Vector use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector> get(int matrixId, int rowId, long[] indices, InitFunc func)
    throws AngelException {
    return get(new LongIndexGetRowRequest(matrixId, rowId, indices, func));
  }

  private FutureResult<Vector> get(IndexGetRowRequest request) {
    checkParams(request.getMatrixId(), request.getRowId());

    List<PartitionKey> partitions = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    FutureResult<Vector> result = new FutureResult<>();
    Map<PartitionKey, IndicesView> splits;

    long startTs = System.currentTimeMillis();
    if (request instanceof IntIndexGetRowRequest) {
      splits = split(partitions, ((IntIndexGetRowRequest) request).getIndices());
    } else {
      splits = split(partitions, ((LongIndexGetRowRequest) request).getIndices());
    }
    // LOG.info("get row split use time=" + (System.currentTimeMillis() - startTs));

    IndexGetRowCache cache = new IndexGetRowCache(splits.size());
    int requestId = request.getRequestId();
    requestIdToSubresponsMap.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    // LOG.info("start to request " + requestId);
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (Entry<PartitionKey, IndicesView> entry : splits.entrySet()) {
      matrixClient.indexGetRow(requestId, request.getMatrixId(), request.getRowId(), entry.getKey(),
        entry.getValue(), request.getFunc());
    }
    return result;
  }

  /**
   * Get elements of the rows use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowIds   rows ids
   * @param indices  elements indices
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, int[] indices)
    throws AngelException {
    /* new code */
    LOG.info("public FutureResult<Vector[]> get(int matrixId, int[] rowIds, int[] indices)");
    LOG.info("matrixId = " + matrixId);
    LOG.info("rowIds.size " + rowIds.length);
    LOG.info("indices.size = " + indices.length); // total number of pulled parameters
    /* code end */
    return get(new IntIndexGetRowsRequest(matrixId, rowIds, indices, null));
  }

  /**
   * Get elements of the rows use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowIds   rows ids
   * @param indices  elements indices
   * @param func     element init function
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, int[] indices, InitFunc func)
    throws AngelException {
    return get(new IntIndexGetRowsRequest(matrixId, rowIds, indices, func));
  }

  private IndicesView getIndicesView(PartitionKey partKey, Map<PartitionKey, IndicesView> views) {
    for (Entry<PartitionKey, IndicesView> entry : views.entrySet()) {
      if (partKey.getStartCol() == entry.getKey().getStartCol() && partKey.getEndCol() == entry
        .getKey().getEndCol()) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Get elements of the rows use long indices, the row type should has "long" type indices
   *
   * @param matrixId matrix id
   * @param rowIds   rows ids
   * @param indices  elements indices
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, long[] indices)
    throws AngelException {
    /* new code */
    LOG.info("public FutureResult<Vector[]> get(int matrixId, int[] rowIds, long[] indices)");
    LOG.info("matrixId = " + matrixId);
    LOG.info("rowIds.size " + rowIds.length);
    LOG.info("indices.size = " + indices.length);
    /* code end */
    return get(new LongIndexGetRowsRequest(matrixId, rowIds, indices, null));
  }

  /**
   * Get elements of the rows use long indices, the row type should has "long" type indices
   *
   * @param matrixId matrix id
   * @param rowIds   rows ids
   * @param indices  elements indices
   * @param func     element init function
   * @return the Vectors use sparse storage, contains indices and values
   * @throws AngelException
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, long[] indices, InitFunc func)
    throws AngelException {
    return get(new LongIndexGetRowsRequest(matrixId, rowIds, indices, func));
  }

  private FutureResult<Vector[]> get(IndexGetRowsRequest request) {
    /* new code */
    LOG.info("IndexGetRowsRequest request = " + request + ", class = " + request.getClass());
    //LOG.info("PSAgentContext.get().getPsAgent().print_PSAgent() in get(IndexGetRowsRequest request) from pullGradient");
    //PSAgentContext.get().getPsAgent().print_PSAgent();
    /* code end */

    checkParams(request.getMatrixId(), request.getRowIds());
    Map<PartitionKey, List<Integer>> partToRowIdsMap = PSAgentContext.get().getMatrixMetaManager()
      .getPartitionToRowsMap(request.getMatrixId(), request.getRowIds());
    List<PartitionKey> row0Parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), 0);

    /* new code */
    LOG.info("partToRowIdsMap =>");
    for(Map.Entry<PartitionKey, List<Integer>> entry: partToRowIdsMap.entrySet()){
      LOG.info("PartitionId = " + entry.getKey().getPartitionId());
      LOG.info("PartitionKey = " + entry.getKey().toString());
      for (int j = 0; j< entry.getValue().size(); j++){
        LOG.info("      RowId = " + entry.getValue().get(j));
      }
    }
    LOG.info("row0Parts =>");
    for (int i = 0; i<row0Parts.size(); i++){
      LOG.info("PartitionId = " + row0Parts.get(i).getPartitionId());
      LOG.info("PartitionKey = " + row0Parts.get(i).toString());
    }
    /* code end */

    FutureResult<Vector[]> result = new FutureResult<>();

    Map<PartitionKey, IndicesView> splits;
    if (request instanceof IntIndexGetRowsRequest) {
      splits = split(row0Parts, ((IntIndexGetRowsRequest) request).getColIds());
    } else {
      splits = split(row0Parts, ((LongIndexGetRowsRequest) request).getColIds());
    }

    List<PartitionKey> parts = new ArrayList<>(splits.keySet());
    parts.sort((PartitionKey p1, PartitionKey p2) -> {
      if(p1.getStartCol() > p2.getStartCol()) {
        return 1;
      } else if(p1.getStartCol() < p2.getStartCol()) {
        return -1;
      } else {
        return 0;
      }
    });

    /* new code */
    for (Map.Entry<PartitionKey, IndicesView> entry: splits.entrySet()){
      LOG.info("splits~~~~~~partitionKey = " + entry.getKey().toString());
      LOG.info("splits~~~~~~partitionId = " + entry.getKey().getPartitionId());
      LOG.info("IndicesView.startPos = " + entry.getValue().startPos);
      LOG.info("IndicesView.endPos = " + entry.getValue().endPos);
      LOG.info("IndicesView.start-end = " + (entry.getValue().endPos - entry.getValue().startPos));
    }
    for (int i = 0; i< parts.size(); i++){
      LOG.info("parts~~~~~~partitionKey = " + parts.get(i).toString());
      LOG.info("parts~~~~~~partitionId = " + parts.get(i).getPartitionId());
    }
    /* code end */

    Map<PartitionKey, IndicesView> validSplits = new HashMap<>(partToRowIdsMap.size());
    for (Entry<PartitionKey, List<Integer>> entry : partToRowIdsMap.entrySet()) {
      IndicesView indicesView = getIndicesView(entry.getKey(), splits);
      /* old code */
      // if (indicesView != null) {
      // validSplits.put(entry.getKey(), indicesView);
      // }
      /* new code */
      // ParameterServerId serverId = PSAgentContext.get().getMatrixMetaManager().getMasterPS(entry.getKey());
      if (indicesView != null) {
        validSplits.put(entry.getKey(), indicesView);
        /*
        if (serverId.getIndex() != removedParameterServerIndex || currentEpoch != rmServerEpoch) {
          validSplits.put(entry.getKey(), indicesView);
        }else if (!rmServerPull) {
          validSplits.put(entry.getKey(), indicesView);
        }else {
          PartitionKey removedPartKey = entry.getKey();
          LOG.info("removed Part Key Id in pullParameter = " + removedPartKey.getPartitionId());
        }
        */

      }
      /* code end */
    }

    /* new code */
    /*
    int size = parts.size();
    for (int i = 0; i < size;) {
      if (!parts.get(i).status){
        LOG.info("in parts, removed Part Key Id = " + parts.get(i).getPartitionId() + ", status = " + parts.get(i).status);
        parts.remove(i);
        size--;
      }else {
        i++;
      }
    }
    */
    int active_validSplits_size = 0;
    for (Map.Entry<PartitionKey, IndicesView> entry: validSplits.entrySet()){
      LOG.info("validSplits~~~~~~partitionKey = " + entry.getKey().toString());
      LOG.info("validSplits~~~~~~partitionId = " + entry.getKey().getPartitionId());
      LOG.info("IndicesView.startPos = " + entry.getValue().startPos);
      LOG.info("IndicesView.endPos = " + entry.getValue().endPos);
      LOG.info("IndicesView.start-end = " + (entry.getValue().endPos - entry.getValue().startPos));
      if (entry.getKey().status) active_validSplits_size++;
    }
    LOG.info("active_validSplits_size = " + active_validSplits_size);
    for (int i = 0; i< parts.size(); i++){
      LOG.info("real parts~~~~~~partitionKey = " + parts.get(i).toString());
      LOG.info("real parts~~~~~~partitionId = " + parts.get(i).getPartitionId());
    }
    //LOG.info("pullParam....checkpoint.........PSAgentContext.get().getPsAgent().print_PSAgent()");
    //PSAgentContext.get().getPsAgent().print_PSAgent();
    /* code end */

    /* old code
    IndexGetRowsCache cache = new IndexGetRowsCache(validSplits.size(), parts);
    /* new code */
    IndexGetRowsCache cache = new IndexGetRowsCache(active_validSplits_size, parts);
    /* code end */
    int requestId = request.getRequestId();
    requestIdToSubresponsMap.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    // LOG.info("start to request " + requestId);
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();

    for (Entry<PartitionKey, IndicesView> entry : validSplits.entrySet()) {
      /* old code
      matrixClient.indexGetRows(requestId, request.getMatrixId(), entry.getKey(),
        partToRowIdsMap.get(entry.getKey()), validSplits.get(entry.getKey()), request.getFunc());
      /* new code */
      if (entry.getKey().status){
        matrixClient.indexGetRows(requestId, request.getMatrixId(), entry.getKey(),
                partToRowIdsMap.get(entry.getKey()), validSplits.get(entry.getKey()), request.getFunc());
      }else {
        LOG.info("get => removed partKey Id = " + entry.getKey().getPartitionId());
      }
      /* code end */
    }
    return result;
  }


  /**
   * Get a row from ps use a udf.
   *
   * @param func get row udf
   * @return GetResult the result of the udf
   * @throws ExecutionException   exception thrown when attempting to retrieve the result of a task
   *                              that aborted by throwing an exception
   * @throws InterruptedException interrupted while wait the result
   */
  public GetResult get(GetFunc func) throws InterruptedException, ExecutionException {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    GetParam param = func.getParam();

    // Split param use matrix partitons
    List<PartitionGetParam> partParams = param.split();
    int size = partParams.size();

    GetPSFRequest request = new GetPSFRequest(func);

    int requestId = request.getRequestId();
    FutureResult<GetResult> result = new FutureResult<>();
    GetPSFResponseCache cache = new GetPSFResponseCache(size);

    try {
      requests.put(requestId, request);
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);

      for (int i = 0; i < size; i++) {
        matrixClient.get(requestId, func, partParams.get(i));
      }
      return result.get();
    } finally {
      requests.remove(requestId);
      requestIdToResultMap.remove(requestId);
      requestIdToSubresponsMap.remove(requestId);
    }
  }

  /**
   * Notify sub-response is received
   *
   * @param requestId   user request id
   * @param subResponse sub response
   */
  public void notifyResponse(int requestId, Object subResponse) {
    PartitionResponseCache cache = requestIdToSubresponsMap.get(requestId);
    FutureResult result = requestIdToResultMap.get(requestId);
    if (cache == null || result == null) {
      return;
    }

    try {
      cache.lock.lock();
      cache.addSubResponse(subResponse);
      UserRequest request = requests.get(requestId);

      // If all sub-results are received, just remove request and result cache
      if (cache.isReceivedOver()) {
        clear(requestId);
      }

      // LOG.info("request = " + request + ", cache = " + cache);
      if (request != null) {
        switch (request.getType()) {
          case GET_PSF:
            if (cache.canMerge()) {
              // LOG.info("start to merge " + cache + " for request " + request);
              long startTs = System.currentTimeMillis();
              result.set(((GetPSFRequest) request).getGetFunc().merge(cache.getSubResponses()));
              // LOG.info("psf get merge use time = " + (System.currentTimeMillis() - startTs));
            }
            break;

          case GET_ROW:
            LOG.info("case GET_ROW.........."); //////
            if (cache.canMerge()) {
              LOG.info("GET_ROW cache.canMerge()..........");
              if (!((GetRowPipelineCache) cache).merging.getAndSet(true)) {
                workerPool.execute(new RowMerger((GetRowRequest) request, cache, result));
              }
            }
            break;

          case INDEX_GET_ROW:
            if (cache.canMerge()) {
              workerPool.execute(new IndexRowMerger((IndexGetRowRequest) request, cache, result));
            }
            break;

          case INDEX_GET_ROWS:
            LOG.info("case INDEX_GET_ROWS.........."); //////
            LOG.info("subresponse = " + subResponse.toString()); //////
            if (cache.canMerge()) {
              /* new code */
              LOG.info("INDEX_GET_ROWS cache.canMerge()..........");
              LOG.info("get result from request = " + request);
              LOG.info("cache.getProgress() = " + cache.getProgress());
              /* code end */
              workerPool.execute(new IndexRowsMerger((IndexGetRowsRequest) request, cache, result));
            }
            break;

          case GET_ROWS:
            List<RowMergeItem> needMergeRows = ((GetRowsFlowCache) cache).getCanMergeRows();
            if (needMergeRows != null) {
              workerPool.execute(new RowsFlowMerger((GetRowsFlowRequest) request, needMergeRows));
            }
            break;

          case FLUSH:
            if (cache.canMerge()) {
              LOG.info("cache.canMerge in FLUSH"); //////
              result.set(new VoidResult(ResponseType.SUCCESS));
              updateMasterClock((FlushRequest) request);
            }
            break;

          default:
            if (cache.canMerge()) {
              result.set(new VoidResult(ResponseType.SUCCESS));
            }
            break;
        }
      } else {
        if (cache.canMerge()) {
          result.set(new VoidResult(ResponseType.SUCCESS));
        }
      }
    } finally {
      cache.lock.unlock();
    }
  }

  private void clear(int requestId) {
    requests.remove(requestId);
    requestIdToSubresponsMap.remove(requestId);
    requestIdToResultMap.remove(requestId);
  }

  private void updateMasterClock(FlushRequest request) {
    if (request.isUpdateClock()) {
      try {
        PSAgentContext.get().getMasterClient()
          .updateClock(request.getTaskIndex(), request.getMatrixId(), request.getClock());
      } catch (ServiceException e) {
        LOG.warn(
          "update clock to master failed. task=" + request.getTaskIndex() + ", matrix=" + request
            .getMatrixId() + ", clock=" + request.getClock());
      }
    }
  }

  private boolean useNewSplit(int matrixId, int rowId, Vector row) {
    boolean useAdaptive = PSAgentContext.get().getConf()
      .getBoolean(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE,
        AngelConf.DEFAULT_ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE);
    if (useAdaptive) {
      if (row instanceof ComponentVector || row.isDense()) {
        return true;
      }
      int partNum =
        PSAgentContext.get().getMatrixMetaManager().getRowPartitionSize(matrixId, rowId);
      if (partNum > partNumThreshold && row.getSize() < colNumThreshold) {
        return false;
      } else {
        return true;
      }
    } else {
      return PSAgentContext.get().getConf()
        .getBoolean(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_VIEW_ENABLE,
          AngelConf.DEFAULT_ANGEL_PSAGENT_UPDATE_SPLIT_VIEW_ENABLE);
    }
  }

  private boolean useNewSplit(int matrixId, Vector[] rows) {
    return useNewSplit(matrixId, rows[0].getRowId(), rows[0]);
  }

  private boolean useNewSplit(int matrixId, Matrix matrix) {
    int rowNum = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getRowNum();
    for (int i = 0; i < rowNum; i++) {
      Vector row = matrix.getRow(i);
      if (row != null && row.getSize() > 0) {
        return useNewSplit(matrixId, i, row);
      }
    }
    return false;
  }

  public Vector getRow(int matrixId, int rowId) throws ExecutionException, InterruptedException {
    return getRow(matrixId, rowId, -1);
  }

  public GetRowsResult getRowsFlow(GetRowsResult result, RowIndex index, int batchSize) {
    return getRowsFlow(result, index, batchSize, -1);
  }

  public Future<VoidResult> update(int matrixId, int rowId, Vector delta, UpdateOp op) {
    checkParams(matrixId, rowId);
    delta.setMatrixId(matrixId);
    delta.setRowId(rowId);

    if (useNewSplit(matrixId, rowId, delta)) {
      List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowId);
      Vector[] rows = new Vector[1];
      rows[0] = delta;

      UpdateRowRequest request = new UpdateRowRequest(matrixId, rowId, op);
      UpdateRowCache cache = new UpdateRowCache(partitions.size());
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);

      MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
      long colNum = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getColNum();
      for (PartitionKey partKey : partitions) {
        RowsViewUpdateItem item = new RowsViewUpdateItem(partKey, rows, colNum);
        matrixClient.update(requestId, request.getMatrixId(), partKey, item, null, -1, false, op);
      }
      return result;
    } else {
      List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowId);
      delta.setMatrixId(matrixId);
      delta.setRowId(rowId);
      Map<PartitionKey, RowUpdateSplit> splitMap = RowUpdateSplitUtils.split(delta, partitions);
      Map<PartitionKey, List<RowUpdateSplit>> splitListMap = new HashMap<>(splitMap.size());
      for (Entry<PartitionKey, RowUpdateSplit> entry : splitMap.entrySet()) {
        RowUpdateSplitContext context = new RowUpdateSplitContext();
        context.setEnableFilter(false);
        context.setFilterThreshold(0);
        context.setPartKey(entry.getKey());
        entry.getValue().setSplitContext(context);

        List<RowUpdateSplit> splitList = splitListMap.get(entry.getKey());
        if (splitList == null) {
          splitList = new ArrayList<>();
          splitListMap.put(entry.getKey(), splitList);
        }
        splitList.add(entry.getValue());
      }

      UpdateRowRequest request = new UpdateRowRequest(matrixId, rowId, op);
      UpdateRowCache cache = new UpdateRowCache(splitListMap.size());
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);
      plus(requestId, request.getMatrixId(), splitListMap, null, false);
      return result;
    }
  }

  public Future<VoidResult> update(int matrixId, Matrix delta, UpdateOp op) {
    checkParams(matrixId);

    delta.setMatrixId(matrixId);
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);

    if (useNewSplit(matrixId, delta)) {
      List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int rowNum = matrixMeta.getRowNum();
      int validRowNum = 0;
      for (int rowId = 0; rowId < rowNum; rowId++) {
        Vector vector = delta.getRow(rowId);
        if (vector != null)
          validRowNum++;
      }

      int index = 0;
      Vector[] rows = new Vector[validRowNum];
      for (int rowId = 0; rowId < rowNum && index < validRowNum; rowId++) {
        Vector vector = delta.getRow(rowId);
        if (vector != null) {
          vector.setMatrixId(matrixId);
          vector.setRowId(rowId);
          rows[index++] = vector;
        }
      }

      UpdateMatrixRequest request = new UpdateMatrixRequest(matrixId, op);
      UpdateMatrixCache cache = new UpdateMatrixCache(partitions.size());
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);

      MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
      long colNum = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getColNum();
      for (PartitionKey partKey : partitions) {
        RowsViewUpdateItem item = new RowsViewUpdateItem(partKey, rows, colNum);
        matrixClient.update(requestId, request.getMatrixId(), partKey, item, null, -1, false, op);
      }
      return result;
    } else {
      List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int rowNum = matrixMeta.getRowNum();

      Map<PartitionKey, List<RowUpdateSplit>> splitListMap = new HashMap<>();
      for (int rowId = 0; rowId < rowNum; rowId++) {
        Vector vector = delta.getRow(rowId);
        if (vector == null)
          continue;

        // Split this row according the matrix partitions
        Map<PartitionKey, RowUpdateSplit> splitMap = RowUpdateSplitUtils.split(vector, partitions);

        // Set split context
        for (Map.Entry<PartitionKey, RowUpdateSplit> entry : splitMap.entrySet()) {
          RowUpdateSplitContext context = new RowUpdateSplitContext();
          context.setEnableFilter(false);
          context.setFilterThreshold(0);
          context.setPartKey(entry.getKey());
          entry.getValue().setSplitContext(context);

          List<RowUpdateSplit> splitList = splitListMap.get(entry.getKey());
          if (splitList == null) {
            splitList = new ArrayList<>();
            splitListMap.put(entry.getKey(), splitList);
          }
          splitList.add(entry.getValue());
        }
      }

      UpdateMatrixRequest request = new UpdateMatrixRequest(matrixId, op);
      UpdateMatrixCache cache = new UpdateMatrixCache(splitListMap.size());
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);
      plus(requestId, request.getMatrixId(), splitListMap, null, false);
      return result;
    }
  }

  public Future<VoidResult> update(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op) {
    assert rowIds.length == rows.length;
    checkParams(matrixId, rowIds);

    /*new code*/
    LOG.info("update(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op) in UserRequestAdapter.java");
    LOG.info("rows.length = " + rows.length);
    // LOG.info("PSAgentContext.get().getPsAgent().print_PSAgent(); in update(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op) from pushGradient");
    // PSAgentContext.get().getPsAgent().print_PSAgent();
    /*code end*/

    if (useNewSplit(matrixId, rows)) {
      LOG.info("rows.length = " + rows.length); //////
      for (int i = 0; i < rows.length; i++) {
        rows[i].setRowId(rowIds[i]);
        rows[i].setMatrixId(matrixId);
        LOG.info("rowIds[" + i + "] = " + rowIds[i]);
        LOG.info("rows[" + i + "] get size = " + rows[i].getSize()); //////
        LOG.info("rows[" + i + "] get rowId = " + rows[i].getRowId()); //////
        LOG.info("rows[" + i + "] get class = " + rows[i].getClass()); //////
      }

      List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

      /* new code */
      /*
      if (currentEpoch == rmServerEpoch && rmServerPush) {
        for (PartitionKey partKey: partitions){
          if (partKey.getPartitionId() == removedParameterServerIndex){
            LOG.info("removed PartKey Id in pushGradient = " + partKey.getPartitionId());
            partitions.remove(partKey);
          }
        }
      }
      */
      // LOG.info("pushGrad.....checkpoint.........PSAgentContext.get().getPsAgent().print_PSAgent();");
      // PSAgentContext.get().getPsAgent().print_PSAgent();
      int active_partitions_size = 0;
      for (int i = 0; i < partitions.size(); i++){
        if (partitions.get(i).status) active_partitions_size++;
      }
      LOG.info("active_partitions_size = " + active_partitions_size); // push Gradients
      /* code end */


      UpdateRowsRequest request = new UpdateRowsRequest(matrixId, op);
      /* old code
      UpdateMatrixCache cache = new UpdateMatrixCache(partitions.size());
      /* new code */
      UpdateMatrixCache cache = new UpdateMatrixCache(active_partitions_size);
      /* code end */
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);

      MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
      long colNum = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getColNum();
      LOG.info("colNum = " + colNum); //////
      for (PartitionKey partKey : partitions) {
        /* old code
        RowsViewUpdateItem item = new RowsViewUpdateItem(partKey, rows, colNum);
        matrixClient.update(requestId, request.getMatrixId(), partKey, item, null, -1, false, op);
        /*new code*/
        LOG.info("update clock = -1");
        LOG.info("partKey = " + partKey.toString());
        if (partKey.status) {
          RowsViewUpdateItem item = new RowsViewUpdateItem(partKey, rows, colNum);
          matrixClient.update(requestId, request.getMatrixId(), partKey, item, null, -1, false, op);
        }else {
          LOG.info("update => removed PartKey index = " + partKey.getPartitionId());
        }
        /*code end*/
      }
      return result;
    } else {
      List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

      Map<PartitionKey, List<RowUpdateSplit>> splitListMap = new HashMap<>();
      for (int i = 0; i < rows.length; i++) {
        rows[i].setRowId(rowIds[i]);
        rows[i].setMatrixId(matrixId);
        // Split this row according the matrix partitions
        Map<PartitionKey, RowUpdateSplit> splitMap = RowUpdateSplitUtils.split(rows[i], partitions);

        // Set split context
        for (Map.Entry<PartitionKey, RowUpdateSplit> entry : splitMap.entrySet()) {
          RowUpdateSplitContext context = new RowUpdateSplitContext();
          context.setEnableFilter(false);
          context.setFilterThreshold(0);
          context.setPartKey(entry.getKey());
          entry.getValue().setSplitContext(context);

          List<RowUpdateSplit> splitList = splitListMap.get(entry.getKey());
          if (splitList == null) {
            splitList = new ArrayList<>();
            splitListMap.put(entry.getKey(), splitList);
          }
          splitList.add(entry.getValue());
        }
      }

      UpdateRowsRequest request = new UpdateRowsRequest(matrixId, op);
      UpdateMatrixCache cache = new UpdateMatrixCache(splitListMap.size());
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);
      plus(requestId, request.getMatrixId(), splitListMap, null, false);
      return result;
    }
  }

  /*new code */
  public Future<VoidResult> update_partial(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op, int epoch) {
    assert rowIds.length == rows.length;
    checkParams(matrixId, rowIds);

    LOG.info("update_partial in UserRequestAdapter.java in epoch = " + epoch);
    LOG.info("update_partial(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op) in UserRequestAdapter.java");
    LOG.info("rows.length = " + rows.length);

    if (useNewSplit(matrixId, rows)) {
      for (int i = 0; i < rows.length; i++) {
        rows[i].setRowId(rowIds[i]);
        rows[i].setMatrixId(matrixId);
      }

      List<PartitionKey> partitions =
              PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

      UpdateRowsRequest request = new UpdateRowsRequest(matrixId, op);
      // UpdateMatrixCache cache = new UpdateMatrixCache(partitions.size());
      UpdateMatrixCache cache = new UpdateMatrixCache(partitions.size() - 1);
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);

      MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
      long colNum = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getColNum();
      for (PartitionKey partKey : partitions) {
        LOG.info("partKey = " + partKey);
        if (partKey.getPartitionId() == 0) {
          LOG.info("partitionId = " + partKey.getPartitionId());
          RowsViewUpdateItem item = new RowsViewUpdateItem(partKey, rows, colNum);
          matrixClient.update(requestId, request.getMatrixId(), partKey, item, null, -1, false, op);
        }
      }
      return result;
    } else {
      List<PartitionKey> partitions =
              PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

      Map<PartitionKey, List<RowUpdateSplit>> splitListMap = new HashMap<>();
      for (int i = 0; i < rows.length; i++) {
        rows[i].setRowId(rowIds[i]);
        rows[i].setMatrixId(matrixId);
        // Split this row according the matrix partitions
        Map<PartitionKey, RowUpdateSplit> splitMap = RowUpdateSplitUtils.split(rows[i], partitions);

        // Set split context
        for (Map.Entry<PartitionKey, RowUpdateSplit> entry : splitMap.entrySet()) {
          RowUpdateSplitContext context = new RowUpdateSplitContext();
          context.setEnableFilter(false);
          context.setFilterThreshold(0);
          context.setPartKey(entry.getKey());
          entry.getValue().setSplitContext(context);

          List<RowUpdateSplit> splitList = splitListMap.get(entry.getKey());
          if (splitList == null) {
            splitList = new ArrayList<>();
            splitListMap.put(entry.getKey(), splitList);
          }
          splitList.add(entry.getValue());
        }
      }

      UpdateRowsRequest request = new UpdateRowsRequest(matrixId, op);
      UpdateMatrixCache cache = new UpdateMatrixCache(splitListMap.size());
      FutureResult<VoidResult> result = new FutureResult<>();
      int requestId = request.getRequestId();
      requestIdToSubresponsMap.put(requestId, cache);
      requestIdToResultMap.put(requestId, result);
      requests.put(requestId, request);
      plus(requestId, request.getMatrixId(), splitListMap, null, false);
      return result;
    }
  }


  public void update_none(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op, int epoch) {
    assert rowIds.length == rows.length;
    LOG.info("update_none in UserRequestAdapter.java in epoch = " + epoch);
    LOG.info("update_none(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op) in UserRequestAdapter.java");
    LOG.info("rows.length = " + rows.length);
  }
  /* code end*/

  /**
   * Row splits merge thread.
   */
  class RowMerger extends Thread {
    private final GetRowRequest request;
    private final PartitionResponseCache cache;
    private FutureResult result;

    public RowMerger(GetRowRequest request, PartitionResponseCache cache, FutureResult result) {
      this.request = request;
      this.cache = cache;
      this.result = result;
    }

    private void mergeRowPipeline(GetRowPipelineCache pipelineCache) {
      try {
        Vector vector = RowSplitCombineUtils
          .combineRowSplitsPipeline(pipelineCache, request.getMatrixId(), request.getRowIndex());
        vector.setMatrixId(request.getMatrixId());
        result.set(vector);
      } catch (Exception x) {
        LOG.fatal("merge row failed ", x);
        PSAgentContext.get().getPsAgent().error("merge row splits failed " + x.getMessage());
      }
    }

    @Override public void run() {
      if (cache instanceof GetRowPipelineCache) {
        mergeRowPipeline((GetRowPipelineCache) cache);
      }
    }
  }


  /**
   * Row splits merge thread.
   */
  class IndexRowMerger extends Thread {
    private final IndexGetRowRequest request;
    private final PartitionResponseCache cache;
    private FutureResult result;

    public IndexRowMerger(IndexGetRowRequest request, PartitionResponseCache cache,
      FutureResult result) {
      this.request = request;
      this.cache = cache;
      this.result = result;
    }

    private void mergeIndexRow(IndexGetRowCache cache) {
      try {
        //long startTs = System.currentTimeMillis();
        Vector vector = RowSplitCombineUtils.combineIndexRowSplits(request, cache);
        //LOG.error("combine use time = " + (System.currentTimeMillis() - startTs));
        vector.setMatrixId(request.getMatrixId());
        result.set(vector);
      } catch (Exception x) {
        LOG.fatal("merge row failed ", x);
        PSAgentContext.get().getPsAgent().error("merge row splits failed " + x.getMessage());
      }
    }

    @Override public void run() {
      if (cache instanceof IndexGetRowCache) {
        mergeIndexRow((IndexGetRowCache) cache);
      }
    }
  }


  /**
   * Row splits merge thread.
   */
  class IndexRowsMerger extends Thread {
    private final IndexGetRowsRequest request;
    private final PartitionResponseCache cache;
    private FutureResult result;

    public IndexRowsMerger(IndexGetRowsRequest request, PartitionResponseCache cache,
      FutureResult result) {
      this.request = request;
      this.cache = cache;
      this.result = result;
    }

    private void mergeIndexRow(IndexGetRowsCache cache) {
      try {
        LOG.info("subResponse size = " + cache.getSubResponses().size()); //////
        Vector[] vectors = RowSplitCombineUtils.combineIndexRowsSplits(request, cache);
        LOG.info("vectors.length = " + vectors.length); //////
        result.set(vectors);
      } catch (Exception x) {
        LOG.fatal("merge row failed ", x);
        PSAgentContext.get().getPsAgent().error("merge row splits failed " + x.getMessage());
      }
    }

    @Override public void run() {
      if (cache instanceof IndexGetRowsCache) {
        mergeIndexRow((IndexGetRowsCache) cache);
      }
    }
  }


  /**
   * Merge thread for GET_ROWS request.
   */
  public class RowsFlowMerger implements Runnable {
    private final GetRowsFlowRequest request;
    private final List<RowMergeItem> rowSplits;

    public RowsFlowMerger(GetRowsFlowRequest request, List<RowMergeItem> rowSplits) {
      this.request = request;
      this.rowSplits = rowSplits;
    }

    @Override public void run() {
      for (RowMergeItem item : rowSplits) {
        notifyAllGetRows(mergeSplit(item.getRowIndex(), item.getRowSplits()));
      }
    }

    private Vector mergeSplit(int rowIndex, List<ServerRow> splits) {
      Vector vector = null;
      try {
        vector = RowSplitCombineUtils
          .combineServerRowSplits(splits, request.getIndex().getMatrixId(), rowIndex);
        return vector;
      } catch (Exception x) {
        LOG.fatal("merge row failed ", x);
        PSAgentContext.get().getPsAgent().error("merge row splits failed " + x.getMessage());
      }

      return vector;
    }

    private void notifyAllGetRows(Vector row) {
      if (row == null) {
        return;
      }
      if(PSAgentContext.get().getMatrixStorageManager() != null)
        PSAgentContext.get().getMatrixStorageManager().addRow(row.getMatrixId(), row.getRowId(), row);
      ReentrantLock lock = getLock(row.getMatrixId());
      try {
        lock.lock();

        Iterator<Entry<RowIndex, GetRowsResult>> iter = resultsMap.entrySet().iterator();
        Entry<RowIndex, GetRowsResult> resultEntry = null;
        while (iter.hasNext()) {
          resultEntry = iter.next();
          if (resultEntry.getKey().getMatrixId() == row.getMatrixId() && resultEntry.getKey()
            .contains(row.getRowId()) && !resultEntry.getKey().isFilted(row.getRowId())) {
            resultEntry.getKey().filted(row.getRowId());
            resultEntry.getValue().put(row);
          }

          if (resultEntry.getKey().getRowsNumber() == resultEntry.getValue().getRowsNumber()) {
            resultEntry.getKey().clearFilted();
            resultEntry.getValue().fetchOver();
            iter.remove();
          }
        }

        IntOpenHashSet fetchingRowsForMatrix = fetchingRowSets.get(row.getMatrixId());
        if (fetchingRowsForMatrix != null) {
          fetchingRowsForMatrix.remove(row.getRowId());
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted when notify getrowrequest, exit now ", e);
      } finally {
        lock.unlock();
      }
    }
  }


  private ReentrantLock getLock(int matrixId) {
    if (!locks.containsKey(matrixId)) {
      locks.putIfAbsent(matrixId, new ReentrantLock());
    }
    return locks.get(matrixId);
  }

  public VoidResult mergeFlushResult(List<VoidResult> resultList) {
    return new VoidResult(ResponseType.SUCCESS);
  }

  public VoidResult mergeUpdaterResult(List<VoidResult> resultList) {
    return new VoidResult(ResponseType.SUCCESS);
  }

  /**
   * Split the rowIndex to batches to generate rpc dispatcher items
   *
   * @param rowIndex rowIds needed to been requested from PS
   */
  private void dispatchGetRows(RowIndex rowIndex, int rpcBatchSize, int clock) {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();

    // Get the partition to sub-row splits map:use to storage the rows stored in a matrix partition
    Map<PartitionKey, List<RowIndex>> partToRowIndexMap =
      PSAgentContext.get().getMatrixMetaManager().getPartitionToRowIndexMap(rowIndex, rpcBatchSize);
    List<RowIndex> rowIds;
    int size;

    // Generate dispatch items and add them to the corresponding queues
    int totalRequestNumber = 0;
    for (Entry<PartitionKey, List<RowIndex>> entry : partToRowIndexMap.entrySet()) {
      totalRequestNumber += entry.getValue().size();
    }

    GetRowsFlowRequest request = new GetRowsFlowRequest(rowIndex, clock);

    // Filter the rowIds which are fetching now
    ReentrantLock lock = getLock(rowIndex.getMatrixId());
    Int2IntOpenHashMap rowIndexToPartSizeMap;
    try {
      lock.lock();
      rowIndexToPartSizeMap = matrixToRowSplitSizeCache.get(rowIndex.getMatrixId());
    } finally {
      lock.unlock();
    }

    GetRowsFlowCache cache =
      new GetRowsFlowCache(totalRequestNumber, rowIndex.getMatrixId(), rowIndexToPartSizeMap);

    int requestId = request.getRequestId();
    requests.put(requestId, request);
    requestIdToSubresponsMap.put(requestId, cache);
    requestIdToResultMap.put(requestId, new FutureResult());

    for (Entry<PartitionKey, List<RowIndex>> entry : partToRowIndexMap.entrySet()) {
      totalRequestNumber += entry.getValue().size();
      rowIds = entry.getValue();
      size = rowIds.size();

      for (int i = 0; i < size; i++) {
        matrixClient.getRowsSplit(requestId, entry.getKey(), rowIndexToList(rowIds.get(i)), clock);
      }
    }
  }

  private List<Integer> rowIndexToList(RowIndex index) {
    int[] rowIndexes = index.getRowIds().toIntArray();
    List<Integer> ret = new ArrayList<Integer>();
    for (int i = 0; i < rowIndexes.length; i++) {
      ret.add(rowIndexes[i]);
    }
    return ret;
  }

  private RowIndex findNewRows(RowIndex rowIndex) {
    IntOpenHashSet need = new IntOpenHashSet();
    IntOpenHashSet fetchingRowIds = fetchingRowSets.get(rowIndex.getMatrixId());

    IntIterator iter = rowIndex.getRowIds().iterator();
    while (iter.hasNext()) {
      int rowId = iter.nextInt();
      if (!fetchingRowIds.contains(rowId)) {
        need.add(rowId);
        fetchingRowIds.add(rowId);
      }
    }

    return new RowIndex(rowIndex.getMatrixId(), need, rowIndex);
  }

  private int chooseRpcBatchSize(RowIndex rowIndex) {
    PartitionKey part =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(rowIndex.getMatrixId()).get(0);
    int rowNumInPart = part.getEndRow() - part.getStartRow();
    return Math.max(rowNumInPart / 4, 10);
  }

  public static Map<PartitionKey, IndicesView> split(List<PartitionKey> partKeys, int[] indexes) {
    // Sort the parts by partitionId
    Arrays.sort(indexes);

    HashMap<PartitionKey, IndicesView> ret = new HashMap<>();

    // Sort partition keys use start column index
    //Collections.sort(partKeys, (PartitionKey key1, PartitionKey key2) -> {
    //  return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
    //});

    /* old code
    int ii = 0;
    int keyIndex = 0;
    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indexes.length || keyIndex < partKeys.size()) {
      int length = 0;
      long endOffset = partKeys.get(keyIndex).getEndCol();
      while (ii < indexes.length && indexes[ii] < endOffset) {
        ii++;
        length++;
      }

      if (length != 0) {
        ret.put(partKeys.get(keyIndex), new IntIndicesView(indexes, ii - length, ii));
      }
      keyIndex++;
    }
    /* new code */
    // Sort partition keys use start column index
    Collections.sort(partKeys, (PartitionKey key1, PartitionKey key2) -> {
      return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
    });
    for (int i = 0; i < partKeys.size(); i++){
      LOG.info("partitionKey_split = " + partKeys.get(i).toString());
    }
    int ii = 0;
    int keyIndex = 0;
    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indexes.length || keyIndex < partKeys.size()) {
      int length = 0;
      long endOffset = partKeys.get(keyIndex).getEndCol();
      long startOffset = partKeys.get(keyIndex).getStartCol();
      while (ii < indexes.length && indexes[ii] < endOffset) {
        if (indexes[ii] >= startOffset) length++;
        ii++;
      }

      if (length != 0) {
        LOG.info("ii = " + ii + ", length = " + length);
        ret.put(partKeys.get(keyIndex), new IntIndicesView(indexes, ii - length, ii));
      }
      keyIndex++;
    }

    /* code end */
    return ret;
  }

  public static Map<PartitionKey, IndicesView> split(List<PartitionKey> partKeys, long[] indexes) {
    // Sort the parts by partitionId
    Arrays.sort(indexes);

    HashMap<PartitionKey, IndicesView> ret = new HashMap<>();

    // Sort partition keys use start column index
    //Collections.sort(partKeys, (PartitionKey key1, PartitionKey key2) -> {
    //  return key1.getStartCol() < key2.getStartCol() ? -1 : 1;
    //});

    int ii = 0;
    int keyIndex = 0;
    // For each partition, we generate a update split.
    // Although the split is empty for partitions those without any update data,
    // we still need to generate a update split to update the clock info on ps.
    while (ii < indexes.length || keyIndex < partKeys.size()) {
      int length = 0;
      long endOffset = partKeys.get(keyIndex).getEndCol();
      while (ii < indexes.length && indexes[ii] < endOffset) {
        ii++;
        length++;
      }

      if (length != 0) {
        ret.put(partKeys.get(keyIndex), new LongIndicesView(indexes, ii - length, ii));
      }
      keyIndex++;
    }
    return ret;
  }
}
