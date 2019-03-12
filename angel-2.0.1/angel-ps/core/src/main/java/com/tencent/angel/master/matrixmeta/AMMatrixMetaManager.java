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


package com.tencent.angel.master.matrixmeta;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.ml.matrix.*;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.MatrixFilesMeta;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.ha.RecoverPartKey;
import com.tencent.angel.ps.server.data.PSLocation;
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.partitioner.Partitioner;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Matrix meta manager in angel application master. it contains all matrices meta and partitions for each parameter server hold
 */
public class AMMatrixMetaManager {
  private static final Log LOG = LogFactory.getLog(AMMatrixMetaManager.class);
  private final AMContext context;

  /**
   * Matrix meta manager
   */
  private final MatrixMetaManager matrixMetaManager;

  /**
   * inverted index, psId--->Map( matrixId---->List<PartitionMeta>), used for PS
   */
  private final Map<ParameterServerId, Map<Integer, MatrixMeta>> matrixPartitionsOnPS;

  /**
   * ps id to matrices on this ps map
   */
  private final Map<ParameterServerId, Set<Integer>> psIdToMatrixIdsMap;

  /**
   * matrix id to psId which has build partitions of this matrix map, use to add matrix
   */
  private final Map<Integer, Set<ParameterServerId>> matrixIdToPSSetMap;

  private final Map<ParameterServerId, Set<RecoverPartKey>> psIdToRecoverPartsMap;

  /**
   * matrix id generator
   */
  private int maxMatrixId = 0;

  private final Lock readLock;
  private final Lock writeLock;

  /* new code */
  public int cur_serverNum = 0;
  public List<MatrixContext> matrixContexts;

  public int rmParameterServerIndex = -1;
  public int rmServerEpoch = -1;
  /* code end */

  public AMMatrixMetaManager(AMContext context) {
    this.context = context;
    matrixMetaManager = new MatrixMetaManager();
    matrixPartitionsOnPS = new HashMap<>();
    matrixIdToPSSetMap = new HashMap<>();
    psIdToMatrixIdsMap = new HashMap<>();
    psIdToRecoverPartsMap = new ConcurrentHashMap<>();

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();

    /* new code */
    cur_serverNum = context.getConf().getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER) - 1;
    matrixContexts = new ArrayList<MatrixContext>();

    rmParameterServerIndex = context.getConf().getInt(AngelConf.ANGEL_WORKER_RM_SERVER_ID, AngelConf.DEFAULT_ANGEL_WORKER_RM_SERVER_ID);
    rmServerEpoch = context.getConf().getInt(AngelConf.ANGEL_WORKER_RM_SERVER_EPOCH, AngelConf.DEFAULT_ANGEL_WORKER_RM_SERVER_EPOCH);
    /* code end */
    // Add one sync matrix
    // addSyncMatrix();
  }


  /**
   * Get matrix meta use matrix name
   *
   * @param matrixName matrix name
   * @return MatrixMeta matrix meta proto of the matrix, if not found, just return null
   */
  public MatrixMeta getMatrix(String matrixName) {
    return matrixMetaManager.getMatrixMeta(matrixName);
  }

  /**
   * Get matrix meta use matrix id
   *
   * @param matrixId matrix id
   * @return MatrixMeta matrix meta proto of the matrix
   */
  public MatrixMeta getMatrix(int matrixId) {
    return matrixMetaManager.getMatrixMeta(matrixId);
  }

  /**
   * get partitions of a specific parameter server hold
   *
   * @param psId, parameter server id
   * @return List<MatrixPartition> the partitions of the parameter server hold
   */
  public final Map<Integer, MatrixMeta> getMatrixPartitions(ParameterServerId psId) {
    try {
      readLock.lock();
      Map<Integer, MatrixMeta> metaInPS = matrixPartitionsOnPS.get(psId);
      if (metaInPS == null) {
        return new HashMap<>();
      } else {
        return new HashMap<>(metaInPS);
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Create matrices
   *
   * @param matrixContexts matrices meta
   * @throws InvalidParameterException
   */
  public void createMatrices(List<MatrixContext> matrixContexts) throws Exception {
    /* old code
    int size = matrixContexts.size();
    for (int i = 0; i < size; i++) {
      LOG.info("matrixContext[" + i + "]_ToString = " + matrixContexts.get(i).toString());
      createMatrix(matrixContexts.get(i));
    }
    /* new code */
    this.matrixContexts = new ArrayList<>(matrixContexts);
    int size = this.matrixContexts.size();
    LOG.info("matrixContexts.size() = " + size);
    for (int i = 0; i < size; i++) {
      LOG.info("matrixContext[" + i + "]_ToString = " + this.matrixContexts.get(i).toString());
      createMatrix(this.matrixContexts.get(i));
    }
    /* code end */
  }

  /**
   * Create a new matrix
   *
   * @param matrixContext matrix context
   * @throws InvalidParameterException
   */
  public void createMatrix(MatrixContext matrixContext) throws Exception {
    // Check whether the matrix name conflicts with the existing matrix names, the matrix name must be only
    if (matrixMetaManager.exists(matrixContext.getName())) {
      String errorMsg = "build matrix failed. matrix name " + matrixContext.getName()
        + " has exist, you must choose a new one";
      LOG.error(errorMsg);
      throw new InvalidParameterException(errorMsg);
    }

    LOG.info("before init matrix "); //////

    MatrixMeta meta = initMatrixMeta(matrixContext);

    LOG.debug("after init matrix " + meta);

    LOG.info("after init matrix " + meta.toString()); //////

    matrixMetaManager.addMatrix(meta);

    try {
      writeLock.lock();
      buildPSMatrixMeta(meta);
    } finally {
      writeLock.unlock();
    }
  }

  private Partitioner initPartitioner(MatrixContext matrixContext, Configuration conf)
    throws IllegalAccessException, InvocationTargetException, InstantiationException,
    NoSuchMethodException {
    Class<? extends Partitioner> partitionerClass = matrixContext.getPartitionerClass();
    Constructor<? extends Partitioner> constructor = partitionerClass.getConstructor();
    constructor.setAccessible(true);
    Partitioner partitioner = constructor.newInstance();
    partitioner.init(matrixContext, conf);
    return partitioner;
  }

  private MatrixMeta initMatrixMeta(MatrixContext matrixContext)
    throws InvocationTargetException, NoSuchMethodException, InstantiationException,
    IllegalAccessException, IOException {

    try {
      writeLock.lock();
      matrixContext.setMatrixId(maxMatrixId++);
    } finally {
      writeLock.unlock();
    }

    String loadPath = matrixContext.getAttributes().get(MatrixConf.MATRIX_LOAD_PATH);
    Partitioner partitioner = initPartitioner(matrixContext, context.getConf());

    List<PartitionMeta> partitions;
    if (loadPath != null) {
      partitions = loadPartitionInfoFromHDFS(loadPath, matrixContext, context.getConf());
    } else {
      partitions = partitioner.getPartitions();
    }

    assignPSForPartitions(partitioner, partitions);
    assignReplicationSlaves(partitions);

    int size = partitions.size();
    Map<Integer, PartitionMeta> partIdToMetaMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      partIdToMetaMap.put(partitions.get(i).getPartId(), partitions.get(i));
    }
    MatrixMeta meta = new MatrixMeta(matrixContext, partIdToMetaMap);
    /* new code */
    meta.PartitionIdStart = partIdToMetaMap.size();
    LOG.info("meta toString = " + meta.toString());
    /* code end */
    return meta;
  }


  /* new code */
  public void rmOneParameterServer() throws NoSuchMethodException, InstantiationException, IllegalAccessException, IOException, InvocationTargetException {
    print_Meta();
    Map<Integer, PartitionMeta> matrixId2PartMeta = new HashMap<Integer, PartitionMeta>();

    // find ParameterServerId that corresponds to rmParameterServerIndex
    ParameterServerId rmParameterServerId = null;
    for (ParameterServerId psId : psIdToMatrixIdsMap.keySet()){
      if (psId.getIndex() == rmParameterServerIndex) rmParameterServerId = psId;
    }
    if (rmParameterServerId == null) return;

    // re-build related data structures (remove) : psIdToMatrixIdsMap and matrixIdToPSSetMap
    Integer[] matrixIds_rm = new Integer[psIdToMatrixIdsMap.get(rmParameterServerId).size()];
    psIdToMatrixIdsMap.get(rmParameterServerId).toArray(matrixIds_rm);
    for (int i = 0; i< matrixIds_rm.length; i++){
      int matrixId = matrixIds_rm[i];
      matrixIdToPSSetMap.get(matrixId).remove(rmParameterServerId);
    }
    psIdToMatrixIdsMap.remove(rmParameterServerId);

    // re-build related data structures (remove): matrixPartitionsOnPS and matrixMetaManager
    Map<Integer, MatrixMeta> MatrixId2Meta = matrixPartitionsOnPS.get(rmParameterServerId);
    for (Map.Entry<Integer, MatrixMeta> entry: MatrixId2Meta.entrySet()){
      int matrixId = entry.getKey();
      MatrixMeta matrixMeta = entry.getValue();
      Map<Integer, PartitionMeta> partitionMetas = matrixMeta.getPartitionMetas();
      for (Map.Entry<Integer, PartitionMeta> entry2: partitionMetas.entrySet()){
        LOG.info("removed PartitionIndex = " + entry2.getKey());
        LOG.info("removed PartitionMeta = " + entry2.getValue().toString());
        PartitionMeta partitionMeta = matrixMetaManager.getMatrixMeta(matrixId).getPartitionMeta(entry2.getKey());
        matrixMetaManager.getMatrixMeta(matrixId).getPartitionMetas().remove(entry2.getKey());
        matrixId2PartMeta.put(matrixId, partitionMeta);
      }
    }
    matrixPartitionsOnPS.remove(rmParameterServerId);
    print_Meta();

    // re-assign
    reAssignPartitionMetas(matrixId2PartMeta);
  }

  public void reAssignPartitionMetas(Map<Integer, PartitionMeta> matrixId2PartMeta) throws NoSuchMethodException, InstantiationException, IllegalAccessException, IOException, InvocationTargetException {
    for (Map.Entry<Integer, PartitionMeta> entry : matrixId2PartMeta.entrySet()){
      int matrixIdwithIdle = entry.getKey();
      List<PartitionMeta> partitions = initIdlePartitionMeta(matrixIdwithIdle, entry.getValue());
      for (int i = 0; i < partitions.size(); i++){
        int matrixId = matrixIdwithIdle;
        List<ParameterServerId> parameterServerIdList = partitions.get(i).getPss();
        for (int j = 0; j < parameterServerIdList.size(); j++){
          ParameterServerId psId = parameterServerIdList.get(j);
          if (!psIdToMatrixIdsMap.get(psId).contains(matrixId)) psIdToMatrixIdsMap.get(psId).add(matrixId);
          if (!matrixIdToPSSetMap.get(matrixId).contains(psId)) matrixIdToPSSetMap.get(matrixId).add(psId);
          MatrixMeta psMatrixMeta = matrixPartitionsOnPS.get(psId).get(matrixId);
          if (psMatrixMeta == null) {
            psMatrixMeta = new MatrixMeta(matrixContexts.get(matrixId));
            matrixPartitionsOnPS.get(psId).put(matrixId, psMatrixMeta);
          }
          psMatrixMeta.addPartitionMeta(partitions.get(i).getPartId(), partitions.get(i));
        }
      }
    }
    print_Meta();
  }


  public void print_Meta(){
    LOG.info("");
    print_MatrixMetaManager();
    LOG.info("");
    LOG.info("");
    print_matrixIdToPSSetMap();
    LOG.info("");
    LOG.info("");
    print_matrixPartitionsOnPS();
    LOG.info("");
    LOG.info("");
    print_psIdToMatrixIdsMap();
    LOG.info("");
    LOG.info("");
    print_psIdToRecoverPartsMap();
    LOG.info("");
    LOG.info("");
  }


  public void print_psIdToRecoverPartsMap(){
    LOG.info("print_psIdToRecoverPartsMap");
    for (Map.Entry<ParameterServerId, Set<RecoverPartKey>> entry: psIdToRecoverPartsMap.entrySet()){
      ParameterServerId psId = entry.getKey();
      LOG.info("parameterServerId = " + psId.toString());
      RecoverPartKey[] recoverPartKeys = new RecoverPartKey[entry.getValue().size()];
      for (int i = 0; i<recoverPartKeys.length; i++){
        LOG.info("  recoverPartKey.partKey = " + recoverPartKeys[i].partKey.toString());
        LOG.info("  recoverPartKey.psLoc = " + recoverPartKeys[i].psLoc.toString());
      }
    }
  }

  public void print_psIdToMatrixIdsMap(){
    LOG.info("print_psIdToMatrixIdsMap");
    for (Map.Entry<ParameterServerId, Set<Integer>> entry: psIdToMatrixIdsMap.entrySet()){
      ParameterServerId psId = entry.getKey();
      LOG.info("parameterServerId = " + psId.toString());
      Integer[] matrixIds = new Integer[entry.getValue().size()];
      entry.getValue().toArray(matrixIds);
      for (int i = 0; i<matrixIds.length; i++){
        LOG.info("  matrixId = " + matrixIds[i]);
      }
    }
  }

  public void print_matrixPartitionsOnPS(){
    LOG.info("print_matrixPartitionsOnPS");
    for (Map.Entry<ParameterServerId, Map<Integer, MatrixMeta>> entry: matrixPartitionsOnPS.entrySet()){
      ParameterServerId psId = entry.getKey();
      LOG.info("parameterServerId = " + psId.toString());
      for (Map.Entry<Integer, MatrixMeta> entry2: entry.getValue().entrySet()) {
        LOG.info("  matrixId = " + entry2.getKey());
        // LOG.info("    MatrixMeta_toString = " + entry2.getValue().toString());
        Map<Integer, PartitionMeta> partitionMetas = entry2.getValue().getPartitionMetas();
        for (Map.Entry<Integer, PartitionMeta> entry3 : partitionMetas.entrySet()) {
          LOG.info("    partitionId = " + entry3.getKey());
          LOG.info("    PartitionMeta = " + entry3.getValue());
          /*
          List<ParameterServerId> storedPs = entry3.getValue().getPss();
          for (int i = 0; i < storedPs.size(); i++) {
            LOG.info("        storedPSId[" + i + "] = " + storedPs.get(i).toString());
          }
          */
        }
      }
    }
  }

  public void print_matrixIdToPSSetMap(){
    LOG.info("print_matrixIdToPSSetMap");
    for (Map.Entry<Integer, Set<ParameterServerId>> entry : matrixIdToPSSetMap.entrySet()){
      LOG.info("matrixId = " + entry.getKey());
      ParameterServerId[] psIdSet = new ParameterServerId[entry.getValue().size()];
      entry.getValue().toArray(psIdSet);
      for (int i = 0; i < psIdSet.length; i++){
        LOG.info("  ParameterServerId = " + psIdSet[i].toString());
      }
    }
  }

  public void print_MatrixMetaManager(){
    LOG.info("print_MatrixMetaManager");
    // LOG.info("MatrixMetaManager_toString = " + matrixMetaManager.toString());
    Map<Integer, MatrixMeta> matrixIdToMetaMap = matrixMetaManager.getMatrixMetas();
    for (Map.Entry<Integer, MatrixMeta> entry: matrixIdToMetaMap.entrySet()){
      LOG.info("matrixId = " + entry.getKey());
      // LOG.info("  MatrixMeta_toString = " + entry.getValue().toString());
      Map<Integer, PartitionMeta> partitionMetas = entry.getValue().getPartitionMetas();
      for (Map.Entry<Integer, PartitionMeta> entry2 : partitionMetas.entrySet()){
        LOG.info("  partitionId = " + entry2.getKey());
        LOG.info("  PartitionMeta = " + entry2.getValue());
        /*
        List<ParameterServerId> storedPs = entry2.getValue().getPss();
        for (int i = 0; i < storedPs.size(); i++){
          LOG.info("                  storedPSId[" + i + "] = " + storedPs.get(i).toString());
        }
        */
      }
      Map<Integer, PartitionMeta> partitionMetas_idle = entry.getValue().partitionMetas_idle;
      for (Map.Entry<Integer, PartitionMeta> entry2 : partitionMetas_idle.entrySet()){
        LOG.info("  partitionId_idle = " + entry2.getKey());
        LOG.info("  PartitionMeta_idle = " + entry2.getValue());
        /*
        List<ParameterServerId> storedPs_idle = entry2.getValue().getPss();
        for (int i = 0; i < storedPs_idle.size(); i++){
          LOG.info("                  storedPSId[" + i + "]_idle = " + storedPs_idle.get(i).toString());
        }
        */
      }
    }
  }

  private List<PartitionMeta> initIdlePartitionMeta(int matrixId, PartitionMeta idlePartitionMeta)
          throws InvocationTargetException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IOException {
    LOG.info("#####initIdlePartitionMeta#####");

    MatrixContext matrixContext = matrixContexts.get(matrixId);

    Partitioner partitioner = initPartitioner(matrixContext, context.getConf());

    List<PartitionMeta> partitions = partitioner.getPartitions_idle(cur_serverNum, idlePartitionMeta, matrixMetaManager.getMatrixMeta(matrixId).PartitionIdStart);
    matrixMetaManager.getMatrixMeta(matrixId).PartitionIdStart += partitions.size();

    assignPSForPartitions_idle(partitioner, partitions);

    int size = partitions.size();
    for (int i = 0; i < size; i++) {
      matrixMetaManager.getMatrixMeta(matrixId).partitionMetas_idle.put(partitions.get(i).getPartId(), partitions.get(i));
    }
    for (Map.Entry<Integer, PartitionMeta> entry: matrixMetaManager.getMatrixMeta(matrixId).partitionMetas_idle.entrySet()) {
      LOG.info("PartitionId = " + entry.getKey());
      LOG.info("PartitionMeta toString = " + entry.getValue().toString());
    }
    return partitions;
  }
  /* code end */

  /**
   * Load matrix proto from hdfs.
   *
   * @param path the path
   * @param conf the conf
   * @return matrix partitions
   * @throws IOException the io exception
   */
  private List<PartitionMeta> loadPartitionInfoFromHDFS(String path, MatrixContext matrixContext,
    Configuration conf) throws IOException {
    Path meteFilePath =
      new Path(new Path(path, matrixContext.getName()), ModelFilesConstent.modelMetaFileName);
    MatrixFilesMeta meta = new MatrixFilesMeta();

    FileSystem fs = meteFilePath.getFileSystem(conf);
    LOG.info("Load matrix meta for matrix " + matrixContext.getName());

    if (!fs.exists(meteFilePath)) {
      throw new IOException("matrix meta file does not exist ");
    }

    FSDataInputStream input = fs.open(meteFilePath);
    try {
      meta.read(input);
    } catch (Throwable e) {
      throw new IOException("Read meta failed ", e);
    } finally {
      input.close();
    }

    List<PartitionMeta> matrixPartitions = new ArrayList<>();
    Map<Integer, MatrixPartitionMeta> partMetas = meta.getPartMetas();

    int matrixId = 0;
    try {
      writeLock.lock();
      matrixId = maxMatrixId++;
    } finally {
      writeLock.unlock();
    }

    for (Map.Entry<Integer, MatrixPartitionMeta> partMetaEntry : partMetas.entrySet()) {
      matrixPartitions.add(
        new PartitionMeta(matrixId, partMetaEntry.getKey(), partMetaEntry.getValue().getStartRow(),
          partMetaEntry.getValue().getEndRow(), partMetaEntry.getValue().getStartCol(),
          partMetaEntry.getValue().getEndCol()));
    }
    return matrixPartitions;
  }

  private void assignPSForPartitions(Partitioner partitioner, List<PartitionMeta> partitions) {
    int partNum = partitions.size();
    LOG.info("assignPSForPartitions in AMMatrixMetaManager.java......");
    for (int i = 0; i < partNum; i++) {
      int psIndex = partitioner.assignPartToServer(partitions.get(i).getPartId());
      LOG.info("      partitionId = " + partitions.get(i).getPartId() + " => PSIndex = " + psIndex); //////
      ParameterServerId psId = new ParameterServerId(psIndex);
      partitions.get(i).addReplicationPS(psId);
      partitions.get(i).makePsToMaster(psId);
    }
  }

  /* new code */
  private void assignPSForPartitions_idle(Partitioner partitioner, List<PartitionMeta> partitions) {
    int partNum = partitions.size();
    LOG.info("assignPSForPartitions_idle in AMMatrixMetaManager.java......");
    for (int i = 0; i < partNum; i++) {
      int psIndex = partitioner.assignPartToServer_idle(i, cur_serverNum);
      LOG.info("  partitionId = " + partitions.get(i).getPartId() + " => PSIndex = " + psIndex);
      // find ParameterServerId that corresponds to rmParameterServerIndex
      ParameterServerId psId = null;
      for (ParameterServerId existing_psId : psIdToMatrixIdsMap.keySet()){
        if (existing_psId.getIndex() == rmParameterServerIndex) psId = existing_psId;
      }
      if (psId == null) return;
      partitions.get(i).addReplicationPS(psId);
      partitions.get(i).makePsToMaster(psId);
    }
  }
  /* code end */

  private void assignReplicationSlaves(List<PartitionMeta> partitions) {
    int replicationNum = context.getConf().getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
      AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);
    if (replicationNum <= 1) {
      return;
    }

    int psNum =
      context.getConf().getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    int size = partitions.size();
    for (int i = 0; i < size; i++) {
      assignReplicationSlaves(partitions.get(i), replicationNum - 1, psNum);
    }
  }

  private void assignReplicationSlaves(PartitionMeta partition, int slaveNum, int psNum) {
    int psIndex = partition.getMasterPs().getIndex();
    for (int i = 0; i < slaveNum; i++) {
      partition.addReplicationPS(new ParameterServerId((psIndex + i + 1) % psNum));
    }
  }

  /**
   * dispatch matrix partitions to parameter servers
   *
   * @param matrixMeta matrix meta proto
   */
  private void buildPSMatrixMeta(MatrixMeta matrixMeta) {
    Map<Integer, PartitionMeta> partMetas = matrixMeta.getPartitionMetas();
    int matrixId = matrixMeta.getId();
    Set<ParameterServerId> psIdSet = matrixIdToPSSetMap.get(matrixId);
    if (psIdSet == null) {
      psIdSet = new HashSet<>();
      matrixIdToPSSetMap.put(matrixId, psIdSet);
    }

    for (Entry<Integer, PartitionMeta> partEntry : partMetas.entrySet()) {
      List<ParameterServerId> psList = partEntry.getValue().getPss();
      int size = psList.size();
      for (int i = 0; i < size; i++) {
        ParameterServerId psId = psList.get(i);
        Map<Integer, MatrixMeta> psMatrixIdToMetaMap = matrixPartitionsOnPS.get(psId);
        if (psMatrixIdToMetaMap == null) {
          psMatrixIdToMetaMap = new HashMap<>();
          matrixPartitionsOnPS.put(psId, psMatrixIdToMetaMap);
        }

        MatrixMeta psMatrixMeta = psMatrixIdToMetaMap.get(matrixId);
        if (psMatrixMeta == null) {
          psMatrixMeta = new MatrixMeta(matrixMeta.getMatrixContext());
          psMatrixIdToMetaMap.put(matrixId, psMatrixMeta);
        }

        psMatrixMeta.addPartitionMeta(partEntry.getKey(),
          new PartitionMeta(partEntry.getValue().getPartitionKey(),
            new ArrayList<>(partEntry.getValue().getPss())));

        psIdSet.add(psId);
      }
    }

    //    for(Entry<ParameterServerId, Map<Integer, MatrixMeta>> psEntry : matrixPartitionsOnPS.entrySet()) {
    //      LOG.info("ps id = " + psEntry.getKey());
    //      Map<Integer, MatrixMeta> matrixIdToMetaMap = psEntry.getValue();
    //      for(Entry<Integer, MatrixMeta> metaEntry : matrixIdToMetaMap.entrySet()) {
    //        LOG.info("matrix id = " + metaEntry.getKey());
    //        LOG.info("matrix partitons number = " + metaEntry.getValue().getPartitionMetas().size());
    //      }
    //    }
  }

  private void updateMaxMatrixId(int id) {
    if (maxMatrixId < id) {
      maxMatrixId = id;
    }
    LOG.debug("update maxMatrixId  to " + maxMatrixId);
  }

  /**
   * compare the matrix meta on the master and the matrix meta on ps to find the matrix this parameter server needs to create and delete
   *
   * @param matrixReports       parameter server matrix report, include the matrix ids this parameter server hold.
   * @param needCreateMatrixes  use to return the matrix partitions this parameter server need to build
   * @param needReleaseMatrixes use to return the matrix ids this parameter server need to remove
   * @param needRecoverParts    need recover partitions
   * @param psId                parameter server id
   */
  public void syncMatrixInfos(List<MatrixReport> matrixReports, List<MatrixMeta> needCreateMatrixes,
    List<Integer> needReleaseMatrixes, List<RecoverPartKey> needRecoverParts,
    ParameterServerId psId) {

    //get matrix ids in the parameter server report
    IntOpenHashSet matrixInPS = new IntOpenHashSet();
    int size = matrixReports.size();
    for (int i = 0; i < size; i++) {
      matrixInPS.add(matrixReports.get(i).matrixId);
    }

    handleMatrixReports(psId, matrixReports);

    Set<RecoverPartKey> parts = getAndRemoveNeedRecoverParts(psId);
    if (parts != null) {
      needRecoverParts.addAll(parts);
    }

    //get the matrices parameter server need to create and delete
    getPSNeedUpdateMatrix(matrixInPS, needCreateMatrixes, needReleaseMatrixes, psId);
    psMatricesUpdate(psId, matrixReports);
  }

  private void handleMatrixReports(ParameterServerId psId, List<MatrixReport> matrixReports) {
    int size = matrixReports.size();
    for (int i = 0; i < size; i++) {
      handleMatrixReport(psId, matrixReports.get(i));
    }
  }

  private void handleMatrixReport(ParameterServerId psId, MatrixReport matrixReport) {
    int size = matrixReport.partReports.size();
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        handlePartReport(psId, matrixReport.matrixId, matrixReport.partReports.get(i));
      }
    }
  }

  private void handlePartReport(ParameterServerId psId, int matrixId, PartReport partReport) {
    ParameterServerId master = matrixMetaManager.getMasterPs(matrixId, partReport.partId);
    if (!psId.equals(master)) {
      MatrixMeta matrixMeta = matrixMetaManager.getMatrixMeta(matrixId);
      if (matrixMeta == null) {
        return;
      }
      matrixMeta.getPartitionMeta(partReport.partId).addReplicationPS(psId);
      if (partReport.state == PartitionState.INITIALIZING) {
        addNeedRecoverPart(master, new RecoverPartKey(new PartitionKey(matrixId, partReport.partId),
          new PSLocation(psId, context.getLocationManager().getPsLocation(psId))));
      } else if (partReport.state == PartitionState.READ_AND_WRITE) {
        ParameterServerId orignalMaster =
          matrixPartitionsOnPS.get(psId).get(matrixId).getPartitionMeta(partReport.partId)
            .getMasterPs();
        if (orignalMaster.equals(psId)) {
          matrixMetaManager.getMatrixMeta(matrixId).getPartitionMeta(partReport.partId)
            .makePsToMaster(psId);
        }
      }
    }
  }

  private void addNeedRecoverPart(ParameterServerId master, RecoverPartKey needRecoverPart) {
    try {
      writeLock.lock();
      Set<RecoverPartKey> needRecoverParts = psIdToRecoverPartsMap.get(master);
      if (needRecoverParts == null) {
        needRecoverParts = new HashSet<>();
        psIdToRecoverPartsMap.put(master, needRecoverParts);
      }
      needRecoverParts.add(needRecoverPart);
    } finally {
      writeLock.unlock();
    }
  }

  public Set<RecoverPartKey> getAndRemoveNeedRecoverParts(ParameterServerId master) {
    try {
      writeLock.lock();
      return psIdToRecoverPartsMap.remove(master);
    } finally {
      writeLock.unlock();
    }
  }

  private void getPSNeedUpdateMatrix(Set<Integer> matrixIdInPSSet,
    List<MatrixMeta> needCreateMatrixes, List<Integer> needReleaseMatrixes,
    ParameterServerId psId) {
    try {
      readLock.lock();
      Map<Integer, MatrixMeta> matrixIdToPSMetaMap = matrixPartitionsOnPS.get(psId);

      if (matrixIdToPSMetaMap == null) {
        return;
      }

      //if a matrix exists on parameter server but not exist on master, we should notify the parameter server to remove this matrix
      for (int matrixId : matrixIdInPSSet) {
        LOG.debug("matrix in ps " + matrixId);
        LOG.info("matrix in ps " + matrixId); //////
        if (!matrixIdToPSMetaMap.containsKey(matrixId)) {
          LOG.debug("matrix " + matrixId + " need release");
          LOG.info("matrix " + matrixId + " need release"); //////
          needReleaseMatrixes.add(matrixId);
        }
      }

      //if a matrix exists on master but not exist on parameter server, this parameter server need build it.
      for (Entry<Integer, MatrixMeta> psMatrixEntry : matrixIdToPSMetaMap.entrySet()) {
        LOG.debug(
          "matrix in master " + psMatrixEntry.getKey() + ", " + psMatrixEntry.getValue().getName());
        /* new code */
        LOG.info(
                "matrix in master " + psMatrixEntry.getKey() + ", " + psMatrixEntry.getValue().getName());
        /* code end */
        if (!matrixIdInPSSet.contains(psMatrixEntry.getKey())) {
          LOG.info("matrix " + psMatrixEntry.getKey() + " need create"); //////
          LOG.info("matrix meta to string = " + psMatrixEntry.getValue().toString()); //////
          needCreateMatrixes.add(psMatrixEntry.getValue());
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Update the matrices on the PS
   *
   * @param psId
   * @param matrixReports
   */
  private void psMatricesUpdate(ParameterServerId psId, List<MatrixReport> matrixReports) {
    try {
      writeLock.lock();
      Set<Integer> matrixIdSet = psIdToMatrixIdsMap.get(psId);
      if (matrixIdSet == null) {
        matrixIdSet = new HashSet();
        psIdToMatrixIdsMap.put(psId, matrixIdSet);
      }

      int size = matrixReports.size();
      for (int i = 0; i < size; i++) {
        matrixIdSet.add(matrixReports.get(i).matrixId);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Release a matrix. just release matrix meta on master
   *
   * @param matrixId the matrix need release
   */
  public void releaseMatrix(int matrixId) {
    try {
      writeLock.lock();
      matrixMetaManager.removeMatrix(matrixId);
      matrixIdToPSSetMap.remove(matrixId);

      for (Map<Integer, MatrixMeta> psMatrixMap : matrixPartitionsOnPS.values()) {
        psMatrixMap.remove(matrixId);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Release a matrix. just release matrix meta on master
   *
   * @param matrixName the matrix need release
   */
  public void releaseMatrix(String matrixName) {
    try {
      writeLock.lock();
      int matrixId = matrixMetaManager.getMatrixId(matrixName);
      if (matrixId < 0) {
        return;
      }

      matrixMetaManager.removeMatrix(matrixId);
      matrixIdToPSSetMap.remove(matrixId);

      for (Map<Integer, MatrixMeta> psMatrixMap : matrixPartitionsOnPS.values()) {
        psMatrixMap.remove(matrixId);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * write matrix meta protos to output stream
   *
   * @param output output stream
   * @throws IOException
   */
  public void serialize(FSDataOutputStream output) throws IOException {
    Map<Integer, MatrixMeta> matrices = matrixMetaManager.getMatrixMetas();
    for (MatrixMeta meta : matrices.values()) {
      ProtobufUtil.convertToMatrixMetaProto(meta).writeDelimitedTo(output);
    }
  }

  /**
   * read matrix meta protos from input stream
   *
   * @param input input stream
   * @throws IOException, InvalidParameterException
   */
  public void deserialize(FSDataInputStream input)
    throws IOException, InvalidParameterException, ClassNotFoundException {
    while (input.available() > 0) {
      MatrixMeta meta = ProtobufUtil.convertToMatrixMeta(ProtobufUtil.loadMatrixMetaProto(input));
      matrixMetaManager.addMatrix(meta);
      try {
        writeLock.lock();
        buildPSMatrixMeta(meta);
      } finally {
        writeLock.unlock();
      }
    }
  }

  /**
   * Get ps ids which contains the matrix
   *
   * @param matrixId matrix id
   * @return ps id set
   */
  public Set<ParameterServerId> getPsIds(int matrixId) {
    return matrixIdToPSSetMap.get(matrixId);
  }

  /**
   * Get master ps ids which contains the matrix
   *
   * @param matrixId matrix id
   * @return ps id set
   */
  public Set<ParameterServerId> getMasterPsIds(int matrixId) {
    Set<ParameterServerId> psSet = new HashSet<>();
    Map<Integer, PartitionMeta> partMetas =
      matrixMetaManager.getMatrixMeta(matrixId).getPartitionMetas();
    for (PartitionMeta partMeta : partMetas.values()) {
      psSet.add(partMeta.getMasterPs());
    }
    return psSet;
  }

  public Map<Integer, MatrixMeta> getMatrixMetas() {
    return matrixMetaManager.getMatrixMetas();
  }

  public boolean isCreated(String matrixName) {
    if (!matrixMetaManager.exists(matrixName)) {
      return false;
    }
    return isCreated(matrixMetaManager.getMatrixId(matrixName));
  }

  public boolean isCreated(int matrixId) {
    boolean inited = true;

    try {
      readLock.lock();
      if (!matrixMetaManager.exists(matrixId)) {
        return false;
      }

      Set<ParameterServerId> psIdSet = matrixIdToPSSetMap.get(matrixId);

      if (psIdSet == null || psIdSet.isEmpty()) {
        return false;
      }

      inited = true;
      for (ParameterServerId psId : psIdSet) {
        if (!psIdToMatrixIdsMap.containsKey(psId) || !psIdToMatrixIdsMap.get(psId)
          .contains(matrixId)) {
          inited = false;
          break;
        }
      }
    } finally {
      readLock.unlock();
    }

    return inited;
  }

  public List<ParameterServerId> getPss(int matrixId, int partId) {
    return matrixMetaManager.getPss(matrixId, partId);
  }

  public boolean isAllMatricesCreated() {
    boolean isCreated = true;
    try {
      readLock.lock();
      for (int matrixId : matrixMetaManager.getMatrixMetas().keySet()) {
        isCreated = isCreated && isCreated(matrixId);
        if (!isCreated) {
          break;
        }
      }
    } finally {
      readLock.unlock();
    }

    return isCreated;
  }

  public void psFailed(ParameterServerId psId) {
    matrixMetaManager.removePs(psId);
  }

  public void psRecovered(ParameterServerId psId) {
    Map<Integer, MatrixMeta> matrixIdToMetaMap = matrixPartitionsOnPS.get(psId);
    if (matrixIdToMetaMap == null) {
      return;
    }

    for (MatrixMeta meta : matrixIdToMetaMap.values()) {
      Map<Integer, PartitionMeta> partMetas = meta.getPartitionMetas();
      if (partMetas == null) {
        continue;
      }

      for (PartitionMeta partMeta : partMetas.values()) {
        matrixMetaManager.addPs(meta.getId(), partMeta.getPartId(), psId);
      }
    }
  }

  public List<Integer> getMasterPartsInPS(int matrixId, ParameterServerId psId) {
    List<Integer> needSavePartInPS = new ArrayList<>();
    MatrixMeta matrixMeta = matrixMetaManager.getMatrixMeta(matrixId);
    for (PartitionMeta partMeta : matrixMeta.getPartitionMetas().values()) {
      if (psId.equals(partMeta.getMasterPs())) {
        needSavePartInPS.add(partMeta.getPartId());
      }
    }

    return needSavePartInPS;
  }

  /**
   * Check a matrix exist or not
   *
   * @param matrixName matrix name
   * @return true means exist
   */
  public boolean exist(String matrixName) {
    return matrixMetaManager.exists(matrixName);
  }


  private void addSyncMatrix() {
    MatrixContext syncMatrix = new MatrixContext("sync_1", 1, 1);
    try {
      createMatrix(syncMatrix);
    } catch (Exception e) {
      LOG.error("Create sync matrix failed", e);
    }
  }
}
