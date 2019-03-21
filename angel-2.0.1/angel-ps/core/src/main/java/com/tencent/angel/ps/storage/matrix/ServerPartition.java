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


package com.tencent.angel.ps.storage.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.model.output.format.IntFloatElement;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Server partition represents a partition of matrix, which hold and manager the related matrix rows.
 */
public class ServerPartition implements Serialize {
  private final static Log LOG = LogFactory.getLog(ServerPartition.class);

  /**
   * Row index to server row map
   */
  private  PartitionSource rows;

  private String PartitionSourceClassName;

  /**
   * Partition key
   */
  private PartitionKey partitionKey;

  /**
   * Server Matrix row type
   */
  private RowType rowType;

  /**
   * Estimate sparsity for sparse model type
   */
  private final double estSparsity;

  private volatile int clock;

  private volatile PartitionState state;

  private final AtomicInteger updateCounter = new AtomicInteger(0);

  /* new code */
  // rowIndex to loadpath
  public Map<Integer, String> loadPath = new HashMap<Integer, String>();
  /* code end */

  /**
   * Create a new Server partition,include load rows.
   *
   * @param partitionKey the partition meta
   * @param rowType      the row type
   */
  public ServerPartition(PartitionKey partitionKey, RowType rowType, double estSparsity,
    String sourceClass) {
    this.state = PartitionState.INITIALIZING;
    this.partitionKey = partitionKey;
    this.rowType = rowType;
    this.clock = 0;
    this.estSparsity = estSparsity;

    PartitionSource source;
    try {
      source = (PartitionSource) Class.forName(sourceClass).newInstance();
    } catch (Throwable e) {
      LOG.error("Can not init partition source for type " + sourceClass + " use default instead ",
        e);
      source = new PartitionSourceMap();
    }
    source.init(partitionKey.getEndRow() - partitionKey.getStartRow());
    this.rows = source;
  }

  /**
   * Create a new Server partition,include load rows.
   *
   * @param partitionKey the partition meta
   * @param rowType      the row type
   */
  public ServerPartition(PartitionKey partitionKey, RowType rowType, double estSparsity) {
    this(partitionKey, rowType, estSparsity, AngelConf.DEFAULT_ANGEL_PS_PARTITION_SOURCE_CLASS);
  }

  /**
   * Create a new Server partition.
   */
  public ServerPartition() {
    this(null, RowType.T_DOUBLE_DENSE, 1.0);
  }

  /* new code */

  public void load_values(PSContext context) {
    for (Map.Entry<Integer, String> entry : loadPath.entrySet()) {
      LOG.info("rowIndex = " + entry.getKey());
      LOG.info("loadPath = " + entry.getValue());
      ServerIntFloatRow row = (ServerIntFloatRow) getRow(entry.getKey());
      FileSystem fs = null;
      Path loadFilePath = new Path(entry.getValue());
      try {
        fs = loadFilePath.getFileSystem(context.getConf());
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        FSDataInputStream input = fs.open(loadFilePath);
        for (int j = 0; j < row.size(); j++) {
          String line = input.readLine();
          String[] kv = line.split(",");
          float value = Float.valueOf(kv[2]);
          row.set(j, value);
        }
        input.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

      try {
        fs.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void print_ServerPartition() {
    LOG.info("print_ServerPartition");
    for (int i = 0; i < rows.rowNum(); i++){
      LOG.info("rowindex = " + i + ", class = " + rows.getRow(i).getClass() + ", toString = " + rows.getRow(i).toString());
    }
    int colEndIndex = (int) Math.min(10, (partitionKey.getEndCol() - partitionKey.getStartCol()));
    for (int i = 0; i < rows.rowNum(); i++){
      LOG.info("rowIndex = " + i);
      print_values(i, colEndIndex);
    }
  }

  // only apply to Logistic Regression with limited classes
  public void print_values(int rowIndex, int colEndIndex){
    ServerIntFloatRow row = (ServerIntFloatRow) getRow(rowIndex);
    IntFloatVector vector = (IntFloatVector) row.getSplit();
    int baseCol = (int) partitionKey.getStartCol();
    if (vector.isDense()){
      float[] data = vector.getStorage().getValues();
      IntFloatElement element = new IntFloatElement();
      for (int j = 0; j < colEndIndex; j++) {
        element.colId = baseCol + j;
        element.value = data[j];
        LOG.info("colId = " + String.valueOf(element.colId) + " => value = " + String.valueOf(element.value) + "\n");
      }
    }
  }
  /* code end */


  /**
   * Init partition
   */
  public void init() {
    LOG.info("      init() ServerPartition.java, PartitionId = " + partitionKey.getPartitionId()); //////
    if (partitionKey != null) {
      initRows(partitionKey, rowType, estSparsity);
    }
  }

  private void initRows(PartitionKey partitionKey, RowType rowType, double estSparsity) {
    LOG.info("      initRows in ServerPartition.java"); //////
    int rowStart = partitionKey.getStartRow();
    int rowEnd = partitionKey.getEndRow();
    for (int rowIndex = rowStart; rowIndex < rowEnd; rowIndex++) {
      LOG.info("           rows.putRow"); //////
      rows.putRow(rowIndex,
        initRow(rowIndex, rowType, partitionKey.getStartCol(), partitionKey.getEndCol(),
          estSparsity));
    }
  }

  private ServerRow initRow(int rowIndex, RowType rowType, long startCol, long endCol,
    double estSparsity) {
    LOG.info("           initRow in ServerPartition.java, RowIndex = " + rowIndex + ", RowType = " + rowType + ", startCol = " + startCol + ", endCol = " + endCol); //////
    LOG.info("estSparsity = " + estSparsity); //////
    int estEleNum = (int) ((endCol - startCol) * estSparsity);
    LOG.info("           estEleNum = " + estEleNum); //////
    return ServerRowFactory.createServerRow(rowIndex, rowType, startCol, endCol, estEleNum);
  }

  private ServerRow initRow(RowType rowType) {
    return initRow(0, rowType, 0, 0, 0.0);
  }

  /**
   * Direct plus to rows in this partition
   *
   * @param buf serialized update vector
   */
  public void update(ByteBuf buf, UpdateOp op) {
    LOG.info("startUpdate()");//////
    startUpdate();
    try {
      int rowNum = buf.readInt();
      int rowId;
      RowType rowType;
      LOG.info("total rowNum = " + rowNum); //////
      for (int i = 0; i < rowNum; i++) {
        LOG.info("i = " + i); //////
        rowId = buf.readInt();
        LOG.info("rowId = " + rowId); //////
        rowType = RowType.valueOf(buf.readInt());
        LOG.info("rowType = " + rowType.toString()); ////// T_FLOAT_SPARSE
        ServerRow row = getRow(rowId);
        LOG.info("row.update(rowType, buf, op);"); //////
        row.update(rowType, buf, op);
      }
    } finally {
      endUpdate();
    }
  }

  /** new code
   * Direct plus to rows in this partition with more information
   *
   * @param buf serialized update vector
   */
  public void update_with_more(ByteBuf buf, UpdateOp op, String[] infos) {
    LOG.info("startUpdate()");//////


    startUpdate();
    try {
      int rowNum = buf.readInt();
      int rowId;
      RowType rowType;

      LOG.info("put update request=" + infos[0]);//////
      LOG.info(infos[1]);//////
      LOG.info("op = " + infos[2]);//////
      LOG.info("rowNum = " + rowNum);

      for (int i = 0; i < rowNum; i++) {
        LOG.info("i = " + i);
        rowId = buf.readInt();
        rowType = RowType.valueOf(buf.readInt());
        LOG.info("rowType = " + rowType);
        ServerRow row = getRow(rowId);
        LOG.info("rowId = " + rowId);
        LOG.info("row.update(rowType, buf, op);");
        LOG.info("row.class = " + row.getClass());
        row.update(rowType, buf, op);
      }
    } finally {
      endUpdate();
    }
  }
  /* code end */

  /**
   * Update the partition use psf
   *
   * @param func      PS update function
   * @param partParam the parameters for the PSF
   */
  public void update(UpdateFunc func, PartitionUpdateParam partParam) {
    startUpdate();
    try {
      func.partitionUpdate(partParam);
    } finally {
      endUpdate();
    }
  }

  /**
   * Replace the row in the partition
   *
   * @param rowSplit new row
   */
  public void update(ServerRow rowSplit) {
    ServerRow oldRowSplit = rows.getRow(rowSplit.getRowId());
    if (oldRowSplit == null || rowSplit.getClock() > oldRowSplit.getClock()
      || rowSplit.getRowVersion() > oldRowSplit.getRowVersion()) {
      rows.putRow(rowSplit.getRowId(), rowSplit);
    }
  }

  public void update(List<ServerRow> rowsSplit) {
    int size = rowsSplit.size();
    for (int i = 0; i < size; i++) {
      update(rowsSplit.get(i));
    }
  }

  /**
   * Gets specified row by row index.
   *
   * @param rowIndex the row index
   * @return the row
   */
  public ServerRow getRow(int rowIndex) {
    return rows.getRow(rowIndex);
  }

  /**
   * Get a batch rows
   *
   * @param rowIndexes row indices
   * @return
   */
  public List<ServerRow> getRows(List<Integer> rowIndexes) {
    if (rowIndexes == null || rowIndexes.isEmpty()) {
      return new ArrayList<>();
    }

    int size = rowIndexes.size();
    List<ServerRow> rows = new ArrayList<ServerRow>();
    for (int i = 0; i < size; i++) {
      rows.add(this.rows.getRow(i));
    }

    return rows;
  }

  public PartitionSource getRows() {
    return rows;
  }

  /**
   * Gets related partition key.
   *
   * @return the partition key
   */
  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  /**
   * Reset rows.
   */
  public void reset() {
    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      row.reset();
    }
  }


  // TODO: dynamic add/delete row
  @Override public void serialize(ByteBuf buf) {
    if (partitionKey == null) {
      return;
    }

    byte[] bytes = this.PartitionSourceClassName.getBytes();
    buf.writeInt(bytes.length);
    buf.writeBytes(bytes);

    partitionKey.serialize(buf);
    buf.writeInt(rowType.getNumber());
    buf.writeInt(rows.rowNum());

    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      buf.writeInt(row.getRowType().getNumber());
      row.serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    int size = buf.readInt();
    byte[] bytes = new byte[size];
    buf.readBytes(bytes);
    this.PartitionSourceClassName = new String(bytes);
    try {
      rows = (PartitionSource) Class.forName(this.PartitionSourceClassName).newInstance();
    } catch (Throwable e) {
      LOG.error("Can not init partition source for type " + this.PartitionSourceClassName + " use default instead ",
          e);
      rows = new PartitionSourceMap();
    }
    partitionKey = new PartitionKey();
    partitionKey.deserialize(buf);
    rowType = RowType.valueOf(buf.readInt());
    int rowNum = buf.readInt();
    RowType rowType;
    for (int i = 0; i < rowNum; i++) {
      rowType = RowType.valueOf(buf.readInt());
      ServerRow row = initRow(rowType);
      row.deserialize(buf);
      rows.putRow(row.getRowId(), row);
    }
  }

  @Override public int bufferLen() {
    if (partitionKey == null) {
      return 0;
    }

    int len = partitionKey.bufferLen() + 8;

    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      len += row.bufferLen();
    }

    return len;
  }

  public int elementNum() {
    int num = 0;

    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      num = row.size();
    }

    return num;
  }

  public int getClock() {
    return clock;
  }

  public void setClock(int clock) {
    this.clock = clock;
  }

  public RowType getRowType() {
    return rowType;
  }

  public void waitAndSetReadOnly() throws InterruptedException {
    setState(PartitionState.READ_ONLY);
    while (true) {
      if (updateCounter.get() == 0) {
        return;
      } else {
        Thread.sleep(10);
      }
    }
  }

  public void setState(PartitionState state) {
    this.state = state;
  }

  public PartitionState getState() {
    return state;
  }

  public void startUpdate() {
    updateCounter.incrementAndGet();
  }

  public void endUpdate() {
    updateCounter.decrementAndGet();
  }

  public void recover(ServerPartition part) {
    startUpdate();
    try {
      Iterator<Map.Entry<Integer, ServerRow>> iter = part.rows.iterator();
      while (iter.hasNext()) {
        Map.Entry<Integer, ServerRow> entry = iter.next();
        rows.putRow(entry.getKey(), entry.getValue());
      }
      setState(PartitionState.READ_AND_WRITE);
    } finally {
      endUpdate();
    }
  }
}
