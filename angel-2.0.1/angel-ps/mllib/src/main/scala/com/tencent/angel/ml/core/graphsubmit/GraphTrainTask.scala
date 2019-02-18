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


package com.tencent.angel.ml.core.graphsubmit

import com.tencent.angel.ml.core.TrainTask
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.core.utils.{DataParser, NetUtils}
import com.tencent.angel.worker.WorkerContext
import com.tencent.angel.worker.storage.{DataBlock, DiskDataBlock, MemoryAndDiskDataBlock, MemoryDataBlock}
import com.tencent.angel.worker.task.TaskContext
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}


class GraphTrainTask(ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG: Log = LogFactory.getLog(classOf[GraphTrainTask])

  private val valiRat = SharedConf.validateRatio
  private val posnegRatio: Double = SharedConf.posnegRatio()

  // validation data storage
  /* old code */
  // val validDataBlock: DataBlock[LabeledData] = getDataBlock("memory")
  // val posDataBlock: DataBlock[LabeledData] = taskDataBlock
  // val negDataBlock: DataBlock[LabeledData] = getDataBlock()
  /* new code */
  var validDataBlock: DataBlock[LabeledData] = getDataBlock("memory")
  var posDataBlock: DataBlock[LabeledData] = taskDataBlock
  var negDataBlock: DataBlock[LabeledData] = getDataBlock()
  /* code end */

  // data format of training data, libsvm or dummy
  override val dataParser = DataParser(SharedConf.get())
  val modelType: RowType = SharedConf.modelType
  val modelClassName: String = SharedConf.modelClassName

  override def train(ctx: TaskContext) {
    /* new code */
    if (NewTask) {
      LOG.info("this is a new task, taskindex = " + ctx.getTaskIndex);
    }
    /* code end */
    val trainer = new GraphLearner(modelClassName, ctx)

    trainer.NewLearner = NewTask;//////
    if (posnegRatio == -1) {
      trainer.train(taskDataBlock, validDataBlock)
    } else {
      trainer.train(posDataBlock, negDataBlock, validDataBlock)
    }
  }

  override def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  override def preProcess(taskContext: TaskContext) {
    LOG.info("preProcess input data")//////
    val start = System.currentTimeMillis()

    /* old code
    var count = 0
    val vali = Math.ceil(1.0 / valiRat).toInt

    val reader = taskContext.getReader
    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (count % vali == 0)
          validDataBlock.put(out)
        else if (posnegRatio != -1) {
          if (out.getY > 0) {
            posDataBlock.put(out)
          } else {
            negDataBlock.put(out)
          }
        } else {
          taskDataBlock.put(out)
        }
        count += 1
      }

      null.asInstanceOf[Vector]
    }
    code end */

    /* new code */
    var count = 0
    val vali = Math.ceil(1.0 / valiRat).toInt

    val SCIndex = 0
    var TotalS = 0l
    var TrainS = 0l
    var ValidS = 0l
    for (SCIndex <- 0 until WorkerContext.get().getDataBlockManager().realSplitClassifications.size()){
      val reader = taskContext.getReaderForRealSC(SCIndex)

      while (reader.nextKeyValue) {
        val out = parse(reader.getCurrentKey, reader.getCurrentValue)
        if (out != null) {
          if (count % vali == 0)
            validDataBlock.put(out)
          else if (posnegRatio != -1) {
            if (out.getY > 0) {
              posDataBlock.put(out)
            } else {
              negDataBlock.put(out)
            }
          } else {
            taskDataBlock.put(out)
          }
          count += 1
        }

        null.asInstanceOf[Vector]
      }
      TotalS = taskDataBlock.size + validDataBlock.size - TotalS
      TrainS = taskDataBlock.size - TrainS
      ValidS = validDataBlock.size - ValidS
      LOG.info("TotalS = " + TotalS)
      LOG.info("TrainS = " + TrainS)
      LOG.info("ValidS = " + ValidS)
      WorkerContext.get().getDataBlockManager().realSCsTotalSLength.add(TotalS)
      WorkerContext.get().getDataBlockManager().realSCsTrainSLength.add(TrainS)
      WorkerContext.get().getDataBlockManager().realSCsValidSLength.add(ValidS)
      TotalS = taskDataBlock.size + validDataBlock.size
      TrainS = taskDataBlock.size
      ValidS = validDataBlock.size
    }

    WorkerContext.get().getDataBlockManager().update_realSCsAllSTotalLength()
    WorkerContext.get().getDataBlockManager().print_realSCs_allSamples()

    posDataBlock.flush()
    negDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskIndex}] preprocessed ${
      taskDataBlock.size + validDataBlock.size
    } samples, ${taskDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation." +
      s" processing time is $cost"
    )
  }

  def getDataBlock(level: String = null): DataBlock[LabeledData] = {
    val storageLevel = if (level != null && level.length != 0) {
      level
    } else {
      SharedConf.storageLevel
    }

    if (storageLevel.equalsIgnoreCase("memory")) {
      new MemoryDataBlock[LabeledData](-1)
    } else if (storageLevel.equalsIgnoreCase("memory_disk")) {
      new MemoryAndDiskDataBlock[LabeledData](ctx.getTaskId.getIndex)
    } else {
      new DiskDataBlock[LabeledData](ctx.getTaskId.getIndex)
    }
  }
}
