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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.MLLearner
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers.AngelGraph
import com.tencent.angel.ml.core.optimizer.decayer.{StepSizeScheduler, WarmRestarts}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.{DoubleVector, IntKeyVector, LongKeyVector, Vector}
import com.tencent.angel.ml.metric.LossMetric
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.core.utils.{DataParser, ValidationUtils}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

import util.control.Breaks._

class GraphLearner(modelClassName: String, ctx: TaskContext) extends MLLearner(ctx) {
  val LOG: Log = LogFactory.getLog(classOf[GraphLearner])

  val epochNum: Int = SharedConf.epochNum
  val indexRange: Long = SharedConf.indexRange
  val modelSize: Long = SharedConf.modelSize
  val lr0: Double = SharedConf.learningRate

  /*new code*/
  val skippedWorkerEpochBoolean: Boolean = SharedConf.skippedWorkerEpochBoolean
  val skippedWorkerEpochStart: Int = SharedConf.skippedWorkerEpochStart
  val skippedWorkerEpochEnd: Int = SharedConf.skippedWorkerEpochEnd

  val skippedServerEpochBoolean: Boolean = SharedConf.skippedServerEpochBoolean
  val skippedServerEpochStart: Int = SharedConf.skippedServerEpochStart
  val skippedServerEpochEnd: Int = SharedConf.skippedServerEpochEnd

  var keepExecution: Boolean = true
  val dataParser = DataParser(SharedConf.get())
  /*code end*/

  // Init Graph Model
  val model: GraphModel = GraphModel(modelClassName, conf, ctx)
  model.buildNetwork()
  val graph: AngelGraph = model.graph
  val ssScheduler: StepSizeScheduler = StepSizeScheduler(SharedConf.getStepSizeScheduler, lr0)
  val decayOnBatch = conf.getBoolean(MLConf.ML_OPT_DECAY_ON_BATCH, MLConf.DEFAULT_ML_OPT_DECAY_ON_BATCH)

  /* old code */
  // def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double = {
  /* new code */
  def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double = {
  /* code end */
    var batchCount: Int = 0
    var loss: Double = 0.0

    breakable(//////
    while (iter.hasNext) {
      /* old code
      // LOG.info("start to feedData ...")
      graph.feedData(iter.next())

      // LOG.info("start to pullParams ...")
      graph.pullParams(epoch)

      // LOG.info("calculate to forward ...")
      loss = graph.calLoss() // forward
      // LOG.info(s"The training los of epoch $epoch batch $batchCount is $loss" )

      // LOG.info("calculate to backward ...")
      graph.calBackward() // backward

      // LOG.info("calculate and push gradient ...")
      graph.pushGradient() // pushgrad
      // waiting all gradient pushed

      // LOG.info("waiting for push barrier ...")
      code end */
      /*new code*/
      if (ctx.getTaskId.getIndex != 0 && epoch >= skippedWorkerEpochStart && epoch <= skippedWorkerEpochEnd && skippedWorkerEpochBoolean){
        LOG.info(s"task ${ctx.getTaskId.getIndex} skips epoch $epoch!")
        iter.next()
        LOG.info("just push null gradient ...")
        graph.pushGradient_null() // this worker does not work, just skipped this epoch and push null
      }else{
        LOG.info("start to feedData ...")
        graph.feedData(iter.next())

        LOG.info("start to pullParams ...")
        graph.pullParams(epoch)

        LOG.info("calculate to forward ...")
        loss = graph.calLoss() // forward
        LOG.info(s"The training los of epoch $epoch batch $batchCount is $loss" )

        LOG.info("calculate to backward ...")
        graph.calBackward() // backward

        LOG.info("calculate and push gradient ...")
        if (epoch >= skippedServerEpochStart && epoch <= skippedServerEpochEnd && skippedServerEpochBoolean){
          LOG.info("partial update with epoch " + epoch)
          // graph.pushGradient_partial(epoch) // this worker push its gradients to partial servers
          graph.pushGradient_none(epoch) // this worker do not push its gradients to any server
        }else{
          graph.pushGradient() // pushgrad
        }
        // waiting all gradient pushed

        LOG.info("waiting for push barrier ...")
      }
      /*code end*/

      PSAgentContext.get().barrier(ctx.getTaskId.getIndex)
      LOG.info("push barrier is finished!") //////
      if (decayOnBatch) {
        graph.setLR(ssScheduler.next())
      }
      if (ctx.getTaskId.getIndex == 0) {
        LOG.info("start to update ...")
        graph.update(epoch * numBatch + batchCount, 1) // update parameters on PS
      }

      // waiting all gradient update finished
      LOG.info("waiting for update barrier ...")
      PSAgentContext.get().barrier(ctx.getTaskId.getIndex)
      LOG.info("update barrier is finished!") //////
      batchCount += 1

      LOG.info(s"epoch $epoch batch $batchCount is finished!")

      /* new code */
      if (ctx.getTaskId.getIndex == 1 && epoch > 1000000){
        PSAgentContext.get().removeWorker(ctx.getTaskId.getIndex)
        LOG.info("break the execution of trainOneEpoch at epoch = " + epoch + ", batch = " + batchCount)
        keepExecution = false
        break()
      }
      /* code end */
    }
    )//////

    loss
  }

  /**
    * train LR model iteratively
    *
    * @param trainData      : trainning data storage
    * @param validationData : validation data storage
    */
  override def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): MLModel = {
    train(trainData, null, validationData)
  }

  def train(posTrainData: DataBlock[LabeledData],
            negTrainData: DataBlock[LabeledData],
            validationData: DataBlock[LabeledData]): MLModel = {
    LOG.info(s"Task[${ctx.getTaskIndex}]: Starting to train ...")
    LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epochNum, initLearnRate=$lr0")
    /* new code */
    LOG.info("trainData size = " + posTrainData.size());
    LOG.info("validationData size = " + validationData.size());
    /* code end */

    val trainDataSize = if (negTrainData == null) posTrainData.size() else {
      posTrainData.size() + negTrainData.size()
    }

    globalMetrics.addMetric(MLConf.TRAIN_LOSS, LossMetric(trainDataSize))
    globalMetrics.addMetric(MLConf.VALID_LOSS, LossMetric(validationData.size))
    graph.taskNum = ctx.getTotalTaskNum

    val loadModelPath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH, "")
    if (loadModelPath.isEmpty) {
      model.init(ctx.getTaskId.getIndex)
    }

    PSAgentContext.get().barrier(ctx.getTaskId.getIndex)

    val numBatch = SharedConf.numUpdatePerEpoch
    val batchSize: Int = (trainDataSize + numBatch - 1) / numBatch
    val batchData = new Array[LabeledData](batchSize)

    if (SharedConf.useShuffle) {
      posTrainData.shuffle()
      if (negTrainData != null) {
        negTrainData.shuffle()
      }
    }

    /*new code*/
    LOG.info(s"skipped epoch start at $skippedWorkerEpochStart epoch.")
    LOG.info(s"skipped epoch end at $skippedWorkerEpochEnd epoch.")

    LOG.info(s"num of batches within one epoch = $numBatch.")
    LOG.info(s"batchSize = $batchSize.")

    LOG.info(s"Trainable layers in the graph are:")
    graph.getTrainable.foreach(layer => LOG.info(layer.toString))
    /*code end*/

    breakable(//////
    while (ctx.getEpoch < epochNum) {
      val epoch = ctx.getEpoch
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch start.")

      val iter = if (negTrainData == null) {
        getBathDataIterator(posTrainData, batchData, numBatch)
      } else {
        getBathDataIterator(posTrainData, negTrainData, batchData, numBatch)
      }
      /* new code */
      LOG.info("iter.samples.size = " + posTrainData.size())
      LOG.info("iter.validate.size = " + validationData.size())
      /* code end */

      val startTrain = System.currentTimeMillis()
      if (!decayOnBatch) {
        graph.setLR(ssScheduler.next())
      }
      val loss: Double = trainOneEpoch(epoch, iter, numBatch)
      val trainCost = System.currentTimeMillis() - startTrain
      globalMetrics.metric(MLConf.TRAIN_LOSS, loss * trainDataSize)
      LOG.info(s"$epoch-th training finished! the trainCost is $trainCost")

      LOG.info(s"Begin to validate in $epoch-th epoch")
      val startValid = System.currentTimeMillis()
      validate(epoch, validationData)
      val validCost = System.currentTimeMillis() - startValid

      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch success. " +
        s"epoch cost ${trainCost + validCost} ms." +
        s"train cost $trainCost ms. " +
        s"validation cost $validCost ms.")

      ctx.incEpoch()

      /* new code */
      if (!keepExecution){
        LOG.info("break the execution of this while (ctx.getEpoch < epochNum) at epoch = " + epoch)
        break()
      }
      if (epoch == 2){
        LOG.info("appendProcess input data at the end of epoch = " + epoch)
        appendProcess(validationData, posTrainData, negTrainData)
      }
      /* code end */
    }
    )//////

    model.graph.timeStats.summary()
    model
  }

  /* new code */
  def appendProcess(validDataBlock: DataBlock[LabeledData],posDataBlock: DataBlock[LabeledData],
                             negDataBlock: DataBlock[LabeledData]) {
    if (!ctx.ExistAppendSplits()){
      return
    }
    LOG.info("appendProcess input data")
    val start = System.currentTimeMillis()

    var count = 0
    val valiRat = SharedConf.validateRatio
    val posnegRatio: Double = SharedConf.posnegRatio()
    val vali = Math.ceil(1.0 / valiRat).toInt

    val reader = ctx.getReaderForAppendSplits
    var i: Int = 0
    while (reader.nextKeyValue) {
      i = i + 1
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
          posDataBlock.put(out)
        }
        count += 1
      }

      null.asInstanceOf[Vector]
    }
    LOG.info("i =" + i)

    posDataBlock.flush()
    negDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskIndex}] appendprocessed ${
      posDataBlock.size + validDataBlock.size
    } samples, ${posDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation." +
      s" processing time is $cost"
    )
  }


  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }
  /* code end */

  private def getBathDataIterator(trainData: DataBlock[LabeledData],
                                  batchData: Array[LabeledData], numBatch: Int) = {
    trainData.resetReadIndex()
    assert(batchData.length > 1)

    new Iterator[Array[LabeledData]] {
      private var count = 0

      override def hasNext: Boolean = count < numBatch

      override def next(): Array[LabeledData] = {
        batchData.indices.foreach { i => batchData(i) = trainData.loopingRead() }
        count += 1
        batchData
      }
    }
  }

  private def getBathDataIterator(posData: DataBlock[LabeledData],
                                  negData: DataBlock[LabeledData],
                                  batchData: Array[LabeledData], numBatch: Int) = {
    posData.resetReadIndex()
    negData.resetReadIndex()
    assert(batchData.length > 1)

    new Iterator[Array[LabeledData]] {
      private var count = 0
      val posnegRatio: Double = SharedConf.posnegRatio()
      val posPreNum: Int = Math.max((posData.size() + numBatch - 1) / numBatch,
        batchData.length * posnegRatio / (1.0 + posnegRatio)).toInt

      val posNum: Int = if (posPreNum < 0.5 * batchData.length) {
        Math.max(1, posPreNum)
      } else {
        batchData.length / 2
      }
      val negNum: Int = batchData.length - posNum

      LOG.info(s"The exact pos/neg is ${1.0 * posNum / negNum} ")

      val posDropRate: Double = if (posNum * numBatch > posData.size()) {
        0.0
      } else {
        1.0 * (posData.size() - posNum * numBatch) / posData.size()
      }

      LOG.info(s"${posDropRate * 100}% of positive data will be discard in task ${ctx.getTaskIndex}")

      val negDropRate: Double = if (negNum * numBatch > negData.size()) {
        0.0
      } else {
        1.0 * (negData.size() - negNum * numBatch) / negData.size()
      }

      LOG.info(s"${negDropRate * 100}% of negative data will be discard in task ${ctx.getTaskIndex}")

      override def hasNext: Boolean = count < numBatch

      override def next(): Array[LabeledData] = {
        (0 until posNum).foreach { i =>
          if (posDropRate == 0) {
            batchData(i) = posData.loopingRead()
          } else {
            var flag = true
            while (flag) {
              val pos = posData.loopingRead()
              if (Math.random() > posDropRate) {
                batchData(i) = pos
                flag = false
              }
            }
          }
        }

        (0 until negNum).foreach { i =>
          if (negDropRate == 0) {
            batchData(i + posNum) = negData.loopingRead()
          } else {
            var flag = true
            while (flag) {
              val neg = negData.loopingRead()
              if (Math.random() > negDropRate) {
                batchData(i + posNum) = neg
                flag = false
              }
            }
          }
        }

        count += 1
        batchData
      }
    }
  }

  /**
    * validate loss, Auc, Precision or other
    *
    * @param epoch    : epoch id
    * @param valiData : validata data storage
    */
  def validate(epoch: Int, valiData: DataBlock[LabeledData]): Unit = {
    val isClassification = conf.getBoolean(MLConf.ML_MODEL_IS_CLASSIFICATION, MLConf.DEFAULT_ML_MODEL_IS_CLASSIFICATION)
    val numClass = conf.getInt(MLConf.ML_NUM_CLASS, MLConf.DEFAULT_ML_NUM_CLASS)
    if (isClassification && valiData.size > 0) {
      if (numClass == 2) {
        val validMetric = new ValidationUtils(valiData, model).calMetrics(model.lossFunc)
        LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch " +
          s"validationData loss=${validMetric._1 / valiData.size()} " +
          s"precision=${validMetric._2} " +
          s"auc=${validMetric._3} " +
          s"trueRecall=${validMetric._4} " +
          s"falseRecall=${validMetric._5}")
        globalMetrics.metric(MLConf.VALID_LOSS, validMetric._1)
      } else {
        val validMetric = new ValidationUtils(valiData, model).calMulMetrics(model.lossFunc)

        LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch " +
          s"validationData loss=${validMetric._1 / valiData.size()} " +
          s"accuracy=${validMetric._2} ")

        globalMetrics.metric(MLConf.VALID_LOSS, validMetric._1)
      }
    } else if (valiData.size > 0) {
      val validMetric = new ValidationUtils(valiData, model).calMSER2()
      LOG.info(s"Task[${ctx.getTaskIndex}]: epoch=$epoch " +
        s"validationData MSE=${validMetric._1} " +
        s"RMSE=${validMetric._2} " +
        s"MAE=${validMetric._3} " +
        s"R2=${validMetric._4} ")
      globalMetrics.metric(MLConf.VALID_LOSS, validMetric._1 * valiData.size)
    } else {
      LOG.info("No Validate !")
    }
  }

  def sparsity(weight: DoubleVector, dim: Int): Double = {
    weight match {
      case w: IntKeyVector => w.numZeros().toDouble / modelSize
      case w: LongKeyVector => w.numZeros().toDouble / modelSize
    }
  }
}
