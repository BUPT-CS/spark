package mlib

import base.StatisticAction
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType, Strategy}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object GbdtAndLr {

  //get decision tree leaf's nodes
  def getLeafNodes(node:Node):Array[Int] = {
    var treeLeafNodes = new Array[Int](0)
    if (node.isLeaf){
      treeLeafNodes = treeLeafNodes.:+(node.id)
    }else{
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.leftNode.get)
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.rightNode.get)
    }
    treeLeafNodes
  }

  // predict decision tree leaf's node value
  def predictModify(node:Node,features:DenseVector):Int={
    val split = node.split
    if (node.isLeaf) {
      node.id
    } else {
      if (split.get.featureType == FeatureType.Continuous) {
        if (features(split.get.feature) <= split.get.threshold) {
          //          println("Continuous left node")
          predictModify(node.leftNode.get,features)
        } else {
          //          println("Continuous right node")
          predictModify(node.rightNode.get,features)
        }
      } else {
        if (split.get.categories.contains(features(split.get.feature))) {
          //          println("Categorical left node")
          predictModify(node.leftNode.get,features)
        } else {
          //          println("Categorical right node")
          predictModify(node.rightNode.get,features)
        }
      }
    }
  }

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: GbdtAndLr  <sample hdfs> <numtrees> <saveGBDTModelDir> <transform data dir>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("GbdtAndLr")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[StatisticAction]))
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    val sampleDir= args(0)

    val dataSet = sc.textFile(sampleDir,200).map(x=>x.trim.split("\t"))

    val data = dataSet.map(x=>LabeledPoint(x(0).toInt,new DenseVector(Array(x(1).toInt,x(2).toInt,x(3).toInt,
      x(4).toInt,x(5).toInt,x(6).toInt,x(7).toInt))))


    val splits = data.randomSplit(Array(0.8,0.2))
    val train = splits(0)
    val test = splits(1)

    // GBDT Model
    val numTrees = args(1).toInt
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(numTrees)
    val treeStratery = Strategy.defaultStrategy("Classification")
    treeStratery.setMaxDepth(5)
    treeStratery.setNumClasses(2)
    treeStratery.setCategoricalFeaturesInfo(Map[Int, Int]())
    boostingStrategy.setTreeStrategy(treeStratery)
    val gbdtModel = GradientBoostedTrees.train(train, boostingStrategy)
    val gbdtModelDir = args(2)
    gbdtModel.save(sc, gbdtModelDir)
//    val gbdtModel = GradientBoostedTreesModel.load(sc,gbdtModelDir)

    val labelAndPreds = test.map { point =>
      val prediction = gbdtModel.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / test.count()
    println("Test Error = " + testErr)
    println("Learned classification GBT model:\n" + gbdtModel.toDebugString)

    val treeLeafArray = new Array[Array[Int]](numTrees)
    for(i<- 0.until(numTrees)){
      treeLeafArray(i) = getLeafNodes(gbdtModel.trees(i).topNode)
    }

    val transformDataDir = args(3)
    val newFeatureDataSet = sc.textFile(transformDataDir,200).map(x=>x.trim.split("\t")).
      map(x=>(x(0).toInt,new DenseVector(Array(x(1).toInt,x(2).toInt,x(3).toInt,x(4).toInt,
      x(5).toInt,x(6).toInt,x(7).toInt)))).
      map{x=>
        var newFeature = new Array[Double](0)
        for(i<- 0.until(numTrees)){
          val treePredict = predictModify(gbdtModel.trees(i).topNode,x._2)
          //gbdt tree is binary tree
          val treeArray = new Array[Double]((gbdtModel.trees(i).numNodes+1)/2)
          treeArray(treeLeafArray(i).indexOf(treePredict))=1
          newFeature = newFeature ++ treeArray
        }
      (x._1,newFeature)
    }
    newFeatureDataSet.saveAsTextFile("")


    val newData = newFeatureDataSet.map(x=>LabeledPoint(x._1,new DenseVector(x._2)))
    val splits2 = newData.randomSplit(Array(0.8,0.2))
    val train2 = splits2(0)
    val test2 = splits2(1)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(train2).setThreshold(0.01)
    model.weights
    val predictionAndLabels = test2.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)

    sc.stop()
  }
}


