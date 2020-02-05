package com.minal.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.PrintWriter
import java.io.File

object Main {
  
  val conf = new SparkConf().setAppName("AprioriMinal").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) 
  {
//    val support = 1000
//    val confidence = 0.5
    val support = args(1).toDouble
    val confidence = args(2).toDouble
    
    val fileInputPath = args(0).toString
    
//    val fileOutputPath = "outputt.txt"
    val fileOutputPath = args(3).toString
    
    apriori(fileInputPath, support, confidence, fileOutputPath) 
  }

  def apriori(fileInputPath: String, supportt: Double, confidence: Double,fileOutputPath: String) = 
  {  
    val transactions = sc.textFile(fileInputPath)
    val numOfTransactions: Double = transactions.count
    
    var support = supportt*numOfTransactions
    
    val fw = new PrintWriter(new File(fileOutputPath))
    var frequentItemRDD_1: org.apache.spark.rdd.RDD[(String, Int)] = aprioriPass1(transactions, support)
    println("k = 1")
    println("\nFrequent items: \n")
    fw.write("k = 1\n") 
    fw.write("\nFrequent items: \n")
  
    val frequentItemArr_1 = frequentItemRDD_1.map( rdd => {
      "\n"+rdd._1+","+rdd._2
    } ).collect().foreach(fw.write)
    
    frequentItemRDD_1.foreach(println)
    
    val freqItems = getFreqItems_1(frequentItemRDD_1)
    
    var frequentItemRDD_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)] = frequentItemRDD_1
    
    var k: Int = 2
   
    while(!frequentItemRDD_kMinusOne.isEmpty())
    {
      val freqCombos: org.apache.spark.rdd.RDD[String] = generateCandidatesSet(frequentItemRDD_kMinusOne, freqItems)
      val frequentItemRDD_k: org.apache.spark.rdd.RDD[(String, Int)] = generatefrequentItemRDD_k(transactions, freqCombos, support, k)
    
      println("--- --- --- --- ---\n\n")
      println("k = " + k)
      fw.write("\n\n--- --- --- --- ---\n\n") 
      fw.write("\nk = " + k+"\n")
      
      if(!frequentItemRDD_k.isEmpty())
      {
        println("\nFrequent itemsets: \n")
        fw.write("\nFrequent itemsets: \n")
        val frequentItemArr_k = frequentItemRDD_k.map( rdd => {
          "\n"+rdd._1+","+rdd._2
        } ).collect().foreach(fw.write)
        
        frequentItemRDD_k.foreach(println)
        
        val filteredAssocRulesWithConfidence = associationRulesFormation(confidence, numOfTransactions, frequentItemRDD_1, frequentItemRDD_k, frequentItemRDD_kMinusOne)
        println("\nAssociation rules:")
        fw.write("\n\nAssociation rules: \n\n")
        filteredAssocRulesWithConfidence.map(assocRule=>assocRule+"\n").collect().foreach((fw.write))
        filteredAssocRulesWithConfidence.foreach((println))
        
      }else{
        fw.write("\nNo more frequent itemsets.")
        print("No more frequent itemsets.")  
      }
      
      frequentItemRDD_kMinusOne = frequentItemRDD_k
      
      k += 1
    }
    fw.close()
  }
  
  def associationRulesFormation(confidence: Double, numOfTransactions: Double, frequentItemRDD_1: org.apache.spark.rdd.RDD[(String, Int)], 
      frequentItemRDD_k: org.apache.spark.rdd.RDD[(String, Int)], frequentItemRDD_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)]) : org.apache.spark.rdd.RDD[String] = 
  {
      val frequentItemRDD_1Set = frequentItemRDD_1.collect.toSet
      val frequentItemRDD_kSet = frequentItemRDD_k.collect.toSet
      val frequentItemRDD_kMinusOneSet = frequentItemRDD_kMinusOne.collect.toSet
    
      val itemsetsStr = frequentItemRDD_k.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
      val itemsets = itemsetsStr.map(str => str.split(",").toSet)
      
      val desiredSubsetSize = itemsets.take(1)(0).size
      val subsets = itemsets.map(s => (s, s.subsets.toSet.filter(ss => ss.size != 0 && ss.size != desiredSubsetSize)))

      val rules = subsets.map{ case (parentSet, childSet) => 
          childSet.map(setOfStr => 
            (getSupportFromfrequentItemRDD_k(frequentItemRDD_kSet, ("[" + (parentSet.mkString(",")) + "]")),
                setOfStr, parentSet diff setOfStr)) 
        }
      val rulesFlattened = rules.flatMap(s => s)

      val rulesWithConfidence = rulesFlattened.map{ case (num, i, j) => (num, "[" + (i.mkString(",")) + "]", "[" + (j.mkString(",")) + "]") }
      .map{ case (num, str, j) => (str + "-->" + j + " with confidence: ",   
        ((num.toDouble / (if (str.replaceAll("[\\[()\\]]", "").split(",").size == 1) 
                    getSupportFromfrequentItemRDD_k(frequentItemRDD_1Set, str)
                  else  
                    getSupportFromfrequentItemRDD_k(frequentItemRDD_kMinusOneSet, str)).toDouble))
        )
      }
      
      val filteredAssocRulesWithConfidence = rulesWithConfidence.filter{ case (str, conf) => conf >= confidence }
        .map{ case (str, conf) => str + conf }
        
      
      return filteredAssocRulesWithConfidence
  }
  
  def getSupportFromfrequentItemRDD_k(frequentItemRDD_kSet: Set[(String, Int)], keyToFind: String): String =
  {
    val support = frequentItemRDD_kSet.filter{ case (s,i) => s == keyToFind }.map{ case (s,i) => i }
    val supportStr = support.mkString("")
    
    return supportStr
  }
  
  def aprioriPass1(transactions: org.apache.spark.rdd.RDD[String], support: Double): org.apache.spark.rdd.RDD[(String, Int)] =
  { 
      val items = transactions.flatMap(txn => txn.split(" "))
      val itemOnes = items.map(item => ("["+item+"]", 1))
      val itemCounts_C1 = itemOnes.reduceByKey(_+_)
      val itemCounts_L1 = itemCounts_C1.filter{case (item,sup) => sup >= support}
      
      return itemCounts_L1
  }
  
  def getFreqItems_1(L1: org.apache.spark.rdd.RDD[(String, Int)]): org.apache.spark.rdd.RDD[String]  = 
  {
    val freqItems = L1.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
    return freqItems
  }
  
  def generateCandidatesSet(frequentItemRDD_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)], freqItems_L1: org.apache.spark.rdd.RDD[String])
  :org.apache.spark.rdd.RDD[String] = 
  {
    val tupleLength = frequentItemRDD_kMinusOne.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }.top(1)
    val desiredTupleLength = tupleLength(0).split(",").size + 1
    
    val itemsets = frequentItemRDD_kMinusOne.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
      val freqItemCombos = itemsets.cartesian(freqItems_L1).map(x=>x.toString.replaceAll("[\\[()\\]]", "").split(",").toSet.toSeq.sorted)
      .filter(tupleSet => tupleSet.size == desiredTupleLength)
      .map(strArr => strArr.map(_.toString())).collect.toSet
      
      val freqItemCombosStr = freqItemCombos.map(setOfInts => setOfInts.mkString(","))
      
      val freqItemCombosRdd: org.apache.spark.rdd.RDD[String] = 
        sc.parallelize(freqItemCombosStr.toSeq)
        
    print("\n")
        
      return freqItemCombosRdd
  }
  
  def generatefrequentItemRDD_k(transactions: org.apache.spark.rdd.RDD[String], freqCombos: org.apache.spark.rdd.RDD[String], support: Double, k: Int)
  : org.apache.spark.rdd.RDD[(String, Int)] 
  = 
  {
    val itemsPerTxn = transactions.map(txn => txn.split(" "))
    val itemsPerTxnInt = itemsPerTxn.map(txnItems => txnItems.map(_.toString()))
    val txnCombos = itemsPerTxnInt.map(txnItems => txnItems.combinations(k).toArray)
      
    val txnCombosStr = txnCombos.map(arrOfarr => arrOfarr.map(arrOfInt => arrOfInt.toSet.mkString(",")))
   
    val freqCombosSets = freqCombos.collect().toSet
      
    val intersection = txnCombosStr.map(arr => arr.filter(tuple => freqCombosSets.contains(tuple)))
    val intersectionOnes = intersection.flatMap(arrayOfPairs => arrayOfPairs.map(array => ("["+array+"]",1)))
    
    val C_k = intersectionOnes.reduceByKey(_+_)

    val frequentItemRDD_k = C_k.filter{case (str,sup) => sup >= support}
    
    return frequentItemRDD_k
  }
}