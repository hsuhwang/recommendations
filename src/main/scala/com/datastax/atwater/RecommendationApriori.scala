/**
  * Build a prediction model based on purchased history,
  * Recommendations based on the current items in the
  * shopping cart.
  * @author Hsu-Kwang Hwang
  */


object RecommendationApriori {

  case class ProductList(customer_id: String,products: Array[String])

  def main(args: Array[String]) {


    val session = SparkSession.builder().appName("AtwaterRecommendJob").config("spark.sql.crossJoin.enabled","true").master("local[2]").getOrCreate()
    val data = session.read.cassandraFormat("orders", "ebusiness").load().select("customer_id","product_name")
    val customerProduct = data.withColumn("product_list",collect_list("product_name").over(Window.partitionBy("customer_id")))

    import session.implicits._
    val products = customerProduct.select("product_list").distinct().map(x=>x.getAs[mutable.WrappedArray[String]](0).toArray)

    val fpg = new FPGrowth().setMinSupport(0.01).setNumPartitions(1)
    val model = fpg.run(products.rdd)
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") +
        ", " + itemset.freq)
    }

    val minConfidence = 0.8

    model.generateAssociationRules(minConfidence).collect.foreach(rule => {
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    })

    println("The suggestted list")

    val ar = new AssociationRules()
      .setMinConfidence(0.1)
    val results = ar.run(model.freqItemsets)

    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "=>"
        + rule.consequent.mkString(",") + "]," +
        rule.confidence)
    }

    session.stop()
    sys.exit(0)
  }

}
