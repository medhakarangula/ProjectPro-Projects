//read businesses
val bizDF = spark.read.json("s3://projectpro2022/yelp_academic_dataset_business.json")

//to get the complex schema and write it to local path
val schema = bizDF.schema.prettyJson
import java.nio.file._
val path = Paths.get(new java.net.URI("file:///home/hadoop/businesses_schema.json"))
Files.write(path, schema.getBytes, StandardOpenOption.CREATE_NEW)

//convert categories to array of string
val bizDF1=bizDF.select(col("business_id"),split(col("categories"),",").as("categories")).filter(col("categories").isNotNull)

val catRdd = bizDF1.select("categories").rdd.flatMap(row => 
row.getAs[scala.collection.mutable.WrappedArray[String]](0).toSeq).
    distinct.
    sortBy(i => i).
    coalesce(1).  //change the rdd to a partition of n
    mapPartitions(itr => {
        var index = 0
        itr map (i => {
            index = index + 1
            (index, i)
        })
    })

val catDF =catRdd.toDF
catDF.write.saveAsTable("categories")

def parseRow(row: Row) : Seq[(String, String)] ={
val user_Id = row.getAs[String](0)
val fList = row.getAs[scala.collection.mutable.WrappedArray[String]](1)
fList.map(s => (user_Id, s))
}


//create a map of category to id (NightLife -> 10)
val catMap = catDF.rdd.map((row: Row) => (row.getAs[String](1)->row.getAs[Integer](0))).collect.toMap

//(business_id, category_name)
val bizCatRDD = bizDF1.select("business_id","categories").flatMap(parseRow(_))

//(business_id, category_id)
val bizCat = bizCatRDD.map(t => (t._1, catMap(t._2))).toDF

//insert into business_category table
bizCat.toDF.coalesce(1).write.saveAsTable("business_category")







import org.apache.spark.sql.functions._
val dayOfWeek=udf[String,String]((fdate: String)=>{
import java.text.SimpleDateFormat
val df=new SimpleDateFormat("yyyy-MM-dd")
new SimpleDateFormat("EEEEEE").format(df.parse(fdate))})

val monthName=udf[String,Int]((month:Int)=>{
month match{
case 1=> "January"
case 2=> "February"
case 3=> "March"
case 4=> "April"
case 5=> "May"
case 6=> "June"
case 7=> "July"
case 8=> "August"
case 9=> "September"
case 10=> "October"
case 11=> "November"
case 12=> "December"
case _=> "January"
}
})




val reviewDF=spark.read.json("s3://projectpro2022/yelp_academic_dataset_review_small.json")

val revReviewDF = reviewDF.select($"review_id",$"user_id",$"business_id",$"stars",$"text",$"date",$"cool",$"funny",$"useful", substring($"date", 1, 4).cast("int").as("year"), substring($"date", 6, 2).cast("int").as("month"),  substring($"date", 9, 2).cast("int").as("day"), monthName(substring(reviewDF("date"), 6, 2)).as("monthname"), dayOfWeek(reviewDF("date")).as("dayofweek"), weekofyear(substring(reviewDF("date"), 1,10)).as("weekofyear"))

//coalesce to reduce number of partitions
revReviewDF.coalesce(5).write.saveAsTable("reviews")

//coalesce to increase number of partitions
revReviewDF.repartition(5).write.saveAsTable("reviews")
revReviewDF.coalesce(2).write.saveAsTable("reviews")

bizDF1.cache



//Bonus Code(to remove spaces from category RDD)
//Note: Replace catRdd rdd declaration with the following
val catRdd = bizDF1.select("categories").rdd.flatMap(row => 
row.getAs[scala.collection.mutable.WrappedArray[String]](0).toSeq).
    distinct.
    sortBy(i => i).
    coalesce(1).  //change the rdd to a partition of n
    mapPartitions(itr => {
        var index = 0
        itr map (i => {
            index = index + 1
            (index, i.trim())
        })
    })

//Note: Replace bizCat definition as follows to search with trim
val bizCat = bizCatRDD.map(t => (t._1, catMap(t._2.trim()))).toDF













