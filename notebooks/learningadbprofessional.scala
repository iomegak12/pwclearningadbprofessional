// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Movies and Ratings Analytics
// MAGIC ## This Spark Application analyzes Movies and Ratings

// COMMAND ----------

val defaultMoviesUrl = "https://iomegastorage.blob.core.windows.net/data/movies.csv"
val moviesUrlWidget = dbutils.widgets.text("moviesUrl", "")
var moviesUrl = dbutils.widgets.get("moviesUrl")

if(moviesUrl == null || moviesUrl == "") {
  moviesUrl = defaultMoviesUrl
}

val defaultRatingsUrl = "adl://iomegadlsv2.azuredatalakestore.net/data/ratings-c.csv"
val ratingsUrlWidget = dbutils.widgets.text("ratingsUrl","")
var ratingsUrl = dbutils.widgets.get("ratingsUrl")

if(ratingsUrl == null || ratingsUrl == "") {
  ratingsUrl = defaultRatingsUrl
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![PwC Corporation](https://www.logolynx.com/images/logolynx/76/76c488503093cc9560df2a5cca50781f.png)

// COMMAND ----------

package com.pwc.libraries.analytics

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object MovieNamesLoaderv2 {
  def loadMovieNames(moviesUrl: String): Map[Int, String] = {
    if(moviesUrl == null || moviesUrl == "") {
      throw new Exception("Invalid Argument(s) Specified!")
    }
    
    implicit val codec: Codec = Codec("UTF-8")
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    
    val lines = Source.fromURL(moviesUrl).getLines
    var map: Map[Int, String] = Map()
    
    for(line <- lines) {
      val splittedLine = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      val movieId = splittedLine(0).trim.toInt
      val movieName = splittedLine(1)
      
      map += (movieId -> movieName)
    }
    
    map
  }
}

// COMMAND ----------

import com.pwc.libraries.analytics._

val broadcastedMovies = sc.broadcast(() => { MovieNamesLoaderv2.loadMovieNames(moviesUrl) })

// COMMAND ----------

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type","ClientCredential")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id","49cb0a8b-8576-4067-8c94-db39ef412e12")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential","AGpb5gBxxqn0biB9qIQpJAw/+oM8g2F0btxOVYDzwGM=")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url","https://login.microsoftonline.com/97ea9543-5331-4483-aafd-b4673f025370/oauth2/token")
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id","49cb0a8b-8576-4067-8c94-db39ef412e12")
spark.conf.set("dfs.adls.oauth2.credential","AGpb5gBxxqn0biB9qIQpJAw/+oM8g2F0btxOVYDzwGM=")
spark.conf.set("dfs.adls.oauth2.refresh.url","https://login.microsoftonline.com/97ea9543-5331-4483-aafd-b4673f025370/oauth2/token")


// COMMAND ----------

val ratings = sc.
  textFile(ratingsUrl).
  mapPartitionsWithIndex((index, iterator) => {
    if(index == 0) {
      iterator.drop(1)
    } else iterator
  }).
  map(line => {
    val splitted = line.split(",")
    val rating = splitted(2).trim.toFloat
    
    (rating, 1)
  }).
  reduceByKey((value1, value2) => value1 + value2).
  toDF

// COMMAND ----------

case class Rating(rating: Float, noOfRatings: Int)

val ratingsDS = ratings.map(record => new Rating(record.getFloat(0), record.getInt(1)))

ratingsDS.createOrReplaceTempView("ratings")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT rating, SUM(noOfRatings) as totalRatings
// MAGIC FROM ratings
// MAGIC GROUP BY rating
// MAGIC HAVING rating BETWEEN 2.0 AND 5.0
// MAGIC ORDER BY totalRatings DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT rating, SUM(noOfRatings) as totalRatings
// MAGIC FROM ratings
// MAGIC GROUP BY rating
// MAGIC HAVING rating BETWEEN 2.0 AND 5.0
// MAGIC ORDER BY totalRatings DESC

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)
// MAGIC 
// MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
// MAGIC 
// MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)
// MAGIC 
// MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$

// COMMAND ----------

val colorsRDD = sc.parallelize(Array((197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), (230,245,208), (184,225,134), (127,188,65), (77,146,33)))
val colors = colorsRDD.collect()

displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

path {
  fill: yellow;
  stroke: #000;
}

circle {
  fill: #fff;
  stroke: #000;
  pointer-events: none;
}

.PiYG .q0-9{fill:rgb${colors(0)}}
.PiYG .q1-9{fill:rgb${colors(1)}}
.PiYG .q2-9{fill:rgb${colors(2)}}
.PiYG .q3-9{fill:rgb${colors(3)}}
.PiYG .q4-9{fill:rgb${colors(4)}}
.PiYG .q5-9{fill:rgb${colors(5)}}
.PiYG .q6-9{fill:rgb${colors(6)}}
.PiYG .q7-9{fill:rgb${colors(7)}}
.PiYG .q8-9{fill:rgb${colors(8)}}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
<script>

var width = 960,
    height = 500;

var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("class", "PiYG")
    .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });

var path = svg.append("g").selectAll("path");

svg.selectAll("circle")
    .data(vertices.slice(1))
  .enter().append("circle")
    .attr("transform", function(d) { return "translate(" + d + ")"; })
    .attr("r", 2);

redraw();

function redraw() {
  path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
  path.exit().remove();
  path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
}

</script>
  """)


// COMMAND ----------

