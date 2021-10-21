


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.util.StringTokenizer
import scala.collection.JavaConverters.*
import scala.util.matching.Regex  ///
import com.typesafe.config.ConfigFactory  ///

/**
 * Compute a spreadsheet or an CSV file that shows the distribution of different types of messages
 * across predefined time intervals and injected string instances of the designated regex pattern for
 * these log message types.
 */

class Task1
object Task1 {


  private val config = ConfigFactory.load("application") ///
  val pattern: String = config.getString("randomLogGenerator.Pattern") ///
  val regx = pattern.r


  /**
  * Take the log and as input produce the time interval as key which is one minute and the rest of the log as value
  */
  class TokenizerMapper extends Mapper[Object, Text, Text, Text] {

    val word = new Text()
    val one = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      val itr = value.toString

      val msg = itr.split(" - ",2)

      if(!(msg.length < 2)) {
        val start_time = itr.substring(0, 5)

//        val end_time = start_time.substring(0, 2) + ":0" + (start_time.substring(4, 5).toInt + 1).toString
//
//        val interval = start_time + " to " + end_time
        //compare the time now and find the correct slot as key for this time of the itr and assign that time
        //as the key during context write.
        //time_interval = Current minute that is all logs in current minute will be mapped to this time interval.
        val interval = "Distribution during this minute " +start_time + "  "

        val msg = itr.substring(12)

        val matched = regx.findFirstIn(msg)

        if (!matched.isEmpty) {
          word.set(interval)
          one.set(msg)
          context.write(word, one)

        }
      }
    }
  }

  /**
   * Take the time interval as key and finds the distribution of different types of messages across time intervals
   */

  class IntSumReader extends Reducer[Text,Text,Text,Text] {

    val total = new Text()

    // Key is the time interval and the value is all the logs in that time interval
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      var log_count = Array(0,0,0,0)

      val v = values.iterator
      while(v.hasNext){
        val msg = v.next().toString()
        if(msg.contains("ERROR")){
          log_count(0) = log_count(0) +1
        }
        if(msg.contains("INFO")){
          log_count(1) = log_count(1) +1
        }
        if(msg.contains("DEBUG")){
          log_count(2) = log_count(2) +1
        }
        if(msg.contains("WARN")){
          log_count(3) = log_count(3) +1
        }
      }

      val reduc_txt = "WARN: "+log_count(3).toString + " DEBUG: "+log_count(2).toString + " INFO: "+log_count(1).toString +" ERROR: "+log_count(0).toString

      total.set(reduc_txt)

      context.write(key, total)
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")

    val job = Job.getInstance(configuration,"log count per interval")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[IntSumReader])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

}

