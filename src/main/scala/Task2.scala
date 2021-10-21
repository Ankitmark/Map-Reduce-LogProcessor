

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



import org.apache.hadoop.io.IntWritable///
import org.apache.hadoop.io.WritableComparable///

/**
 * Compute time intervals sorted in the descending order that contained most log messages of the type ERROR
 * with injected regex pattern string instances.
 */

class Task2
object Task2 {


  /**
   * Retrive the pattern from the config file
   */
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
        val end_time = start_time.substring(0, 2) + ":0" + (start_time.substring(4, 5).toInt + 1).toString
        val interval = start_time + " to " + end_time + " ERROR Count = "
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
   * Take the time interval as key and finds the distribution of ERROR types of messages across time intervals
   */

  class Task2Reducer extends Reducer[Text, Text, Text, IntWritable] {

    val total = new IntWritable()

    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
      var error_count = 0
      val v = values.iterator
      while (v.hasNext) {
        val msg = v.next().toString()
        if (msg.contains("ERROR")) {
          error_count = error_count + 1
        }
      }

      val reduc_txt = error_count
      total.set(reduc_txt)
      context.write(key, total)
    }
  }


  class keyValueSwapper extends Mapper[Object, Text, IntWritable, Text] {

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {

      val kv = value.toString.split("\t")
      val time_intv = kv(0)
      val error_count = Integer.parseInt(kv(1))
      context.write(new IntWritable(error_count), new Text(time_intv))

    }
  }


  class secondReducer extends Reducer[IntWritable, Text, Text, IntWritable] {

    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      context.write(new Text(values.iterator.next() ), key)
    }
  }


    class DecreasingComparator extends IntWritable.Comparator {
      override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = -super.compare(a, b)
      override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = -super.compare(b1, s1, l1, b2, s2, l2)
    }


  def main(args: Array[String]): Unit = {

    //First job
    val configuration = new Configuration

    val job = Job.getInstance(configuration, "first")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[Task2Reducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable]);

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.waitForCompletion(true)


    //Second job
    val configuration2 = new Configuration
    configuration2.set("mapred.textoutputformat.separator", ",")

    val job2 = Job.getInstance(configuration2, "second")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[keyValueSwapper])
    job2.setReducerClass(classOf[secondReducer])
    job2.setMapOutputKeyClass(classOf[IntWritable])
    job2.setMapOutputValueClass(classOf[Text])


    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setSortComparatorClass(classOf[DecreasingComparator]);



    FileInputFormat.addInputPath(job2, new Path(args(1)))
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))

    System.exit(if (job2.waitForCompletion(true)) 0 else 1)
  }


}