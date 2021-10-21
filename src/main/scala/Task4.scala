

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
 * Produce the number of characters in each log message for each log message type that contain
 * the highest number of characters in the detected instances of the designated regex pattern.
 */

class Task4
object Task4 {

  /**
   * Retrive the pattern from the config file
   */

    private val config = ConfigFactory.load("application") ///
    val pattern: String = config.getString("randomLogGenerator.Pattern") ///
    val regx = pattern.r

  /**
   * Mapper takes the log as input and produce logtype as key and the attached message as the value.
   */
    class Task4Mapper extends Mapper[Object, Text, Text, Text] {

      val word = new Text()
      val one = new Text()
      val loglist = List("ERROR", "DEBUG", "INFO", "WARN") /////

      override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

        val itr = value.toString
        val msg = itr.split(" - ",2)

        if(!(msg.length < 2)){
          val logtype = loglist.find(itr.contains).getOrElse("")

          val matched = regx.findFirstIn(msg(1))

          if (!logtype.isEmpty && itr.contains(logtype) && !matched.isEmpty) {
            word.set(logtype)
            one.set(msg(1))
            context.write(word, one)
          }
        }
      }
    }
  /**
   * Reducer takes the log type as the key and finds the max length of the message associated to that log type.
   */

      class Task4Reducer extends Reducer[Text,Text,Text,IntWritable] {

        val total = new IntWritable()

        def resursiveMax( values: Iterable[Text], max: Int): Int = {
          val v = values.iterator
          if(!v.hasNext){
            return max
          }
          val size = v.next().toString().length
          if (size > max){
            resursiveMax( values: Iterable[Text], size: Int)
          }else{
            resursiveMax( values: Iterable[Text], max: Int)
          }
        }

        override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {

          val max = resursiveMax(values, 0)

          total.set(max)
          context.write(key, total)
        }
      }


      def main(args: Array[String]): Unit = {
        val configuration = new Configuration
        configuration.set("mapred.textoutputformat.separator", ",")

        val job = Job.getInstance(configuration,"largest msg")
        job.setJarByClass(this.getClass)
        job.setMapperClass(classOf[Task4Mapper])
        job.setReducerClass(classOf[Task4Reducer])
        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[Text])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable]);
        FileInputFormat.addInputPath(job, new Path(args(0)))
        FileOutputFormat.setOutputPath(job, new Path(args(1)))
        System.exit(if(job.waitForCompletion(true))  0 else 1)
      }

  }
