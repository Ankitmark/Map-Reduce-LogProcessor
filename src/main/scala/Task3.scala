

import java.lang.Iterable
import java.util.StringTokenizer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.collection.JavaConverters._

/**
 * Produce the number of the generated log messages for each message type in the log.
 */

class Task3
 object Task3 {

   /**
    * Take the log and as input produce the message type as key and one value
    */
   class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

     val one = new IntWritable(1)
     val word = new Text()

     override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
       val itr = new StringTokenizer(value.toString)
       val m1 = List("ERROR", "DEBUG", "INFO", "WARN") ////
       while (itr.hasMoreTokens()) {  ///
         val tokan = itr.nextToken()
         if(m1.contains(tokan)){
           word.set(tokan)
           context.write(word, one)
         }
       }
     }
   }

   /**
    * Take message type as key and produce the sum of the number of message for each message type as value.
    */


  class IntSumReader extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")

    val job = Job.getInstance(configuration,"log count")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if(job.waitForCompletion(true))  0 else 1)
  }

}