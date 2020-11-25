import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.JavaConversions._

object PlayAverage  {

  class PlayTimeMapper extends Mapper[Object,Text,Text,LongWritable] {
    val one = new LongWritable(1)
    val word = new Text("PlayTime(Sec)")

    override
    def map(key:Object, value:Text, context:Mapper[Object,Text,Text,LongWritable]#Context): Unit = {
          val startTime = Calendar.getInstance()
          val endTime = Calendar.getInstance()
          val dataFormat = new SimpleDateFormat("yyyy/mm/dd HH:mm:ss")
          val dataSet = value.toString.split(",")
          startTime.setTime(dataFormat.parse(dataSet(4)))
          endTime.setTime(dataFormat.parse(dataSet(5)))
          val playTime: Long = (endTime.getTimeInMillis - startTime.getTimeInMillis) / 1000
          val values = new LongWritable(playTime)
          context.write(word,values)
        }
  }
  class PlayTimeReducer extends Reducer[Text,LongWritable,Text,LongWritable] {
    override
    def reduce(key:Text, values:java.lang.Iterable[LongWritable], context:Reducer[Text,LongWritable,Text,LongWritable]#Context): Unit = {
      var avg: Long = 0
      var sum: Long = 0
      var cnt: Long = 0
      for(value <- values) {
        sum += value.get()
        cnt += 1
      }
      avg = sum / cnt
      context.write(key, new LongWritable(avg))
    }
  }
  def main (args: Array[String]): Unit ={
    val conf = new Configuration()
    val job = new Job(conf, "word count")
    job.setJarByClass(classOf[PlayTimeMapper])
    job.setMapperClass(classOf[PlayTimeMapper])
    job.setCombinerClass(classOf[PlayTimeReducer])
    job.setReducerClass(classOf[PlayTimeReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])
    FileInputFormat.addInputPath(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path("output"))
    job.waitForCompletion(true)
  }
}
