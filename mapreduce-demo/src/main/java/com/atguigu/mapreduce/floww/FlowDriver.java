package com.atguigu.mapreduce.floww;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author smh
 * @create 2021-04-19 18:17
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        /*
        排序 对于输出输出的键值对来说是按照key来排序的,如果有特殊的需求,可以把排序标准设置成key
        要排序的key所属的类要实现writableComparable接口,在其中实现compareTo,在里面书写排序的逻辑
        排序方式有快排,归并排序
        快排:环形缓冲区数据达到百分之八十或者索引数组满的时候会发生溢写,溢写的时候会用快排对数据进行排序.先根据分区号进行排序,再根据key进行排序
        归并排序:如果溢写的文件有多个,那么会对溢写出的多个文件进行归并排序,把他们归并成一个文件,减少io
                在reduceTask阶段从各个mapTask那里复制本分区的数据文件时,会先存到内存里,如果内存里存的多了,就会写出到磁盘中.
                如果内存里或者磁盘里的文件数过多,就会分别对他们进行归并.
                归并完成后还会对内存,磁盘中归并得到的文件进行一次总的归并.
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定Combiner
        /*
        如果使用了Combiner,如果环形缓冲区溢写了多个文件,那么在快排之后就会先对他们进行一次合并.
        当多个溢写文件归并之后,再对他们进行一次合并.
        Combiner是处于Mapper和Reducer之间的一个组件,可以对Mapper输出的结果进行一个合并,减少网络传输
        Combiner是继承于Reducer类,也需要在里面重写reduce方法,将合并的逻辑写入.
        Combiner是一个可选的组件,使用它的要求是不能改变工作的逻辑,比如统计的时候可以使用Combiner合并数据,但是如果
        Reducer是求平均值,那么就不能使用Combiner合并数据
        Combiner的数据类型是MapTask的输出类型,同时也是Reducer的输入类型.
        每一个MapTask都会有一个对应的Combiner来处理MapTask输出的数据
        Combiner的操作逻辑一般与Reducer相同,所以在job驱动中指定Combiner的类的时候也可以直接使用Reducer来指定
         */
//        job.setCombinerClass(FlowReducer.class);


        //指定分区类,设置启动的reduceTask个数
        /*
        当mapTask中的数据输入到环形缓冲区之前,就会对它们进行分区.分区的结果会存到环形缓冲区的索引数组中.
        在溢写的时候,会先根据分区进行排序,再根据key进行排序.
        分区输入的k,v类型与mapTask的输出的k,v类型一致
        可以根据业务的需求将数据按照一定的条件进行分区输出
        如果没有指定分区方式的话,会默认的按照HashPartitioner的分区方式来进行分区(通过key的hashcode%分区数)
        用户可以自定义partitioner类,实现里面的抽象方法来指定分区的方式
        在job驱动中指定要按照哪个分区类进行分区(分区号从零开始),还要设置启动的ReduceTask数量,一般跟分区数相等
        如果启动的reduceTask个数等于一,则不会进行分区,所有的文件都会输出到一个文件中,默认的数量就为1
        如果大于分区数,则会产生一些空文件
        如果小于分区数,则会报错.
        如果启动的ReduceTask个数为0,那么就会没有reduce阶段,只有map阶段,而且没有shuffle阶段.
        这时输出的文件数量就等于启动的mapTask个数
         */
        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job,new Path("d:/phone_data .txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:/mapreduce/flow1"));

        job.waitForCompletion(true);
        /*
        关于job的提交:
            1.会先确定是走yarn还是走本地
            2.会确定输出的路径是否正确
            3.提交对应的文件:yarn(8个配置文件,切片信息,jar)
                            本地(8个配置文件,切片信息)
            4.根据切片信息启动对应数量的mapTask(每个mapTask执行一个切片)
         关于切片:
            1.切片并不是严格的按照块大小来切分的,而是有一个1.1倍的溢出值.当文件大于块大小*1.1的时候才会进行切块.
            2.切片数等于启动的mapTask数量.
            3.切片的大小是可以通过调整minSize和maxSize来设置的
         */
    }
}
/*
shuffle机制:
mapTask通过解析输入的k,v得到新的k,v之后,会将这些新得到的k,v输出到环形缓冲区中.
在输入到环形缓冲区之前,会先根据设置的partitioner对数据进行分区.
在环形缓冲区中,有两个数组,一个索引数组(1M),一个k,v数组(100M)
索引数组会存储输入的键值对的索引以及元标签(partition,keyStart,valStart,valLen),分别是分区信息,key开始的索引,value开始的索引,value的长度
k,v数组中存放的是读入的k,v序列化之后的字节.(挨个存放)
当环形缓冲区内的数据达到总容量的百分之八十,或者索引数组存满了,那么就会将里面存放的数据溢写到磁盘中,并再次过程中对数据进行排序.
关于排序,先根据分区,再根据key.排序并不是对k,v进行排序,而是对索引进行排序.(快排)
在溢写的同时,再往环形缓冲区内存放数据的时候,是从开始时的反方向开始存的.
每次溢写都会产生两个文件,一个里面存放了kv,另一个存放了索引(此时并不会根据分区将k,v分成多个文件)
如果溢写产生了多个文件,可以使用Combiner对他们根据key先进性合并.
合并之后会对多个文件进行归并排序将他们合并成一个文件.
合并之后还可以使用Combiner对他们进行合并.(压缩)
再之后将他们输出到磁盘中.
在Reduce阶段,Reduce会从各个MapTask中去复制本分区的数据.
复制的数据会先写到内存中,如果内存中的数据达到了一定的值,就会将剩下的数据写到磁盘中.
此时如果内存,磁盘中的文件数量分别达到了一定数量,会分别对他们进行归并.
对于归并之后的内存中,磁盘中的文件,会再进行一次总的归并.
归并后的文件会根据Key进行分组.
分组之后--->对分组后的k,v执行reduce方法.

 */