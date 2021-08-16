package com.atguigu.mapreduce.joinn;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author smh
 * @create 2021-04-20 14:56
 */
public class JoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable> {

}
