package com.atguigu.mapreduce.join;

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
public class JoinReducer extends Reducer<Text,JoinBean,JoinBean, NullWritable> {
    private ArrayList<JoinBean> list = new ArrayList<>();
    private JoinBean joinBean = new JoinBean();
    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
        list.clear();

        for (JoinBean value : values) {
            if(value.getFlag().contains("order")){
                JoinBean s = new JoinBean();
                try {
                    BeanUtils.copyProperties(s,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                list.add(s);
            }else{
                try {
                    BeanUtils.copyProperties(joinBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (JoinBean bean : list) {
            bean.setpName(joinBean.getpName());
            context.write(bean,NullWritable.get());
        }
    }
}
