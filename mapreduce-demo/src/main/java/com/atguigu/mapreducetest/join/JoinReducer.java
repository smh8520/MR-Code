package com.atguigu.mapreducetest.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author smh
 * @create 2021-04-22 14:20
 */
public class JoinReducer extends Reducer<Text,JoinBean,JoinBean, NullWritable> {
    private JoinBean joinBean = new JoinBean();
    private ArrayList<JoinBean> list = new ArrayList<>();
    @Override
    protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {
        list.clear();
        for (JoinBean value : values) {
            if(value.getFlag().contains("order")){
                JoinBean bean = new JoinBean();
                try {
                    BeanUtils.copyProperties(bean,value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                list.add(bean);
            }else{
                try {
                    BeanUtils.copyProperties(joinBean,value);
                } catch (Exception e) {
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
