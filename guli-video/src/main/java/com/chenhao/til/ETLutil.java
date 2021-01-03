package com.chenhao.til;

/**
 * @author ChenHao
 * @create 2020-12-17 20:43
 */
public class ETLutil {

    /**
     * 1.过滤掉长度不够的，即小于9个字段的行
     * 2.去掉类别字符中的空格
     * 3.修改相关视频ID字段的分隔符，由'\t'替换为'&'
     * @param oriStr  输入参数，原始数据
     * @return  过滤后的数据
     */
    public static String etlStr(String oriStr) {
        StringBuffer sb = new StringBuffer();

        //1.切割
        String[] fields = oriStr.split("\t");

        //2.对字段长度进行判断
        if (fields.length < 9) {
            return null;
        }

        //3.去掉类别字段中的空格
        fields[3] = fields[3].replaceAll(" ","");

        //4.修改相关视频id字段的分隔符，由'\t'替换为'&'
        for (int i = 0; i < fields.length; i++) {
            if (i < 9) {    //将前9个字段append到sb中
                if (i == fields.length - 1) {   //如果这行数据只有9个字段，那最后一个字段后面不加\t
                    sb.append(fields[i]);
                }
                else {
                    sb.append(fields[i]).append("\t");
                }
            }
            //对相关视频id字段进行处理
            else {
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                }
                else {
                    sb.append(fields[i]).append("&");
                }
            }
        }

        //5.返回
        return sb.toString();
    }


}
