package com.study.dml;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/9 -- 20:34
 * @Description: com.study
 * @version: 1.0
 */
public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties kp = new Properties();
        kp.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"node02:9092,node03:9092,node04:9092");
        // 创建kafkaAdminClient
        AdminClient adminClient = KafkaAdminClient.create(kp);
        // 注 默认创建topic是异步的 需采用同步方法
//         CreateTopicsResult topic01 = adminClient.createTopics(Arrays.asList(new NewTopic("topic03", 3, (short) 3)));
//         topic01.all().get(); // 同步方法

        // 删除topic
//        DeleteTopicsResult topic02 = adminClient.deleteTopics(Arrays.asList("topic02"));
//        topic02.all().get();

        // 查看topic 详细信息
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("topic04"));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry : stringTopicDescriptionMap.entrySet()) {
            System.out.println("key:"+entry.getKey());
            System.out.println("value:"+entry.getValue());

        }

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> names = listTopicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }


        // 关闭AdminClient
        adminClient.close();

    }
}
