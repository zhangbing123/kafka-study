package com.zb.kafka.client;

import com.alibaba.fastjson.JSON;
import com.zb.kafka.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Producer {

    private final static String TOPIC_NAME = "my-replicated-topic";

    public static void main(String[] args)  {
        //创建配置对象
        Properties properties = new Properties();
        //配置kafka服务端地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.115.131:9092,192.168.115.132:9092,192.168.115.133:9092");
        /*
         发出消息持久化机制参数
        （1）acks=0： 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。性能最高，但是最容易丢消息。
        （2）acks=1： 至少要等待leader已经成功将数据写入本地log，但是不需要等待所有follower是否成功写入。就可以继续发送下一
             条消息。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
        （3）acks=-1或all： 需要等待 min.insync.replicas(默认为1，推荐配置大于等于2) 这个参数配置的副本个数都成功写入日志，这种策略
            会保证只要有一个备份存活就不会丢失数据。这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
         */
        //properties.put(ProducerConfig.ACKS_CONFIG, "1");
         /*
        发送失败会重试，默认重试间隔100ms，重试能保证消息发送的可靠性，但是也可能造成消息重复发送，比如网络抖动，所以需要在
        接收者那边做好消息接收的幂等性处理
        配置重试3次
        */
        //properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        //重试时间间隔 单位ms
        //properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,300);

        //把发送的key从字符串序列化为字节数组
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //创建生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        int msgNum = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(msgNum);
        for (int i = 1; i <= msgNum; i++) {

            Order order = new Order(i, 100 + i, 1, 1000.00);
//            Order order = new Order(1, 100, 1, 1000.00);

            //未指定发送分区，具体发送的分区计算公式：hash(key)%partitionNum
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, order.getOrderId().toString(), JSON.toJSONString(order));

            //等待消息发送成功的同步阻塞方法
            RecordMetadata metadata = null;
                try {
                    metadata = producer.send(producerRecord).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
                    + metadata.partition() + "|offset-" + metadata.offset());

        }

        try {
            countDownLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
