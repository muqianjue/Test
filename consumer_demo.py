# consumer_demo.py
from kafka import KafkaConsumer
import json

# Kafka 服务器地址列表
bootstrap_servers = ['localhost:9092']
# 要订阅的 Kafka 主题
topic_name = 'my_python_topic'
# 消费者组 ID
# 同一个组内的消费者会分摊主题分区的消息。
# 如果多个消费者实例使用相同的 group_id 订阅同一个主题，那么每条消息只会被组内的一个消费者处理。
consumer_group_id = 'my_python_consumer_group'

print(f"尝试连接到 Kafka: {bootstrap_servers}，订阅主题: '{topic_name}', 消费者组: '{consumer_group_id}'")

try:
    consumer = KafkaConsumer(
        topic_name, # 可以是单个主题字符串，或主题列表/元组
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group_id,
        # auto_offset_reset: 当消费者组没有初始偏移量，或者当前偏移量在服务器上不再存在时，如何处理。
        # 'earliest': 从最早的可用消息开始消费。
        # 'latest': 从最新的消息开始消费 (即只消费启动后产生的新消息)。
        auto_offset_reset='earliest',
        # value_deserializer: 指定消息值的反序列化方式。这里我们将 UTF-8 字节解码为字符串，然后用 JSON 解析为字典。
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        # consumer_timeout_ms: 如果设置为一个毫秒数，当 इतने समय तक कोई नई संदेश नहीं मिलता है तो उपभोक्ता का पुनरावृत्ति रुक जाएगा।
        # 如果不设置或设为负数，则 for 循环会一直阻塞等待新消息。
        # consumer_timeout_ms=10000 # 例如10秒超时
    )
    print("成功连接并订阅。等待接收消息...")

    for message in consumer:
        # message 对象包含了很多有用的信息
        print(f"\n收到消息:")
        print(f"  主题 (Topic): {message.topic}")
        print(f"  分区 (Partition): {message.partition}")
        print(f"  偏移量 (Offset): {message.offset}")
        print(f"  消息键 (Key): {message.key}") # 如果生产者发送了 key
        print(f"  消息值 (Value): {message.value}") # 经过反序列化后的值
        print(f"  时间戳 (Timestamp): {message.timestamp} (类型: {message.timestamp_type})")

except KeyboardInterrupt:
    print("\n用户中断消费。")
except Exception as e:
    print(f"消费过程中发生错误或连接失败: {e}")
finally:
    if 'consumer' in locals() and consumer:
        # 关闭消费者连接
        # 默认情况下，消费者会自动提交偏移量。关闭时也会尝试提交。
        print("正在关闭消费者...")
        consumer.close()
        print("消费者已关闭。")
