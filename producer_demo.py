# producer_demo.py
from kafka import KafkaProducer
import json
import time

# Kafka 服务器地址列表
bootstrap_servers = ['localhost:9092']
# 要发送到的 Kafka 主题
topic_name = 'my_python_topic'

# 创建生产者实例
# value_serializer: 指定消息值的序列化方式。这里我们将字典序列化为 JSON 字符串，然后编码为 UTF-8 字节。
try:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"成功连接到 Kafka: {bootstrap_servers}")
except Exception as e:
    print(f"连接 Kafka 失败: {e}")
    exit()

print(f"开始向主题 '{topic_name}' 发送消息...")

try:
    for i in range(10):
        message_data = {
            'message_id': i,
            'content': f'这是来自 Python 生产者的第 {i} 条消息',
            'timestamp': time.time()
        }

        # 发送消息
        # .send() 方法是异步的。它返回一个 Future 对象。
        future = producer.send(topic_name, value=message_data)
        print(f"发送消息: {message_data}")

        # 可选: 阻塞等待发送完成并获取元数据 (通常用于调试或确保关键消息发送)
        try:
            record_metadata = future.get(timeout=10) # 设置10秒超时
            print(f"  消息已发送 -> 主题: {record_metadata.topic}, "
                  f"分区: {record_metadata.partition}, "
                  f"偏移量: {record_metadata.offset}")
        except Exception as e:
            print(f"  发送消息失败或超时: {e}")

        time.sleep(1) # 每秒发送一条

except KeyboardInterrupt:
    print("\n用户中断发送。")
except Exception as e:
    print(f"发送过程中发生错误: {e}")
finally:
    if 'producer' in locals() and producer:
        # 确保所有缓冲区的消息都被发送出去
        print("正在刷新生产者缓冲区...")
        producer.flush(timeout=10) # 等待最多10秒
        # 关闭生产者连接
        print("正在关闭生产者...")
        producer.close()
        print("生产者已关闭。")
