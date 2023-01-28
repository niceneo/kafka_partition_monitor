import requests
import json
import logging
from kafka import KafkaAdminClient
from kafka.admin import NewPartitions

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
loghandler=logging.FileHandler("kafka_partition_monitor.log")
formatter=logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
loghandler.setFormatter(formatter)
logger.addHandler(loghandler)


address='http://x.x.x.x:9090'
expr={
    # uat
    # '1.1.1.1:9092': 'query=kafka_topic_partitions{environment="uat",id="xxxxx",topic!~"A.A.*|.*checkpoints.internal.*|.*heartbeats.*|mm2-.*"} - on(topic) kafka_topic_partitions{environment="uat",id="xxxxxx",topic!~"A.A.*|.*heartbeats.*|.*checkpoints.internal.*|mm2-.*"} > 0',
}
# 查询对比差异
def query(address:str,expr:dict):
    try:
        url = address + '/api/v1/query?' + expr
        return json.loads(requests.get(url=url).content.decode('utf8','ignore'))
    except Exception as e:
        raise e

def kafka_add_partition(bootstrap_servers:str,data:dict):
    try:
        client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        for topic, add_P_Num in data.items():
            # query now topic partitions num
            query_result=client.describe_topics(topics=[topic])
            n=0
            for i in query_result:
                if i['topic'] == topic:
                    n=len(i['partitions'])
            # alter topic partitions n+num
            NewP=NewPartitions(total_count=n+add_P_Num)
            args={
                topic:NewP
            }
            logger.info(topic + ": Partitions Num " + str(n) + "+" + str(add_P_Num) )
            # logger.info(args)
            client.create_partitions(args)
        client.close()
    except Exception as e:
        logger.error(e)

def parse_query_result(result:dict):
    try:
        data={}
        for i in result['data']['result']:
            if int(i['value'][1]) <= 50:
                data[i['metric']['topic']]=int(i['value'][1])
        return data
    except Exception as e:
        logger.error(e)
def Do():
    try:
        logger.info("running")
        for k,v in expr.items():
            result=query(address,v)
            logger.info(result)
            if result['status'] == 'success':
                data=parse_query_result(result)
                logger.info(data)
                kafka_add_partition(bootstrap_servers=k,data=data)
    except Exception as e:
        logger.error(e)

if __name__ == '__main__':
    Do()
