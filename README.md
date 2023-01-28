# kafka_partition_monitor

# 两套kafka做同步完，假如一套kafka topic的分区数进行了扩容，常常忘记另外一台也需要同步进行分区数扩容
# 此脚本通过prometheus 对比两套kafka的topic的分区数，另外一台进行自动扩容。
# kafka分区数只能扩，不能缩
