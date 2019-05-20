#   Hadoop-Practice

- log4j.propeties: 用于在终端显示运行过程中的报告

## HDFS读写和文件压缩
+ PutORget 
  + 本地多文件合并上传到HDFS
  + 云端多个文件合并下载到本地

+ ZipOpt
  + 压缩云端多个文件为一个文件，并下载到本地

+ SequenceFileOpt
  + 随机生成100个文本，每个文本包含10万条数据
  + 使用不同的压缩方式对文件进行压缩（BLOCK, NONE, RECODE）
  + 在不解压的情况下对文件中片段数据的查找

+ FirstMapReduce
  + 了解Mapper 和 Reducer 的函数构造，配置基本的job属性
