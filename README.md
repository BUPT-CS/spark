# spark
此项目是 Spark 的实例包，包中含有 item-based协同过滤和user-based协同
过滤的实现。

数据批量导入redis 和 HBase 数据库。

Spark streaming应用实例

spark 的mllib 包中简单实现facebook 的 GBDT + LR 的 ctr 预估算法，将数据特征使用gbdt进行非线性转化，
再将转换之后的特征作为 LR 的输入进行训练，最终得到预估结果。
