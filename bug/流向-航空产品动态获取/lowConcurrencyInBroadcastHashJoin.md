# 问题
用户生产环境`BroadcastHashJoin`并发度非常低，无法快速完成计算。

# 背景
计算sql的样例如下，主要涉及 4 张表之间的计算(其中t4是一张逻辑表)，其分别具有以下特点：
t1: 具有海量数据的大表
t2、t3: 只有少量数据的普通表，数据量低于`BroadcastHashJoin`阈值
t4: 由t3开窗得到的逻辑表，数据不落盘，数据量低于`BroadcastHashJoin`阈值

```hiveql
select
    t_union.*
from (
    select * from t1
    union all
    select * from t1
     ) t_union
left join t2
    on t_union.f1 = t2.f1
left join (
    select
        *,
        row_number() over (fx partition by fy) rn
    from t3
    where t3.rn = 1
) t4
on t_union.f2 = t4.f2
```

# spark aqe开启时计算原理
1. 当无法从元数据直接推断出是否可以进行`BroadcastHashJoin`时，会先尝试进行`SortMergeJoin`
2. 如果运算过程中，某部分计算出来的结果集很小(低于`BroadcastHashJoin`阈值)，则会自动将Join转化为`BroadcastHashJoin`
3. 转化之前，由于先尝试`SortMergeJoin`，会触发对数据(参照joinKey)进行`shuffle`
4. 转换后，不会取消原理3中的`shuffle`，因此广播计算时，会使用`shuffle`后的数据进行运算

# 原因分析	
理论上，spark开启aqe之后，会自动根据`spark.sql.adaptive.advisoryPartitionSizeInBytes`进行数据切片，但是上述示例中，并发无法提升。
因此怀疑数据shuffle过程中，数据能做的切片太少。
对(具有海量数据的)`t1`用于join运算的key进行分析，检查key(f1, f2) hash之后的聚集程度。
```hiveql
select count(distinct(hash(f1))) cnt from t1;
```
结果表名，cnt数量很小，表名key的重合度太高。参考上述原理，key重合度太高，导致步骤3产生的partition数量很少，
因此，即使采用`BroadcastHashJoin`也无法按照预期`spark.sql.adaptive.advisoryPartitionSizeInBytes`大小
进行数据切片计算并发度。

# 解决方案
该查询能够全方位匹配`BroadcastHashJoin`，因此可以绕过`SortMergeJoin`被aqe优化成`BroadcastHashJoin`时带来的异常`shuffle`。
由此便可以根据`spark.sql.adaptive.advisoryPartitionSizeInBytes`直接触发`BroadcastHashJoin`，则可以达到理想的并行度。

## 如何绕过
将`t4`的数据直接持久化到临时表中，这样的话，构造执行计划的时候即可直接构造出不需要进行`shuffle`的执行计划，绕开上述问题。