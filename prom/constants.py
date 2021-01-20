Counter = 'Counter'
Gauge = 'Gauge'
Histogram = 'Histogram'
Summary = 'Summary'

MetricType = (
    (Counter, '只增不减的计数器'),
    (Gauge, 'Gauge类型的指标侧重于反应系统的当前状态,这类指标的样本数据可增可减'),
    (Histogram, '统计百分位'),
    (Summary, ''),
)