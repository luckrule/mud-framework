# Benchmarks

`bench:logger` 测量 `safeLogger` 包装的每调用开销，对照直接调用同一个 no-op logger。附带的 `createJsonLogger` + 无 I/O sink 只作参照：它测的是 `Date` + `JSON.stringify` 的成本，不是包装成本。基准只测量，不做断言。

## 运行

```bash
bun run bench:logger
```

## 方法

- 每组先独立 warmup，warmup 不计入样本。
- 每个测量对按 ABBA 交替（`raw,safe,safe,raw`，下一对反向），配对差值 = 对内 safe 均值 - raw 均值；线性漂移在同一对内相互抵消。
- 报告 ns/call、ops/s，以及配对差值的中位数与范围。只有中位数与整个范围都大于 0 才报告成本与 ratio；否则输出 `not distinguishable from noise at this sample size`，不把负差值当成提速，也不换算成占比。
- no-op logger 在方法里累加计数，采样后校验计数与调用次数一致；计数不符说明调用被运行时消掉，基准直接失败而不是报告无意义的耗时。
- `child()` 的 raw 基线把返回值写到模块级变量，避免分配被优化掉，否则 raw 会塌缩成一次计数自增。

比较改动前后时，用同一台机器、同一 Bun 版本，看配对差值的中位数与范围是否稳定移动；跨机器的绝对值没有可比性。

结果不写入仓库文档：数字随机器、电源策略与运行时状态漂移，写死后会变成过时的性能承诺。正确性由测试与 CI 保证，基准不进门禁。
