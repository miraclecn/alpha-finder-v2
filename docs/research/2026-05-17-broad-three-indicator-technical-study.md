# 2023-2025 广义三指标技术组合研究

## 范围修正

本研究用于修正上一轮过窄的解释。

- 用户原意不是“只能用双均线、RSI、OBV 这三样”
- 正确口径是：
  - `双均线` 算 `1` 类指标
  - 允许再叠加 `0-2` 类其他指标
  - 总数不超过 `3` 类
  - `截面排名 / z-score / 排序打分` 只是组合方法，不单独算一类技术指标

因此，这一轮不再局限于 `RSI` 和 `OBV`，而是把额外指标家族扩展为：

- `RSI`
- `OBV`
- `MACD`
- `trend_stability`
- `volume_control`

## 结论

- 在修正后的广义三指标搜索下，**仍然没有找到**一组能在 `2023`、`2024`、`2025` 三个自然年都取得正主动收益的策略。
- 搜索结果：
  - `ALL_POSITIVE_COUNT = 0`
- 说明问题不在于“上轮少试了 1-2 个技术指标”，而在于这类中期趋势跟随框架在 `2024` 这一年**结构性失效**。

## 研究设置

- 时间窗：`2023-01-03` 到 `2025-12-31`
- 数据：
  - `output/research_source.duckdb`
  - `output/csi800_benchmark_state_history_2020_20260428.json`
- 真实回测链路：
  - 复用仓库现有 `portfolio backtester`
  - `next_day_open`
  - `T+1`
  - 停牌
  - 涨跌停开盘锁
  - lot size
  - `base_a_share_cash`
  - benchmark-relative 行业预算
- 选股池：
  - 流动性和市值过滤后的全 A 股液态池
  - 非 ST
  - 非北京板
  - 有 `sw2021_l1` PIT 行业标签
- 基础约束：
  - `20` 日成交额中位数 `>= 1.2e8`
  - `float_mcap` 在 `5bn` 到 `120bn`
  - 上市天数 `>= 180`
- 持仓：
  - `24` 只
  - 单 sleeve
  - 等权候选后交给现有 construction 层做约束

## 搜索空间

基础指标家族：

- `dual MA`

额外指标家族：

- `RSI`
- `OBV`
- `MACD`
- `trend_stability`
- `volume_control`

组合规则：

- 每组策略 = `dual MA` + `0-2` 个额外家族

重点搜索对象：

- `10/40` 双均线，双周换仓
- `20/60` 双均线，周频换仓
- `20/60` 双均线，双周换仓

本轮总计测试 `48` 组真实组合。完整结果已落在本地：

- `.tmp/2026-05-17-focused-tech-combo-search.csv`

## 最好结果

按“最差年份主动收益”从高到低排序，前几组如下：

| strategy | 2023 active | 2024 active | 2025 active | min-year active | active ann. | IR |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ma+rsi+volume_control_20_60_staggered_biweekly` | `-6.60%` | `-26.74%` | `+5.00%` | `-26.74%` | `-8.94%` | `-0.523` |
| `ma+rsi+trend_stability_20_60_staggered_biweekly` | `-9.54%` | `-28.85%` | `+27.80%` | `-28.85%` | `-5.44%` | `-0.307` |
| `ma+rsi+trend_stability_20_60_staggered_weekly` | `-7.21%` | `-29.71%` | `+4.37%` | `-29.71%` | `-10.85%` | `-0.643` |
| `ma+rsi+obv_20_60_staggered_weekly` | `-7.54%` | `-29.86%` | `-6.95%` | `-29.86%` | `-14.40%` | `-0.913` |
| `ma+rsi+volume_control_10_40_staggered_biweekly` | `-5.96%` | `-29.88%` | `-10.16%` | `-29.88%` | `-14.61%` | `-0.924` |

## 观察

- `10/40` 这一支基本可以判定无效。
- `20/60` 明显强于 `10/40`，但仍然过不了 `2024`。
- `RSI + volume_control` 和 `RSI + trend_stability` 是相对最接近可用的附加组合。
- `OBV` 在这轮更宽搜索里没有延续上一轮在 benchmark 内部小范围试验时的相对优势。
- `MACD` 基本没有带来改善，很多组合反而更差。

## 关键判断

这轮结果说明：

- 允许更宽的技术指标家族后，结论没有逆转
- 所以问题不是“前一轮只试了 RSI / OBV，所以漏掉了真正的赢家”
- 问题更像是：
  - `2024` 的市场结构不适合这类中期均线趋势跟随
  - 或者 entry 端之外必须补更强的 regime / exit / failure-state 管理

## 下一步建议

- 不建议继续把主要精力放在“再换一个 entry 指标”上。
- 如果继续做，优先顺序应是：
  1. 固定 `20/60` 为基线
  2. 用 `rsi+volume_control` 或 `rsi+trend_stability` 作为最接近可用的候选
  3. 单独拆解 `2024` 失败样本
  4. 给策略加 `regime overlay`
  5. 或把退出管理接到已有 `price_volume_exit_state_study` 路径验证

- 更直接地说：
  - **靠再多加一个技术指标，无法把它变成 2023-2025 三年稳定超额策略。**
