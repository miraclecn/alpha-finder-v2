# 2023-2025 双均线 + RSI / OBV 稳定超额研究

## 结论

- 在本轮约束下，**没有找到**一组 `双均线 + 最多两个附加指标（RSI / OBV）` 能在 `2023`、`2024`、`2025` 三个自然年都取得正主动收益。
- 最接近稳定的组合是：
  - `20/60 双均线 + OBV`，`双周换仓`
  - `2023 active = -0.98%`
  - `2024 active = -6.54%`
  - `2025 active = +24.69%`
  - 整段 `active annualized return = +5.15%`
  - `IR = 0.323`
  - `max drawdown = -29.27%`
  - `turnover = 26.76x`
- 失败主因非常集中：**2024 年**。不管是在 `CSI 800` 成分股内，还是在更宽的液态全 A 股池里，`2024` 都是系统性拖累年份。

## 研究设置

- 时间窗：`2023-01-03` 到 `2025-12-31`
- 数据：
  - `output/research_source.duckdb`
  - `output/csi800_benchmark_state_history_2020_20260428.json`
- 回测约束：复用仓库现有日频 portfolio backtester
  - `next_day_open`
  - `T+1`
  - 停牌约束
  - 涨跌停开盘锁死约束
  - lot size
  - 成本模型 `base_a_share_cash`
  - benchmark-relative 行业预算
- 组合结构：
  - 单 sleeve
  - `24` 只持仓上限
  - 等权候选信号后再交给现有 portfolio construction 做行业与单名约束
  - `20` 交易日持有目标
- 指标家族：
  - 双均线：`5/20`、`10/40`、`20/60`
  - 附加指标：
    - `RSI14`：仅作为 `50-70` 区间过滤
    - `OBV`：用 `20` 日 OBV 斜率 / `20` 日成交量归一后的 `obv_pressure20 > 0`
- 换仓频率：
  - `staggered_weekly`
  - `staggered_biweekly`

## 试验范围

### 1. `CSI 800` 成分股内搜索

共测试 `24` 组：

- `3` 组双均线
- `4` 种指标组合：
  - `ma_only`
  - `ma_rsi`
  - `ma_obv`
  - `ma_rsi_obv`
- `2` 种换仓频率

结果：

- `ALL_POSITIVE_COUNT = 0`

表现最好的几组如下：

| strategy | 2023 active | 2024 active | 2025 active | active ann. | IR | MDD | turnover |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ma_obv_20_60_staggered_biweekly` | `-0.98%` | `-6.54%` | `+24.69%` | `+5.15%` | `0.323` | `-29.27%` | `26.76x` |
| `ma_only_20_60_staggered_biweekly` | `+3.80%` | `-15.02%` | `+28.48%` | `+5.51%` | `0.315` | `-34.87%` | `24.70x` |
| `ma_obv_20_60_staggered_weekly` | `+0.75%` | `-13.06%` | `+18.57%` | `+2.06%` | `0.134` | `-31.75%` | `26.68x` |

观察：

- `20/60` 显著好于 `5/20`、`10/40`
- `OBV` 比 `RSI` 更有帮助
- `RSI` 过滤在这轮里基本都让结果变差
- 即便整段 IR 转正，按年看仍然过不了 `2024`

### 2. 更宽的液态全 A 股池复核

为排除 “只在 benchmark 内部选股太窄” 的影响，又额外测试了 `6` 组较优家族：

- `10/40` 与 `20/60`
- `ma_only` 与 `ma_obv`
- `weekly` / `biweekly`
- 额外流动性边界：
  - `20` 日成交额中位数 `>= 1.2e8`
  - `float_mcap >= 5bn`
  - `float_mcap <= 120bn`
  - 必须有 `sw2021_l1` PIT 行业标签

结果比 `CSI 800` 内搜索更差，仍然是 `2024` 明显失效：

| strategy | 2023 active | 2024 active | 2025 active | active ann. | IR |
| --- | ---: | ---: | ---: | ---: | ---: |
| `liquid_ma_only_20_60_staggered_biweekly` | `-9.83%` | `-33.45%` | `+10.37%` | `-11.14%` | `-0.545` |
| `liquid_ma_obv_20_60_staggered_biweekly` | `-6.93%` | `-41.01%` | `+16.37%` | `-12.08%` | `-0.603` |
| `liquid_ma_only_20_60_staggered_weekly` | `-9.32%` | `-31.48%` | `+1.70%` | `-12.66%` | `-0.645` |

## 解释

- 这类规则在 `2025` 的趋势延续环境下是有效的。
- 但 `2024` 更像“趋势容易形成、也更容易中途失败”的年份。
- 仅靠 entry 端再叠一层 `RSI` 或 `OBV` 过滤，不足以把 `2024` 的假突破 / 中继失败筛干净。
- 因此，问题大概率不在“少了一个入场指标”，而在：
  - 缺 regime gate
  - 缺 path-quality / failure-state 退出层
  - 或双均线本身对 `2024` 这类环境的结构适配度不够

## 直接建议

- 不建议把本轮任何一个双均线 + RSI / OBV 组合直接注册为新 sleeve。
- 如果继续做，优先顺序应是：
  1. 以 `20/60 + OBV` 为基线，单独研究 `2024` 失败样本
  2. 给它加 **regime overlay**，而不是继续堆 entry 指标
  3. 把 exit / state 管理接到已有 `price_volume_exit_state_study` 路径上验证
- 如果目标是“三年稳定超额”，下一轮不应再以“多试几个入场指标”为主，而应以“限制 2024 的回撤来源”为主。
