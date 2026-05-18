# 2023-2025 开放指标组合真实回测续搜

## 结论

- 在继续放开指标家族之后，**这次找到了 4 组** 在 `2023`、`2024`、`2025` 三个自然年都取得正主动收益的组合。
- 最稳的一组是：
  - `20` 交易日换仓
  - `sector_relative_valuation + revenue_growth + volume_overheat_control`
  - `2023 active = +12.06%`
  - `2024 active = +5.02%`
  - `2025 active = +4.26%`
  - `active annualized return = +7.17%`
  - `IR = 0.643`
  - `max drawdown = -26.04%`
  - `turnover = 21.22x`

## 这轮为什么和上一轮不一样

上一轮“混合技术 + 基本面”真实验证里，最强基底是：

- `sector_relative_valuation + volume_overheat_control`

但它在 `2025` 年仍然是负主动收益。

这次继续往下找时，不再只在上一轮那几个基本面家族里打转，而是显式补进了**增长类**指标：

- `revenue_growth`

结果很清楚：

- 仅靠 `value + anti-overheat` 不够
- 补上 `growth` 后，组合第一次跨过了 `2023-2025` 三年都为正主动收益的门槛

## 研究设置

- 时间窗：`2023-01-03` 到 `2025-12-31`
- 数据：
  - `output/research_source.duckdb`
  - `output/csi800_benchmark_state_history_2020_20260428.json`
- 真实回测链路：
  - 仓库现有 `PortfolioBacktester`
  - `next_day_open`
  - `T+1`
  - 停牌
  - 涨跌停开盘锁
  - lot size
  - participation cap
  - `base_a_share_cash`
- 选股池过滤：
  - 非 ST
  - 非停牌
  - 非北京板
  - 有 `sw2021_l1` PIT 行业标签
  - 上市天数 `>= 180`
  - `20` 日成交额中位数 `>= 1.2e8`
  - `float_mcap` 在 `5bn` 到 `120bn`
- 持仓：
  - `24` 只
  - 单次最多每个行业 `3` 只
- 换仓步长：
  - `biweekly = 10` 个交易日
  - `monthly = 20` 个交易日
  - `quarterly = 40` 个交易日

## 指标定义

- `sector_relative_valuation`
  - 行业中性 `-log(pb)`
- `revenue_growth`
  - 以 `q_sales_yoy` 为主，缺失时回退到 `revenue_yoy`
  - 再做 `sign(x) * log(1 + |x|)` 压缩极端值
  - 行业中性标准化
- `volume_overheat_control`
  - `-log(max(volume_ratio_5, 1))`
  - 横截面标准化
- `dividend_yield`
  - `log(1 + dv_ttm)`
  - 行业中性标准化
- 其他测试过但未成为前三名的家族：
  - `profitability_quality`
  - `leverage_conservatism`
  - `balance_sheet_liquidity`
  - `medium_term_relative_strength`
  - `trend_stability`

## 真实回测结果

本轮总计真实验证 `60` 组组合，三年都为正主动收益的有 `4` 组：

| strategy | active ann. | IR | turnover | 2023 active | 2024 active | 2025 active | min-year active |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `monthly_sector_relative_valuation+revenue_growth+volume_overheat_control` | `+7.17%` | `0.643` | `21.22x` | `+12.06%` | `+5.02%` | `+4.26%` | `+4.26%` |
| `biweekly_sector_relative_valuation+revenue_growth` | `+6.40%` | `0.578` | `21.07x` | `+9.10%` | `+3.29%` | `+7.13%` | `+3.29%` |
| `quarterly_sector_relative_valuation+revenue_growth+volume_overheat_control` | `+6.84%` | `0.614` | `12.33x` | `+12.54%` | `+3.20%` | `+4.67%` | `+3.20%` |
| `biweekly_sector_relative_valuation+revenue_growth+volume_overheat_control` | `+9.06%` | `0.843` | `35.72x` | `+12.18%` | `+2.68%` | `+14.90%` | `+2.68%` |

## 小范围调权重复核

围绕最强家族又额外做了一轮小范围调权：

- `monthly value + growth + overheat`
- `biweekly value + growth`

结果：

- 没有比 `monthly value + growth + overheat` 更稳的新版本
- 最接近的是：
  - `biweekly value:0.75 + growth:1.25`
  - `min-year active = +3.89%`

因此当前最优结论不变：

- **首选：`20` 交易日换仓的 `估值 + 增长 + 成交过热控制`**

## 结果文件

- 主结果：
  - `.tmp/2026-05-17-open-combo-real-search.csv`
- 调权重复核：
  - `.tmp/2026-05-17-open-combo-weight-tilt-search.csv`
- 临时研究脚本：
  - `.tmp/2026-05-17-open-combo-real-search.py`

## 风险和下一步

- 这些组合虽然已经跨过“三年都正主动”的门槛，但 `2024` 仍然是最弱年份，最强组合也只有 `+5.02%`。
- 所以它更像“找到了可继续深挖的候选”，还不是可以直接定稿的生产 sleeve。
- 下一步应优先做：
  1. 把前四组扩展到 `2021-2025` 和 `2026 YTD` 复核
  2. 看主动收益是否主要来自少数行业阶段
  3. 拆解 `revenue_growth` 的真实贡献，确认不是单纯吃阶段性高景气 beta
