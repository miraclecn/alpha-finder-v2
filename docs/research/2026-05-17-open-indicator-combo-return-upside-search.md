# 2023-2025 开放指标组合收益提升续搜

## 结论

- 你这句“收益率不够好”是对的。
- 在上一轮已经找到“三年都正主动收益”的前提下，继续往下搜以后，**收益最明显提升**的方向不是继续微调 `value + growth + overheat`，而是：
  - 把主信号切到 `revenue_growth + dividend_yield`
  - 再把持仓数从 `24` 压到 `16-20`

当前最好结果：

- `biweekly_h20_revenue_growth+dividend_yield`
- `active annualized return = +13.55%`
- `IR = 1.416`
- `turnover = 22.93x`
- `2023 active = +19.52%`
- `2024 active = +11.91%`
- `2025 active = +16.15%`

这已经明显高于上一轮最好的稳定组合。

## 本轮搜索做了什么

沿用同一套真实回测约束：

- `PortfolioBacktester`
- `next_day_open`
- `T+1`
- 停牌 / 开盘涨跌停锁 / lot size / participation cap
- `base_a_share_cash`

不改数据边界，只新增两个维度：

1. 扩展高收益候选家族：
   - `sector_relative_valuation`
   - `revenue_growth`
   - `dividend_yield`
   - `volume_overheat_control`
2. 搜持仓集中度：
   - `12`
   - `16`
   - `20`
   - `24`

总计真实验证 `132` 组。

结果文件：

- `.tmp/2026-05-17-open-combo-holdingcount-search.csv`

## 最高收益稳定组合

按“必须 2023/2024/2025 三年都正主动收益”筛选后，收益最好的组合如下：

| strategy | active ann. | IR | turnover | 2023 active | 2024 active | 2025 active |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `biweekly_h20_revenue_growth+dividend_yield` | `+13.55%` | `1.416` | `22.93x` | `+19.52%` | `+11.91%` | `+16.15%` |
| `biweekly_h16_revenue_growth+dividend_yield` | `+13.38%` | `1.363` | `21.16x` | `+21.31%` | `+13.15%` | `+12.24%` |
| `biweekly_h12_revenue_growth+dividend_yield` | `+12.23%` | `1.167` | `21.09x` | `+24.52%` | `+9.56%` | `+7.64%` |
| `quarterly_h16_revenue_growth+dividend_yield` | `+11.67%` | `1.183` | `9.02x` | `+20.88%` | `+10.23%` | `+9.46%` |
| `monthly_h24_revenue_growth+dividend_yield` | `+11.40%` | `1.240` | `15.17x` | `+16.89%` | `+7.76%` | `+15.23%` |
| `monthly_h20_revenue_growth+dividend_yield` | `+11.36%` | `1.215` | `15.53x` | `+19.09%` | `+9.90%` | `+10.29%` |

## 如果必须保留技术类指标

如果你不满足于“高收益，但主要来自基本面双因子”，而是希望至少保留一个明显技术 / 成交约束分支，那么当前最好的是：

| strategy | active ann. | IR | turnover | 2023 active | 2024 active | 2025 active |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `biweekly_h24_sector_relative_valuation+revenue_growth+volume_overheat_control` | `+9.06%` | `0.843` | `35.72x` | `+12.18%` | `+2.68%` | `+14.90%` |
| `quarterly_h20_revenue_growth+volume_overheat_control` | `+8.93%` | `0.569` | `12.28x` | `+4.31%` | `+12.96%` | `+10.86%` |
| `biweekly_h16_sector_relative_valuation+revenue_growth+volume_overheat_control` | `+8.75%` | `0.804` | `31.50x` | `+13.02%` | `+2.54%` | `+14.46%` |

结论也很清楚：

- 想要更高收益，**纯基本面 `growth + dividend` 目前明显强于加技术过热约束的版本**
- 但如果必须要跨类别组合，`growth + overheat` 或 `value + growth + overheat` 仍然是当前最像样的实盘候选

## 观察

- `24 -> 20 / 16` 的集中化，确实能把收益抬起来。
- 但集中到 `12` 以后，很多组合的稳定性反而下降，说明再继续压持仓数不是免费的。
- `revenue_growth` 是这轮最关键的收益驱动。
- `dividend_yield` 在这里不是简单“低估值替代品”，它和 `growth` 拼起来的效果明显强于和 `value` 拼。

## 当前判断

如果目标是：

- **先要高收益，再要求三年都稳**

那么当前第一优先级应改成：

1. `revenue_growth + dividend_yield`
2. 持仓数 `16-20`
3. 双周或月频

如果目标是：

- **必须保留明显技术类指标**

那么当前第一优先级仍是：

1. `sector_relative_valuation + revenue_growth + volume_overheat_control`
2. 或 `revenue_growth + volume_overheat_control`

## 下一步

- 这批高收益候选现在最需要做的不是再加因子，而是：
  1. 扩到 `2021-2025` 与 `2026 YTD`
  2. 看收益是不是过度集中在少数行业阶段
  3. 看 `growth + dividend` 是否隐含了过强的风格暴露
