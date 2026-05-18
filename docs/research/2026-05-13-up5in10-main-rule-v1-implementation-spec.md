# up5in10 主规则 v1 实现规格

这份文档不是研究总结，而是给“另一个平台上的 LLM / 工程师”直接实现代码用的规格说明。

目标：尽可能准确复刻当前仓库里的 `up5in10` 基线主规则，不包含接回模块，不叠加旧策略，不混入其他袖套。

## 1. 策略对象

- 市场：A 股
- 频率：日频
- 信号时点：收盘后生成
- 执行时点：下一交易日开盘买入
- 策略类型：独立短周期选股 + 动态止盈
- 研究对象链路：
  - `候选日 -> path score 打分 -> top10 -> 120日位置过滤 -> 组合执行`

## 2. 主规则最终版本

这是当前应被视为“基线”的固定版本：

- 股票池：主板股票
- 排除：
  - `ST`
  - 停牌
  - 北京交易所
- 日信号排序：
  - 每个交易日按 `path_score` 从高到低排序
  - 保留前 `10` 只
- 价格位置过滤：
  - 只保留 `120日区间位置 >= 0.10 且 < 0.35`
- 组合规则：
  - 最多同时持有 `10` 只
  - 单只目标仓位 `15%`
  - 无杠杆
  - 不设固定止盈
  - 不设最大持仓天数
- 卖出规则：
  - 硬止损 `6%`
  - 浮盈达到 `15%` 后启动动态止盈
  - 动态止盈回撤阈值 `3%`
- 交易约束：
  - `T+1`
  - 买入成本 `12bp`
  - 卖出成本 `12bp`
  - 100 股整手

## 3. 必要输入字段

如果要在别的平台实现，至少需要这些日线字段：

- `security_id`
- `trade_date`
- `board`
- `is_st`
- `is_suspended`
- `open`
- `high`
- `low`
- `close`
- `open_adj`
- `high_adj`
- `low_adj`
- `close_adj`
- `turnover_value_cny`

当前仓库的研究和回测都使用：

- 价格列：`open_adj / high_adj / low_adj / close_adj`
- 成交额列：`turnover_value_cny`

如果你的平台没有完全同名字段，使用等价字段即可，但要保证：

- 价格序列前后一致，不能一会儿前复权一会儿不复权
- 成交额口径在全样本内一致
- 回测买卖触发都用同一套价格口径

## 4. 两个阶段

实现时最好分成两个阶段：

1. 离线训练 / 固定参数阶段
2. 每日实盘信号生成阶段

主规则当前是“固定 `2022-2025` 训练规格，然后向外应用”的做法。

也就是说：

- 训练年份：`2022, 2023, 2024, 2025`
- 线上或样本外应用年份：`2021, 2026`，以及你后续新增年份

如果你只是想复刻当前基线，不必每年重训，可以直接使用第 8 节里的冻结系数。

## 5. 候选日特征构造

### 5.1 基础窗口

- 回看窗口：`60` 个交易日
- 成交额基准窗口：`20` 个交易日
- 未来观察窗口：`30` 个交易日

说明：

- `30` 日未来窗口只用于研究和训练标签
- 实盘实时生成信号时，不会用到任何未来数据

### 5.2 候选样本的基础约束

对每只股票、每个交易日 `t`：

- 需要至少有：
  - 前 `60` 个交易日价格历史
  - 前 `20` 个交易日成交额历史
- 股票必须满足：
  - `board == main_board`
  - 非 `ST`
  - 非停牌
  - 成交额有效且大于 `0`

### 5.3 成交额相对比率

定义：

```text
turnover_median20(t) = 前20个交易日成交额中位数，不含 t
turnover_ratio(t) = turnover_value_cny(t) / turnover_median20(t)
```

### 5.4 单日涨跌分类阈值

- `PRICE_UP = +1%`
- `PRICE_DOWN = -1%`
- `EXPAND_THRESHOLD = 1.3`
- `CONTRACT_THRESHOLD = 0.8`

先定义单日收益：

```text
daily_ret1(t) = close_adj(t) / close_adj(t-1) - 1
```

再定义单日量价状态：

- `expand_up`:
  - `turnover_ratio >= 1.3`
  - 且 `daily_ret1 > 0.01`
- `expand_down`:
  - `turnover_ratio >= 1.3`
  - 且 `daily_ret1 < -0.01`
- `expand_flat`:
  - `turnover_ratio >= 1.3`
  - 且 `-0.01 <= daily_ret1 <= 0.01`
- `contract_up`:
  - `turnover_ratio <= 0.8`
  - 且 `daily_ret1 > 0.01`
- `contract_down`:
  - `turnover_ratio <= 0.8`
  - 且 `daily_ret1 < -0.01`
- `contract_flat`:
  - `turnover_ratio <= 0.8`
  - 且 `-0.01 <= daily_ret1 <= 0.01`
- 其他情况：
  - `neutral`

### 5.5 三段窗口特征

候选日 `t` 之前的 `60` 个交易日被拆成三段：

- `early`: `t-60 ~ t-31`
- `late`: `t-30 ~ t-1`
- `launch_pad`: `t-10 ~ t-1`

对每一段都计算：

- 平均收益率
- 平均 `turnover_ratio`
- 六类状态占比：
  - `expand_up_rate`
  - `expand_down_rate`
  - `expand_flat_rate`
  - `contract_up_rate`
  - `contract_down_rate`
  - `contract_flat_rate`
- 上涨日 / 下跌日 / 横盘日对应的平均 `turnover_ratio`

当前主规则最终打分只直接使用最后 `10` 天的压缩特征，不直接用全部三段特征；但候选构造层仍保留这些字段。

## 6. path score 的训练标签与训练样本

### 6.1 训练标签

定义候选日 `t` 后未来 `10` 个交易日最高价是否先达到 `+5%`：

```text
hit5 = 未来10个交易日最高 high_adj >= close_adj(t) * 1.05
```

当前 path score 训练就是在预测这个 `hit5`。

### 6.2 path score 只看候选日前最后 30 天里的最后 10 天

`path_score` 相关特征来自候选日前 `30` 日 lookback，真正使用的是相对日 `-10 ~ -1` 的压缩统计。

这里的 `relative_day` 定义是：

```text
relative_day = lookback_day_position - candidate_day_position
```

因此：

- `relative_day = -1` 表示候选日前 1 天
- `relative_day = -10` 表示候选日前 10 天

## 7. path score 公式

### 7.1 四个原始特征

对候选日 `t`，取 `relative_day in [-10, -1]`：

1. `mean_turnover10`

```text
最后10天 turnover_ratio 的平均值
```

2. `contract_flat_rate10`

```text
最后10天中，pv_state == contract_flat 的占比
```

3. `expand_up_persist`

```text
最后10天内，连续两天都是 expand_up 的次数
```

4. `down_to_up`

```text
最后10天内，前一天是 expand_down、当天是 expand_up 的次数
```

### 7.2 标准化

只在训练集 `2022-2025` 主板样本上做：

- 对每个特征先做 1% / 99% 分位裁剪
- 再计算训练集均值和标准差
- 转成 z-score

```text
z = (clip(x, clip_low, clip_high) - mean) / std
```

### 7.3 path score 最终公式

```text
path_score
= 1.0 * z(mean_turnover10)
- 1.0 * z(contract_flat_rate10)
+ 1.0 * z(expand_up_persist)
+ 0.5 * z(down_to_up)
```

## 8. 当前冻结版 path score 参数

如果你不想在别的平台重训，直接用这组固定参数：

训练集：

- 主板
- 年份：`2022-2025`
- 样本数：`2,755,326`

固定参数：

| feature | clip_low | clip_high | mean | std | weight |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mean_turnover10` | `0.481695` | `5.768339` | `1.290802` | `0.827952` | `+1.0` |
| `contract_flat_rate10` | `0.000000` | `0.700000` | `0.189459` | `0.174909` | `-1.0` |
| `expand_up_persist` | `0.000000` | `6.000000` | `1.081470` | `1.427022` | `+1.0` |
| `down_to_up` | `0.000000` | `3.000000` | `0.526266` | `0.780123` | `+0.5` |

当前基线使用的分位阈值：

- `p80 threshold = 1.6686971252545528`

备注：

- `p90 = 3.2585133171383855`
- 但当前主规则不用 `p90`

## 9. 日信号生成

### 9.1 候选打分

对某个交易日 `t` 的所有主板候选：

- 计算四个 `path score` 特征
- 用第 8 节固定参数转成 `path_score`

### 9.2 第一层筛选：分位阈值

只保留：

```text
path_score >= 1.6686971252545528
```

### 9.3 第二层筛选：日内 top10

同一交易日内：

- 按 `path_score` 降序
- 若分数相同，按 `security_id` 升序
- 取前 `10` 只

## 10. 120 日区间位置过滤

### 10.1 定义

对候选日 `t`：

```text
rolling_high_120 = 过去120个交易日 high_adj 的最大值，包含 t
rolling_low_120  = 过去120个交易日 low_adj 的最小值，包含 t

range_pos_120 = (close_adj(t) - rolling_low_120) / (rolling_high_120 - rolling_low_120)
```

若分母为 `0`，则该值记为空，不入选。

### 10.2 主规则过滤条件

只保留：

```text
0.10 <= range_pos_120 < 0.35
```

说明：

- 这是主规则最关键的稳定过滤器
- `250` 日位置只做观察，不进入当前基线逻辑

## 11. 最终信号定义

某只股票在交易日 `t` 成为最终买入信号，当且仅当：

1. 是主板
2. 非 `ST`
3. 非停牌
4. 有足够历史数据
5. `path_score >= p80`
6. 位列当日 `top10`
7. `0.10 <= range_pos_120 < 0.35`

这些信号是在 `t` 日收盘后生成，并在 `t+1` 日开盘尝试买入。

## 12. 组合执行规则

### 12.1 调仓频率

- 日频
- 每个信号日收盘后生成新候选
- 下一交易日开盘统一尝试建仓

### 12.2 持仓数量与仓位

- 最多持有 `10` 只
- 默认单只目标仓位 `15%`
- 目标金额按“当日开盘前组合权益”计算

```text
target_position_value = portfolio_value_before_entries * 0.15
```

### 12.3 买入价格

```text
entry_price = next_trade_day.open_adj
```

### 12.4 股数约束

- 100 股整手
- 向下取整

```text
shares = floor(gross_budget / entry_price / 100) * 100
```

### 12.5 成本

- 买入成本：`12bp`
- 卖出成本：`12bp`

```text
buy_cost_rate  = 12 / 10000 = 0.0012
sell_cost_rate = 12 / 10000 = 0.0012
```

### 12.6 现金和跳过规则

信号在下列情况会被跳过：

- 当前持仓数已经达到 `10`
- 该股票已经在持仓中
- 开盘价无效
- 现金不足以买入至少 100 股

## 13. 卖出规则

### 13.1 T+1

买入当天不能卖。

代码等价判断：

```text
holding_days = current_trade_index - entry_trade_index
if holding_days <= 0:
    不允许卖出
```

### 13.2 硬止损

从第一个可卖交易日起：

```text
hard_stop_price = entry_price * (1 - 0.06)
```

若当日：

```text
low_adj <= hard_stop_price
```

则按：

```text
exit_price = hard_stop_price
```

卖出，原因记为：

- `hard_stop_loss`

### 13.3 动态止盈启动

持仓内维护两个状态：

- `peak_price`
- `trailing_active`

初始化：

- `peak_price = entry_price`
- `trailing_active = False`

每个可卖交易日：

1. 先检查硬止损
2. 再检查是否已经激活动态止盈后的回撤卖出
3. 最后才更新当日最高价并判断是否激活动态止盈

这个顺序很重要，因为当前实现明确避免假设“同一天先冲高再回落止盈”。

### 13.4 动态止盈激活条件

如果当日最高价更新后满足：

```text
peak_price / entry_price - 1 >= 0.15
```

则：

```text
trailing_active = True
```

### 13.5 动态止盈卖出条件

若 `trailing_active == True`，则：

```text
trailing_stop_price = peak_price * (1 - 0.03)
```

若当日：

```text
low_adj <= trailing_stop_price
```

则按：

```text
exit_price = trailing_stop_price
```

卖出，原因记为：

- `dynamic_trailing_stop`

### 13.6 数据结束强制平仓

只有在回测数据最后一天，才允许按收盘价强制平仓：

```text
exit_price = close_adj(final_trade_date)
exit_reason = forced_final_close
```

当前基线：

- 不做年末强制平仓
- 不做固定持仓天数到期卖出

## 14. 回测输出口径

单笔交易收益：

```text
gross_ret = exit_price / entry_price - 1
```

净收益：

```text
net_ret =
    (gross_exit_value - sell_cost)
    / (gross_entry_value + buy_cost)
    - 1
```

组合暴露：

```text
gross_exposure = position_value / portfolio_value
```

## 15. 实现伪代码

### 15.1 训练固定参数

```python
train_years = [2022, 2023, 2024, 2025]

events = build_main_board_event_features(train_years)

for feature in ["mean_turnover10", "contract_flat_rate10", "expand_up_persist", "down_to_up"]:
    clip_low  = quantile(feature, 0.01)
    clip_high = quantile(feature, 0.99)
    x_clip    = clip(feature, clip_low, clip_high)
    mean      = mean(x_clip)
    std       = std(x_clip)

p80 = quantile(path_score(train_events), 0.80)
```

### 15.2 每日信号

```python
for trade_date in dates:
    candidates = all_main_board_candidates(trade_date)

    for stock in candidates:
        stock.path_score = score(stock, frozen_spec)

    filtered = [x for x in candidates if x.path_score >= p80]
    ranked = sort(filtered, key=[-path_score, security_id])
    top10 = ranked[:10]

    for stock in top10:
        stock.range_pos_120 = calc_range_pos_120(stock, trade_date)

    final_signals = [
        x for x in top10
        if 0.10 <= x.range_pos_120 < 0.35
    ]
```

### 15.3 次日开盘建仓

```python
entry_date = next_trade_date(signal_date)

for signal in final_signals_of_signal_date:
    if current_positions >= 10:
        skip
    if signal.security already held:
        skip

    target_value = portfolio_value_before_entries * 0.15
    gross_budget = min(target_value, cash / 1.0012)
    shares = floor(gross_budget / open_price / 100) * 100

    if shares <= 0:
        skip

    buy at open_price
```

### 15.4 每日卖出

```python
for position in positions:
    if holding_days <= 0:
        continue

    if low <= entry_price * 0.94:
        sell at entry_price * 0.94
        continue

    if trailing_active and low <= peak_price * 0.97:
        sell at peak_price * 0.97
        continue

    peak_price = max(peak_price, high)
    if peak_price / entry_price - 1 >= 0.15:
        trailing_active = True
```

## 16. 不要改的关键点

如果想复刻当前基线，下面这些点不要擅自改：

- 不要把主板扩成全市场
- 不要把 `p80` 改成 `p90` 或别的阈值
- 不要把 `top10` 改成 `top5`
- 不要把 `120日位置 10%-35%` 改成 `250日位置 10%-35%`
- 不要把 `6%` 硬止损改成 `8%`
- 不要把 `15%` 启动阈值改成 `20%`
- 不要把 `3%` 动态回撤改成 `5%`
- 不要把动态止盈写成“同一天先创新高再按回撤卖”
- 不要忽略 `T+1`
- 不要忽略 100 股整手
- 不要忽略双边 `12bp + 12bp`

## 17. 另一个平台最常见的实现错误

### 错误 1

把 `turnover_ratio` 写成：

```text
当日成交额 / 当日均值
```

正确是：

```text
当日成交额 / 前20日成交额中位数
```

且前 20 日不含当日。

### 错误 2

把 `range_pos_120` 写成过去 120 日不含当日。

当前仓库实现是：

- rolling window 包含当日

### 错误 3

把动态止盈写成“今天先到新高，再回落 3%，同一天按回撤卖出”。

当前实现不是这样。当前实现顺序是：

1. 先止损
2. 再检查已经激活后的回撤止盈
3. 最后更新新高和激活状态

### 错误 4

把单仓 `15%` 理解为固定本金 `15%`。

当前实现是：

- 以“开盘前组合权益”为基准计算目标仓位

### 错误 5

忽略“已有持仓不可重复买同一只”。

当前实现明确跳过重复持仓。

## 18. 给另一个 LLM 的最短任务描述

如果你要把这份规则交给另一个 LLM 写代码，可以直接给它这段：

```text
请实现一个 A 股日频策略：

1. 股票池只看主板，排除 ST、停牌、北交所。
2. 候选日使用收盘后数据，下一交易日开盘买入。
3. 先基于候选日前最后 10 个交易日的四个特征计算 path_score：
   - mean_turnover10
   - contract_flat_rate10
   - expand_up_persist
   - down_to_up
4. 使用固定参数：
   - mean_turnover10: clip[0.481695, 5.768339], mean=1.290802, std=0.827952, weight=+1
   - contract_flat_rate10: clip[0, 0.7], mean=0.189459, std=0.174909, weight=-1
   - expand_up_persist: clip[0, 6], mean=1.081470, std=1.427022, weight=+1
   - down_to_up: clip[0, 3], mean=0.526266, std=0.780123, weight=+0.5
5. 只保留 path_score >= 1.6686971252545528 的股票。
6. 每日按 path_score 从高到低取 top10。
7. 再过滤 120 日区间位置 10%-35%，定义为：
   (close_adj - rolling_120_low) / (rolling_120_high - rolling_120_low)
   其中 rolling high/low 都包含当日。
8. 组合最多持有 10 只，单只目标仓位 15%，100 股整手，双边成本各 12bp。
9. 买入后 T+1 才能卖。
10. 卖出规则：
   - 硬止损 6%：low <= entry * 0.94 时按 entry * 0.94 卖
   - 浮盈达到 15% 后激活动态止盈
   - 激活后若 low <= peak_price * 0.97，则按 peak_price * 0.97 卖
   - 注意止盈逻辑不能假设同一天先创新高再回落触发
11. 不设固定止盈，不设最大持仓天数，只在数据最后一天强制平仓。
12. 请给出可运行代码，并把信号生成、仓位管理、卖出判定拆成独立函数。
```

