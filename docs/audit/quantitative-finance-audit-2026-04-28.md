# alpha-find-v2 量化金融审计报告

> 审计日期：2026-04-28
> 审计范围：全代码库（`src/alpha_find_v2/`、`config/`、`tests/`）
> 系统类型：个人 A 股量化交易系统，多头纯股票，周频调仓
> 修订状态：已按第一批优先级修复绩效指标真实性问题，并落地 `turnover_confirmation` 与 `overlap_mode` 的测试保护；本报告保留原审计发现，同时标注修订后的真实状态。

---

## 目录

1. [系统概述](#1-系统概述)
2. [严重缺陷](#2-严重缺陷)
3. [高优先级问题](#3-高优先级问题)
4. [中优先级问题](#4-中优先级问题)
5. [低优先级问题](#5-低优先级问题)
6. [优化建议](#6-优化建议)
   - [A. Alpha 生成](#a-alpha-生成)
   - [B. 风险管理](#b-风险管理)
   - [C. 投资组合构建](#c-投资组合构建)
   - [D. 执行与可交易性](#d-执行与可交易性)
   - [E. 绩效监控与归因](#e-绩效监控与归因)
   - [F. 数据质量与配置健壮性](#f-数据质量与配置健壮性)
   - [G. A 股特有优化](#g-a-股特有优化)
7. [实施路线图](#7-实施路线图)
8. [附录：关键数据流](#8-附录关键数据流)

---

## 1. 系统概述

### 1.1 核心流水线

```
mandate → thesis → descriptor set → sleeve → portfolio recipe → executable signal → decay record
```

### 1.2 第一产品规格

| 维度 | 配置 |
|----------|-----------|
| 市场 | 中国 A 股 |
| 方向 | 纯多头 |
| 基准 | CSI 800 (000906.SH) |
| 持仓数量 | 15-30 |
| 调仓频率 | 每周（或每周 2-3 次） |
| 执行方式 | 当日收盘研究 → 次日开盘执行 |
| 数据源 | Tushare → DuckDB (PIT 研究数据库) |
| 核心 Alpha | 价格/成交量驱动的中期选股 |
| 慢速锚 | 滞后的质量/价值重估（否决/锚定） |
| 叠加层 | 投资组合层面制度与可交易性控制 |

### 1.3 当前组合构成

- `trend_leadership_core`：70%（16 只股票，20 天目标，16% 单边换手率预算）
- `fundamental_rerating_core`：30%（18 只股票，20 天目标，10% 单边换手率预算）
- `trend_resilience_core`：候选 sleeve（18 只股票，更多向稳定/流动性领导者倾斜）

---

## 2. 严重缺陷

### 2.1 信息比率被硬编码为夏普比率

**文件：** `src/alpha_find_v2/portfolio_backtester.py`，第 1048 行
**严重性：** 严重
**类型：** 指标计算错误
**修订状态：** 已修复。`information_ratio` 已改为基准主动收益 / 跟踪误差；无基准或无有效跟踪误差时返回 `0.0`。

```python
# 第 1048 行
information_ratio=sharpe,
```

审计时 `portfolio_backtester.py:980-1058` 中的 `_summarize()` 方法完全基于投资组合日收益，不使用任何基准数据。`information_ratio` 字段存在但被字面量赋值等于 `sharpe`，没有计算基准收益序列。

对于定义了基准的主动型投资组合，信息比率必须衡量的是*相对于基准*的表现：

```
IR = mean(portfolio_return - benchmark_return) / std(portfolio_return - benchmark_return) × sqrt(252)
```

**影响：** 所有报告的 IR 值均为虚假值。它们衡量的是绝对单位风险的绝对收益，而非主动管理技能。对于 β ≈ 1 的投资组合，误差相对较小；对于 β 显著偏离 1 的投资组合，误差可能很大。

**已采用的修复：**
1. 使用已加载的 `BenchmarkStateArtifact` 个股权重（第 1261 行已加载，但仅用于行业权重约束）计算每日基准收益序列。
2. 计算 `active_return = portfolio_return - benchmark_return`。
3. 计算 `tracking_error = stdev(active_returns) × sqrt(252)`。
4. 计算真正的 `information_ratio = mean(active_returns) × 252 / tracking_error`。
5. 在 `PortfolioBacktestSummary` 中添加字段：`benchmark_annualized_return`、`active_annualized_return`、`tracking_error`、`beta`、`alpha`。

### 2.2 完全未计算基准相对指标

**文件：** `src/alpha_find_v2/portfolio_backtester.py`，第 980-1058 行
**严重性：** 严重
**类型：** 功能缺失
**修订状态：** 已补充 `benchmark_annualized_return`、`active_annualized_return`、`tracking_error`、`beta`、`alpha`。

审计时 `_summarize()` 方法计算 18 个指标，不涉及任何基准数据。缺失的内容包括：

| 指标 | 当前状态 |
|--------|---------------|
| 跟踪误差 | 未计算 |
| 主动收益 / 超额收益 | 未计算 |
| Beta | 未计算 |
| Alpha (Jensen's) | 未计算 |
| 上行/下行捕获率 | 未计算 |
| 主动份额 | 未计算 |

从 data spine 加载的 `BenchmarkStateArtifact` 包含完整的成分股权重历史。这些权重可用于按日重建基准收益。此前从未做到这一点。

**修复：** 复用已加载的工件，基于成分股权重和 `daily_bar_pit` 中可得的收益，计算每日基准收益。至少在摘要中添加跟踪误差、主动收益和真正的信息比率。

---

## 3. 高优先级问题

### 3.1 回测换手率计算错误 — 约为 2 倍

**文件：** `src/alpha_find_v2/portfolio_backtester.py`，第 1022-1027 行
**严重性：** 高
**类型：** 指标计算错误
**修订状态：** 已修复为对称单边口径 `(buy_gross + sell_gross) / 2 / average_equity`，并输出 `buy_turnover` / `sell_turnover` 分项。

```python
# 第 1022-1027 行
average_equity = statistics.mean(state.equity for state in daily_curve)
turnover = (
    sum(fill.gross_value for fill in fills) / average_equity
    if average_equity > 0.0
    else 0.0
)
```

这在*整个回测*期间求和每笔成交的成交额（买入与卖出），然后除以平均权益。在平衡调仓中，买入 ≈ 卖出，分子约等于实际换手价值的 2 倍。

**行业标准：**

```python
# 单向换手率（常见于学术文献）：
total_buy = sum(f.gross_value for f in fills if f.side == "buy")
turnover = total_buy / average_equity

# 备选方案：取 min(buy, sell) 更保守：
total_buy = sum(f.gross_value for f in fills if f.side == "buy")
total_sell = sum(f.gross_value for f in fills if f.side == "sell")
turnover = min(total_buy, total_sell) / average_equity
```

**影响范围修订：** 该问题会使 `PortfolioBacktestSummary.turnover` 偏高，尤其在买卖金额接近平衡的调仓期接近 2 倍口径差异。但当前 promotion、decay 与实盘 readiness 的换手预算主要来自 `PortfolioSimulator` / `ResearchEvaluator` 的逐步模拟结果，不直接消费该回测 summary 字段。因此它首先是回测报告真实性问题，而非已确认会污染 promotion gate 的预算问题。

### 3.2 夏普比率未减去无风险利率

**文件：** `src/alpha_find_v2/portfolio_backtester.py`，第 1010-1013 行
**严重性：** 高
**类型：** 指标计算错误
**修订状态：** 已添加 `risk_free_rate_annual` 输入，默认 `0.0` 保持兼容；Sharpe 现按 `(annualized_return - risk_free_rate_annual) / annualized_volatility` 计算。

```python
# 第 1010-1013 行
sharpe = (
    annualized_return / annualized_volatility
    if annualized_volatility > 0.0
    else 0.0
)
```

这只计算了回报/波动比率，而非夏普比率。正确的定义是：

```
Sharpe = (R - Rf) / σ
```

对于中国 A 股市场，合适的最低标准是减去 1 年期存款利率（历史上约 1.5-3%）或 SHIBOR。

**影响：** 所有报告的夏普比率均系统性偏高。对于年化收益 8%、波动率 15% 的投资组合，误差约为 `0.02/0.15 = 0.133` 个夏普比率单位。

### 3.3 换手率确认等同于流动性偏差

**文件：** `src/alpha_find_v2/trend_research_input_builder.py`，第 959-961 行
**严重性：** 高
**类型：** Alpha 因子设计缺陷
**修订状态：** 已改为相对自身历史成交额基线的确认项；默认 `turnover_baseline_window_days = 120`，且仅正向趋势获得成交放大确认加分。

```python
# 第 959-961 行
"turnover_confirmation": math.log(
    max(candidate.median_turnover_cny / 1_000_000.0, 1.0)
),
```

这体现了对绝对每日换手率水平的单调、正向暴露。换手率高于 100 万的股票得分高于换手率低于 100 万的股票，与价格走势无关。该因子正在购买流动性，而非确认趋势。

真正的换手率确认应衡量成交量是否在方向性价格变动期间相对于*自身历史*增加：

```python
# 正确：当价格高于均线时，相对于历史换手率确认
turnover_ratio = median_turnover_now / median_turnover_6m_history
confirmation = turnover_ratio if ret_short > 0 else 1.0 / turnover_ratio
```

**已采用的修复：** 保留该因子，但改为相对自身历史基线：`log(max(current_median_turnover / historical_baseline, 1.0))`；当 `ret_short <= 0` 时返回 `0.0`，避免把下跌放量作为正向确认。

---

## 4. 中优先级问题

### 4.1 组合内等权配置放弃 Alpha 差异化

**文件：** `src/alpha_find_v2/trend_research_input_builder.py`，第 324 行
**严重性：** 中
**类型：** 组合构建

```python
# 第 324 行
target_weight = 1.0 / len(selected)
```

所有入选标的在得分排序后获得相同的 1/N 权重。排名第 1 与排名第 25 的权重完全相同。这忽略了信号强度（得分差异可达 2+ 个标准差）。标准量化做法是根据得分或波动率倒数分配权重。

### 4.2 投资组合模拟器换手率采用非标准惯例

**文件：** `src/alpha_find_v2/portfolio_simulator.py`，第 112 行
**严重性：** 中
**类型：** 惯例不匹配

```python
# 第 112 行
turnover=max(buy_turnover, sell_turnover),
```

使用 `max(buy, sell)` 是一种偏保守的单向最大边惯例。行业报告中也常见 `(buy + sell) / 2.0`（对称单边口径）。在因进入/退出受阻导致买入 ≠ 卖出的时期，`max(buy, sell)` 相对 `(buy + sell) / 2.0` 会偏高，而不是低估。该口径目前服务 promotion / decay 的逐步模拟预算，若要改动必须同步重标定所有预算阈值。

### 4.3 趋势 Alpha 信号无行业中性化

**文件：** `src/alpha_find_v2/trend_research_input_builder.py`，第 966-973 行
**严重性：** 中
**类型：** Alpha 构建

趋势管道使用全局 `_zscore_map()`（跨板块可比），而基本面管道使用 `_industry_neutral_zscore_map()`（`fundamental_research_input_builder.py:667-681`，行业内可比）。行业中性化仅在组合层面通过 `portfolio_constructor.py` 中的 `_apply_industry_caps` 应用，且仅当 `industry_budget_mode == "benchmark_relative"` 时生效。这种方法不加区别地在事后对所有股票进行裁剪。

对于趋势策略，在 Alpha 生成层面不进行行业中性化可能导致投资组合集中在轮动中的热门行业。组合层面的上限只是机械地削减已聚合的权重。

### 4.4 部分已声明的配置字段从未被代码读取

**文件：** `models.py:435`，`models.py:405`
**严重性：** 中
**类型：** 死配置

- `PortfolioConstructionModel.overlap_mode` 在审计时未被读取，代码无条件地对跨 sleeve 的权重求和。修订后已支持 `sum`、`max`、`average`，且当前配置为 `"sum"` 时保持既有行为。
- `Sleeve.neutralization`（在所有 sleeve 配置中设为 `["industry", "size", "beta"]`）在任何计算代码中均未被引用。这些字段均为无操作项。

### 4.5 CAGR 时间基准不一致

**文件：** `src/alpha_find_v2/portfolio_backtester.py`，第 996-1002 行
**严重性：** 中
**类型：** 计算方法

```python
# 第 996-1002 行
calendar_years = max(
    (end_date - start_date).days / 365.25,
    trading_days / 252.0,
)
```

在日历年份和交易日换算年份之间取 `max`，会根据时段不同产生不一致的分母。A 股通常每年约有 242-244 个交易日（而非 252 个）。在短回测期中，`max()` 逻辑可导致使用交易日计数替代日历计数，从而压低了 CAGR。

**修复：** 使用纯日历时间（`days / 365.25`）——这是行业标准。仅在需要时，通过分析中国市场数据，获得正确的每年交易日计数。

### 4.6 `Mandate.filters` 中的授权过滤器从未在 Python 中被消费

**文件：** `models.py:26`，`config/mandates/a_share_long_only_eod.toml:17-22`
**严重性：** 中
**类型：** 死配置

授权配置声明了 `exclude_st=true`、`exclude_suspended=true`、`exclude_limit_up_locked=true`、`exclude_limit_down_locked=true` 和 `exclude_boards=["beijing"]`。这些存储在 `Mandate.filters` 字典中，配置加载层会保留它们，但运行时代码没有统一消费该字典作为授权过滤器。趋势输入构建器有 case-level `exclude_boards`，基本面输入构建器没有对应板块过滤字段；ST 过滤仍由趋势和基本面输入构建器各自硬编码。如果授权配置发生变化，运行逻辑不会自动随之更新。

---

## 5. 低优先级问题

### 5.1 制度叠加使用二元计数，未考虑相关性

**文件：** `src/alpha_find_v2/regime_overlay.py`
**严重性：** 低

`RegimeOverlayEvaluator` 使用简单的 `risk_off` 输入计数来触发降级（1 个触发 de_risk，3 个触发 cash_heavier）。由于风险指标在压力时期往往同步变动，简单的计数法可能：
- 在嘈杂市场中过度降级（所有指标短暂闪烁 risk_off）
- 在严重危机中降级过慢（需要累积至 3 个才降级，延迟严重）

### 5.2 无成交量依赖性冲击成本

**文件：** `src/alpha_find_v2/models.py`，第 194-231 行
**严重性：** 低

`CostModel` 对所有交易采用固定的 bp 假设（买入 5bp 滑点，卖出 6bp 滑点），无论订单规模或股票流动性如何。对于小盘股或大额交易，这将低估真实的交易成本。

### 5.3 回测中缺少显式 T+1 可用股份台账

**文件：** `src/alpha_find_v2/portfolio_backtester.py`，第 639-805 行
**严重性：** 低

回测器当前采用“决策日 → 下一交易日开盘执行”的调仓时钟，因此不应表述为“买入当日即可卖出”。真实缺口是回测账本没有像实盘账户状态那样显式维护 `available_shares`，也没有把 T+1 可卖股份作为独立结算台账输出。实盘层已有 `available_shares` 检查（`live_state.py:211-213`），回测层仍缺少同等级别的可审计台账。

### 5.4 无运行时退市检测

**文件：** `src/alpha_find_v2/deployment.py`
**严重性：** 低

存在 `security_master_ref.delist_date` 字段，并在数据库中填充。然而，任何 Python 代码均未检查退市日期。实盘账户中持有的退市股票不会被自动标记为强制卖出。

### 5.5 趋势信号缺少 `industry_relative_strength` 因子

**文件：** `config/descriptors/industry_relative_strength.toml`、`config/theses/trend_leadership.toml`
**严重性：** 低

`trend_leadership` thesis 将 `industry_relative_strength` 列为必需数据，而 `trend_leadership_core` 描述符集仅使用 `medium_term_relative_strength`、`trend_stability` 和 `turnover_confirmation`。行业相对强度因子（与行业内同行相比的相对强度）已被声明但从未被连接。

### 5.6 实盘部署中停滞持仓的重叠歧义

**文件：** `src/alpha_find_v2/deployment.py`，第 385-420 行
**严重性：** 低

在 `_instruction_for_asset()` 中，`blocked_entries` 在 `blocked_exits` 之前被检查。如果一个被持有的持仓同时被双阻（例如，被停牌），其操作将被归类为 `skip_enter_blocked` 而非 `hold_exit_blocked`，这可能产生对持仓的误导性表述。

---

## 6. 优化建议

以下建议针对个人量化系统的约束条件设计：有限的数据获取途径、单人维护、无机构级基础设施。目标是在这些约束条件下，尽可能做到专业水平。

### A. Alpha 生成

#### A1. 基于得分的仓位规模 — 替代 1/N 等权

**优先级：** 高 | **难度：** 低
**位置：** `src/alpha_find_v2/trend_research_input_builder.py:324`

后选取的等权分配放弃了信号强度区分。更专业的做法：

```python
# 方案 A：得分比例制（简单、稳健）
scores = [item["score"] for item in selected]
min_score = min(scores)
adjusted = [max(s - min_score, 0) for s in scores]
total = sum(adjusted)
weights = [a / total for a in adjusted] if total > 0 else [1/len(selected)] * len(selected)

# 方案 B：排名加权（对异常值稳健）
ranks = list(range(1, len(selected) + 1))
weights = [1.0 / r for r in ranks]
total = sum(weights)
weights = [w / total for w in weights]
```

**原理：** 排名第 1 的信号应比排名第 25 的信号获得更多资本。得分比例制在 top-decile 股票中保留了 alpha 分散化，同时将更多资本分配给信号最强者。

#### A2. 趋势 Alpha 的行业中性 Z-Score

**优先级：** 高 | **难度：** 低
**位置：** `src/alpha_find_v2/trend_research_input_builder.py:966-973`

复用 `src/alpha_find_v2/fundamental_research_input_builder.py:667-681` 中已有的 `_industry_neutral_zscore_map()` 函数。在趋势输入构建案例配置中添加一个配置开关（`industry_neutral_scoring: bool`）。这使得行业内可比改为按行业分组独立计算 z-score，防止趋势持仓集中在轮动中的热门行业。

#### A3. 信号衰减权重

**优先级：** 中 | **难度：** 低
**位置：** `src/alpha_find_v2/trend_research_input_builder.py:953`

当前对短期和长期收益取简单平均（`0.5 * ret_short + 0.5 * ret_long`）。价格动量在较长时间窗口中呈现指数衰减：

```python
half_life = 10  # 交易日
decay_rate = math.log(2) / half_life
weight_short = 1.0 - math.exp(-decay_rate * short_window_days)
weight_long = math.exp(-decay_rate * short_window_days)
strength = weight_short * ret_short + weight_long * ret_long
```

**原理：** 近期收益对后续收益的预测能力明显更强。指数衰减权重反映了这一经济现实。

#### A4. 连接已声明但未实现的 `neutralization` 字段

**优先级：** 中 | **难度：** 中
**位置：** `src/alpha_find_v2/models.py:405`，`src/alpha_find_v2/portfolio_constructor.py:65-126`

所有 sleeve 配置均声明 `neutralization = ["industry", "size", "beta"]`，但这从未被实现。至少：
- 行业中性化：管道中已有（A2）
- 规模中性化：在组合构建期间，对流通市值的原始得分进行横截面回归
- Beta 中性化：需要风险模型中已配置的 beta 暴露数据

### B. 风险管理

#### B1. 组合层面波动率目标制

**优先级：** 高 | **难度：** 中
**位置：** `src/alpha_find_v2/portfolio_backtester.py:639-805`

目前无任何机制根据市场波动率调整风险敞口。波动率目标制是投资组合层面最稳健的风险控制手段之一：

```python
# 将总敞口缩放至目标波动率
realized_vol = stdev(daily_returns[-60:]) * sqrt(252)  # 年化
target_vol = 0.12  # 年化 12%
vol_scale = min(1.0, target_vol / max(realized_vol, 0.01))
scaled_weights = {k: v * vol_scale for k, v in target_weights.items()}
```

**原理：** A 股波动率可轻易从 15% 跃升至 40%。若没有波动率目标制，你的投资组合会承担远超预期的风险。12% 的目标与 CSI 800 的长期波动率及你的纯多头限制相一致。

#### B2. 最大回撤止损

**优先级：** 中 | **难度：** 低
**位置：** `src/alpha_find_v2/portfolio_backtester.py` (新增)，`src/alpha_find_v2/deployment.py` (新增)

追踪从历史最高点的滚动回撤；当回撤超过可配置的门槛时，降低敞口：

```python
peak_equity = max(peak_equity, current_equity)
drawdown = 1.0 - (current_equity / peak_equity)
if drawdown > max_drawdown_limit:
    de_risk_multiplier = 0.5  # 或渐进缩减
    scaled_weights = {k: v * de_risk_multiplier for k, v in target_weights.items()}
```

处理制度叠加与回撤止损之间的交互：取两者中较低的乘数。

#### B3. 波动率倒数仓位规模（组合内层级）

**优先级：** 低 | **难度：** 低
**位置：** `src/alpha_find_v2/trend_research_input_builder.py:324`（备选修改）

一种简单但有效的风险平价形式：

```python
vol_i = stdev(daily_returns_for_asset_i[-60:])
inv_vol = 1.0 / max(vol_i, 0.005)  # 设定波动率下限
weights = [inv_vol_i / sum(inv_vols) for inv_vol_i in inv_vols]
```

这默认倾向于波动率较低的股票，无需协方差优化即可降低组合层面的特质波动率。

### C. 投资组合构建

#### C1. 实现 `overlap_mode` 逻辑

**优先级：** 中 | **难度：** 低
**位置：** `src/alpha_find_v2/portfolio_constructor.py:86-89`

目前代码无条件地对重叠仓位的权重求和。添加模式感知逻辑：

```python
if overlap_mode == "sum":
    combined_weights[aid] = combined_weights.get(aid, 0.0) + weight
elif overlap_mode == "max":
    combined_weights[aid] = max(combined_weights.get(aid, 0.0), weight)
elif overlap_mode == "average":
    overlap_counts[aid] = overlap_counts.get(aid, 0) + 1
    combined_weights[aid] = combined_weights.get(aid, 0.0) + weight
# ... 对所有重叠资产应用 average 后除以计数
```

#### C2. 换手缓冲

**优先级：** 中 | **难度：** 中
**位置：** `src/alpha_find_v2/portfolio_constructor.py:65-126`

在目标与当前持仓权重之间添加缓冲区间，以防止小幅漂移引发不必要的换手：

```python
buffer = 0.002  # 20bp 缓冲
if abs(target_weight - current_weight) < buffer:
    target_weight = current_weight  # 维持现有持仓，不触发交易
```

**原理：** 交易是需要成本的。对于 25 只持仓、换手率 20% 的投资组合，10bp 的滑点每年约损失 12.5bp 的成本。20bp 的缓冲区间意味着，调仓预期 alpha 低于 20bp 的仓位将不会被触及。

#### C3. 受阻信号下的智能剩余现金重新分配

**优先级：** 低 | **难度：** 低
**位置：** `src/alpha_find_v2/portfolio_backtester.py:639-805`, `src/alpha_find_v2/deployment.py`

当由涨跌停锁定或停牌导致买入订单被阻止时，配置 `cash_policy = "hold_residual_cash"`。增加一个选项，将剩余现金按比例重新分配给未被阻止的仓位，而不是以现金形式持有。

### D. 执行与可交易性

#### D1. 运行时 ST / 退市检测

**优先级：** 高 | **难度：** 低
**位置：** `src/alpha_find_v2/deployment.py:316-353`

授权配置声明了 `exclude_st = true`，但从未在 Python 中被消费。在 `_signals_with_portfolio_state()` 中添加：

```python
for asset_id in list(positions):
    bar = bars.get((asset_id, trade_date))
    if bar and bar.is_st:
        blocked_exits.append(asset_id)  # 强制退出 ST 股票
```

同样适用于 `delist_date`（`security_master_ref` 中可用）——检查任何持仓是否已达到退市日期，并标记为强制卖出。这属于硬风险控制，必须在每个交易日实盘部署前运行。

#### D2. 回测器中的 T+1 可用股份台账

**优先级：** 中 | **难度：** 中
**位置：** `src/alpha_find_v2/portfolio_backtester.py:639-805`

目前回测器已经按 next-open 时钟执行，不应描述为“买入当日卖出”。更准确的增强是补一层可审计的 `available_shares` 台账：买入成交先增加持仓股数，但到下一交易日才进入可卖股数；卖出只能消耗可卖股数。

```python
available_shares_by_asset: dict[str, float] = {}
if sell_quantity > available_shares_by_asset.get(asset_id, 0.0):
    blocked_exits.append(asset_id)  # T+1 可卖股份不足
```

这使回测器与实盘层（通过 `available_shares` 处理 T+1 的 `live_state.py:211-213`）保持一致，同时保留当前 next-open 执行断言。

#### D3. 与成交量相关的滑点

**优先级：** 中 | **难度：** 中
**位置：** `src/alpha_find_v2/models.py:194-231`, `src/alpha_find_v2/portfolio_backtester.py:1125-1137`

当前所有交易使用固定的 5-6bp 滑点。对于个人规模而言，一个简单有效的模型：

```python
volume_share = order_value / bar.turnover_value_cny
impact_bps = base_slippage_bps * (1.0 + 2.0 * volume_share / participation_cap)
spread_bps = (bar.high / bar.low - 1.0) * 5000  # 半价差近似
total_slippage_bps = impact_bps + spread_bps
```

这是简化版的 Almgren-Chriss 模型，不会显著增加复杂度。

#### D4. 开盘前的限价涨跌停风险评估

**优先级：** 低 | **难度：** 低
**位置：** `src/alpha_find_v2/portfolio_backtester.py:1075-1087`

当前逻辑仅检测开盘时刻的涨跌停锁定。增加基于收盘价的启发式方法，用于执行规划：

```python
def _next_day_lock_risk(self, bar: DailyBar, direction: str) -> bool:
    if direction == "buy" and bar.close >= bar.pre_close * 1.099:
        return True  # 接近涨停限制——明天的买入执行风险很高
    if direction == "sell" and bar.close <= bar.pre_close * 0.901:
        return True  # 接近跌停限制
    return False
```

### E. 绩效监控与归因

#### E1. 因子归因报告

**优先级：** 中 | **难度：** 中
**位置：** `src/alpha_find_v2/portfolio_backtester.py:980-1058`，新模块

在回测摘要中添加归因部分，将收益分解为：

```
总收益 = 市场 Beta × 基准收益 + 行业贡献 + 选股效应 + 成本拖累
```

归因框架：
1. **市场贡献：** `beta × benchmark_return`
2. **行业贡献：** `sum(industry_weight × industry_return) - benchmark_return`
3. **选股效应：** `total_return - market_contribution - industry_contribution`
4. **成本拖累：** 佣金 + 滑点 + 印花税的总成本（以收益 bps 表示）

这能够回答"超额收益的真正来源是什么？"，而非仅仅提供一个总体的 IR。

#### E2. 滚动窗口绩效面板

**优先级：** 低 | **难度：** 中
**位置：** CLI 新增命令

生成按滚动窗口划分的绩效指标面板（6 个月窗口，每月滚动）。这些对于以下方面非常有价值：
- 观测 alpha 稳定性随时间变化
- 识别持续表现不佳的时期
- 验证衰减监控器阈值

#### E3. 持仓级 P&L 归因

**优先级：** 低 | **难度：** 低
**位置：** `src/alpha_find_v2/portfolio_backtester.py:946-978`

在每日曲线中，记录每项持仓对投资组合收益的边际贡献：

```python
for asset_id, position in positions.items():
    yesterday_price = previous_bars[asset_id].close
    today_price = bars[(asset_id, trade_date)].close
    pnl_contribution = position.shares * (today_price - yesterday_price)
```

这对于事后理解哪些股票实际驱动了收益，以及纯信号在实践中的表现如何，至关重要。

### F. 数据质量与配置健壮性

#### F1. 启动时配置验证

**优先级：** 中 | **难度：** 中
**位置：** 新模块 `src/alpha_find_v2/config_validator.py`

添加 CLI 命令 `validate-all-configs`，检查所有交叉引用是否可解析：

- 每个 `sleeve.thesis_id` → 存在于 `config/theses/` 中
- 每个 `descriptor_set.components[].descriptor_id` → 存在于 `config/descriptors/` 中
- 每个 `target.risk_model_id` → 存在于 `config/risk_models/` 中
- 每个 `target.cost_model` → 存在于 `config/cost_models/` 中
- 所有 `portfolio.sleeves` → 存在于 `config/sleeves/` 中
- 所有 `portfolio.{construction_model_id, execution_policy_id, promotion_gate_id, regime_overlay_id, decay_monitor_id}` → 存在于其相应的配置目录中

**原理：** 个人系统不会配备 CI/CD 流水线或配置漂移检测。一个简单的预部署验证命令可以在配置问题引起实盘交易错误之前，将其捕获。每个配置更改后、实盘部署之前运行此命令。

#### F2. 市场数据质量检查

**优先级：** 中 | **难度：** 低
**位置：** `src/alpha_find_v2/market_data_bootstrap.py`，新模块

在构建研究源数据库之后，运行数据质量检查：

- 缺失日期（与交易日历对照）
- 调整因子为 0 或缺失的股票（潜在数据损坏）
- 价格变动 > 涨跌停限制（对于主板的 10% 涨跌幅，除非是北京/科创/创业板板块）
- 连续多日停牌（每日数据中的 open/high/low/close 值相同）

#### F3. 每日数据延迟检查

**优先级：** 中 | **难度：** 低
**位置：** `src/alpha_find_v2/deployment.py`（新增）

在构建可执行信号之前，通过检查数据库中的 `MAX(trade_date)` 是否在 1 个交易日内，确认当日的市场数据可用。这可以防止在前一日的数据上构建研究输入，然后在次日执行信号的错误。

### G. A 股特有优化

#### G1. 集合竞价意识

**优先级：** 高 | **难度：** 中
**位置：** `src/alpha_find_v2/deployment.py`（新增）

A 股的集合竞价阶段（09:15-09:25）为执行提供了关键信号。如果 Tushare 提供竞价数据，则添加：

```python
# 需审查的条件：
# 1. 竞价价格 vs. 前收盘价 > 5%
# 2. 竞价成交量 < 日均量的 10%（滑点预期更高）
# 3. 竞价阶段触及涨跌停价格
```

即使没有完整的竞价数据，也可用 `open / pre_close` 比率作为竞价压力的代理变量，并标记极端值以供审查。

#### G2. 板块特定 IPO 锁定期规则

**优先级：** 中 | **难度：** 低
**位置：** `src/alpha_find_v2/trend_research_input_builder.py:700-701`

当前对全部股票采用通用的 `min_listing_days = 120`。这是一条保守研究过滤器，不是交易所涨跌幅制度本身。注册制后不同板块的新股价格限制应按当前规则表述：

- 主板、科创板、创业板新股上市后的前 5 个交易日不设价格涨跌幅限制。
- 第 6 个交易日起，主板普通股票通常恢复 10% 涨跌幅限制；科创板和创业板通常为 20%。
- ST 股票、北交所股票、特殊处理和临时停复牌情形另有规则，不能用单一 `min_listing_days` 表示。

```python
if board in {"chinext", "star"}:
    min_listing_days = 60  # 研究稳定期，不是交易所硬性涨跌幅规则
else:
    min_listing_days = 120
```

参考：上交所交易机制说明（主板 10%、科创板 20% 价格限制）<https://english.sse.com.cn/start/trading/mechanism/>；上交所注册制问答（首次公开发行股票上市后前 5 个交易日不设涨跌幅限制）<https://big5.sse.com.cn/site/cht/www.sse.com.cn/aboutus/mediacenter/hotandd/c/c_20230201_5715605.shtml>。

#### G3. 股息调整验证

**优先级：** 低 | **难度：** 低
**位置：** `src/alpha_find_v2/market_data_bootstrap.py:132-199`

系统使用 qfq（前复权）调整价格，适用 `adj_factor`。验证除息日附近不会产生异常收益（例如，调整因子应用错误导致的 5% 跳空）。在回测摘要中添加关于股息调整方法及其局限性的注释。

---

## 7. 实施路线图

### 阶段 1：缺陷修复（第 2-3 节）
修复指标缺陷和计算错误。这些是纯错误修复，不改变交易行为。

| 顺序 | 项目 | 影响范围 |
|------|------|--------|
| 1 | 计算基准收益，修复 IR（#2.1） | `portfolio_backtester.py`（已修复） |
| 2 | 添加跟踪误差、主动收益、Beta 指标（#2.2） | `portfolio_backtester.py`（已修复） |
| 3 | 在 Sharpe 中添加 Rf（#3.2） | `portfolio_backtester.py`，配置（已修复，默认 Rf=0） |
| 4 | 修复回测换手率公式（#3.1） | `portfolio_backtester.py`（已修复） |
| 5 | 重新设计换手率确认因子（#3.3） | `trend_research_input_builder.py`（已修复） |

### 阶段 2：Alpha 与组合优化（第 4 节 + 第 6.A-C 节）
改善信号质量和投资组合构建。

| 顺序 | 项目 | 影响范围 |
|------|------|--------|
| 6 | 基于得分的仓位规模（A1） | `trend_research_input_builder.py` |
| 7 | 趋势行业中性化（A2 + #4.3） | `trend_research_input_builder.py` |
| 8 | 波动率目标制（B1） | `portfolio_backtester.py` |
| 9 | 实现 overlap_mode（C1 + #4.4） | `portfolio_constructor.py`（已修复） |
| 10 | 连接 sleeve.neutralization（A4） | 多个文件 |
| 11 | 换手缓冲（C2） | `portfolio_constructor.py` |

### 阶段 3：执行与数据加固（第 6.D-F 节）
加固实盘部署路径。

| 顺序 | 项目 | 影响范围 |
|------|------|--------|
| 12 | 运行时 ST/退市检测（D1） | `deployment.py` |
| 13 | 配置验证器（F1） | 新模块 |
| 14 | 数据质量检查（F2） | 新模块 |
| 15 | T+1 结算模型（D2） | `portfolio_backtester.py` |
| 16 | 成交量相关滑点（D3） | `models.py`，`portfolio_backtester.py` |

### 阶段 4：监控与归因（第 6.E、G 节）
添加专业的监控和报告能力。

| 顺序 | 项目 | 影响范围 |
|------|------|--------|
| 17 | 因子归因报告（E1） | `portfolio_backtester.py`，新模块 |
| 18 | 回撤止损（B2） | `portfolio_backtester.py`，`deployment.py` |
| 19 | 竞价意识（G1） | `deployment.py` |
| 20 | 滚动绩效面板（E2） | CLI |
| 21 | 持仓级 P&L（E3） | `portfolio_backtester.py` |

---

## 8. 附录：关键数据流

### 8.1 研究流水线

```
[DB: daily_bar_pit + industry_classification_pit + benchmark_membership_pit]
        │
        ├─→ TrendResearchInputBuilder
        │     - 加载候选标的（过滤流动性、ST、上市天数、涨跌停）
        │     - 全市场 z-score 评分（无行业中性化）
        │     - 附加行业标签（来自 industry_classification_pit）
        │     - 输出 SleeveResearchObservationInput
        │
        ├─→ FundamentalResearchInputBuilder
        │     - 加载带有公告滞后的基本面快照
        │     - 行业内 z-score 评分（在 Alpha 层面进行行业中性化）
        │     - 输出 SleeveResearchObservationInput
        │
        └─→ ResearchArtifactBuilder
              - 评估目标收益（对冲成本）
              - 输出 SleeveResearchArtifact（含 realized_return）
                    │
                    ▼
        PortfolioConstructor
              - 按 overlap_mode 合成各 sleeve 权重（sum / max / average）
              - 应用名称选择（前 N 名）
              - 应用个股权重上限
              - 应用行业权重上限（benchmark_relative）
                    │
                    ▼
        PortfolioSimulator / Backtester / PromotionReplay
              - 执行交易，计算收益
```

### 8.2 换手率数据流

```
PortfolioSimulator._trading_costs()
  │  turni = max(buy_weight_delta_i, sell_weight_delta_i)  [单边惯例]
  │
  ├─→ ResearchEvaluator.summarize()
  │     average_turnover = mean(turnover_i across steps)
  │     realized_turnover_vs_budget = average_turnover / turnover_budget
  │
  ├─→ PromotionGateEvaluator.evaluate()
  │     检查：realized_turnover_vs_budget <= max_realized_vs_budget
  │
  └─→ PortfolioBacktester._summarize()
        turn = ((buy_gross + sell_gross) / 2) / average_equity
        同时输出 buy_turnover / sell_turnover 分项
```

### 8.3 基准使用情况

基准数据在回测流水线中有两条用途：行业权重约束，以及基准相对绩效指标。

```
BenchmarkStateArtifact.weights_by_date()
  │  dict[trade_date, dict[industry, weight]]
  │
  └─→ PortfolioConstructor._apply_industry_caps()
        约束：sum(portfolio_weight_in_industry) <= bench_weight + max_overweight

BenchmarkStateArtifact.steps[].constituents
  │  dict[trade_date, dict[asset_id, weight]]
  │
  └─→ PortfolioBacktester._benchmark_daily_returns()
        使用前一交易日权重与前收/今收 adjusted close 计算基准日收益

仍未覆盖：
  - 上行/下行捕获率
  - 主动份额
  - 完整基准相对归因
```

### 8.4 投资组合中的成本累加

```
CostModel
  ├─ 买入成本 = buy_commission_bps + buy_slippage_bps
  ├─ 卖出成本 = sell_commission_bps + sell_slippage_bps + sell_stamp_duty_bps
  └─ 往返成本 = 买入成本 + 卖出成本

PortfolioSimulator：
  ├─ 成本从收益率中扣除
  └─ 净收益 = 总收益 - 成本

PortfolioBacktester：
  ├─ 每笔成交后，现金减少 = gross_value + cost
  ├─ T+1 执行在下次开盘
  └─ 持仓以收盘价估值
```

---

*审计结束。*
