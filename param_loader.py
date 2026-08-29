# ============================================================
# param_loader.py  v1（新增文件）
#
# 生产脚本（screener.py / daily_analysis.py / intraday_monitor.py）
# 的运行时参数加载层，让回测调出的"最优参数"不需要人工誊抄进代码
# 就能直接应用到生产。
#
# 核心设计原则：
#
#   1. 代码里的硬编码常量【不删除、不替换】，永远是兜底默认值。
#      params.json缺失、损坏、或某个字段类型不对，一律对该字段
#      回退到硬编码值，绝不因为参数文件的问题让生产脚本崩溃，
#      也绝不用一个"部分应用、部分是undefined"的中间状态去运行。
#
#   2. 【只能在各脚本的main()（即if __name__=="__main__"分支）里
#      调用apply_params()，绝不能放在模块顶层（import时）执行】。
#      原因：screener.py被backtest_engine.py直接import并复用其
#      函数（_passes_tier/calc_composite_score等），
#      intraday_monitor.py被backtest_intraday.py的Stage2直接
#      import并复用其函数（detect_mode1_breakout等真实生产函数）。
#      如果参数加载发生在模块导入的时候：
#        - 会污染backtest_engine.py的_SCREENER_PRISTINE_DEFAULTS
#          快照机制——该机制的前提是"import screener时看到的是
#          硬编码字面量"，一旦screener.py自己在import时就套用了
#          params.json，这个"pristine"基线就不再纯粹，
#          --run-queue连续跑实验之间的重置机制会被静默破坏。
#        - 会让backtest_intraday.py Stage2在没有征得同意的情况下
#          开始被"生产环境当前params.json内容"影响，跟"同一个
#          Stage1 param_set重跑两次几乎完全一致"这种回测复现性
#          要求直接冲突。
#      main()级别的调用完全不影响这两个回测引擎的import行为——
#      它们的import发生在更早、且完全独立于生产脚本main()的时机。
#
#   3. dict类型字段做【递归合并】：只覆盖JSON里显式提到的key，
#      未提到的key保留模块原有默认值（例如只想调T1的信心分数基准，
#      不会连带丢失T2-T4默认值——这条已经是backtest_engine.py验证
#      过的行为，这里是同一个设计原则，独立实现）。
#      list类型字段目前只有screener.py的TIERS，按"level"字段
#      逐项合并，同样不会因为JSON里只写了T1就丢失T2-T4定义。
#
#   4. 少数"一旦写错就可能造成真实资金层面严重后果"的字段
#      （仓位比例、止损ATR倍数、手续费率等）有硬性数值范围校验。
#      超出范围的单个字段：拒绝应用、回退默认值、记error，
#      不影响其余字段正常应用（不是"整份文件回退"）。
#
#   5. 审计留痕：每次运行都记录"实际生效的覆盖有哪些、被拒绝的
#      有哪些及原因"；可选传入telegram_on_change回调，仅当
#      params.json内容的hash较上次运行发生变化时才推送提醒
#      （intraday_monitor.py每15分钟跑一次，不适合每次都发消息）。
#
# 使用方式（在各脚本main()的第一行调用）：
#
#   import param_loader
#   param_loader.apply_params(
#       target_module=sys.modules[__name__],
#       mapping=PARAM_MAPPING,      # 各脚本自己定义，见文件内注释
#       log=log,                    # 各脚本自己的logger
#       hard_bounds=HARD_BOUNDS,    # 各脚本自己定义
#       telegram_on_change=send_telegram,  # 可选
#       state_tag="screener",       # 区分不同脚本的变更比对状态文件
#   )
# ============================================================

import os
import json
import hashlib
import logging
from typing import Any, Callable, Optional

PARAMS_JSON_ENV_VAR = "ASX_PARAMS_JSON"
_STATE_DIR_ENV_VAR = "ASX_PARAM_STATE_DIR"


class _MissingSentinel:
    def __repr__(self):
        return "<MISSING>"


_MISSING = _MissingSentinel()


def default_params_path(caller_file: str) -> str:
    """
    默认路径：跟调用方脚本同目录下的params.json。
    支持环境变量ASX_PARAMS_JSON覆盖（临时排查/测试用），
    生产crontab不需要设置这个环境变量。
    """
    env_path = os.environ.get(PARAMS_JSON_ENV_VAR, "").strip()
    if env_path:
        return env_path
    script_dir = os.path.dirname(os.path.abspath(caller_file))
    return os.path.join(script_dir, "params.json")


def _default_state_path(caller_file: str, tag: str) -> str:
    """
    "参数是否较上次运行发生变化"的比对状态文件路径，
    跟调用方脚本同目录存放。tag区分不同脚本，避免互相覆盖
    （比如screener/daily_analysis/intraday_monitor各自一份）。
    """
    state_dir = os.environ.get(_STATE_DIR_ENV_VAR, "").strip()
    if not state_dir:
        state_dir = os.path.dirname(os.path.abspath(caller_file))
    return os.path.join(state_dir, f".param_state_{tag}.json")


def load_params_json(path: str, log: logging.Logger) -> Optional[dict]:
    """
    安全加载params.json。

    文件不存在 → 返回None，只记info（这是正常情况，不是错误——
    生产环境可以在没有params.json的情况下完全用代码内置默认值
    正常运行）。

    文件存在但解析失败/内容不是dict → 返回None，记error（这种
    情况需要引起注意：文件存在却读不出来，可能正在被写入或者
    手滑存坏了）。
    """
    if not os.path.exists(path):
        log.info(f"param_loader: {path} 不存在，本次运行完全使用代码内置默认参数")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        data = json.loads(raw)
        if not isinstance(data, dict):
            log.error(f"param_loader: {path} 解析出的不是JSON object"
                      f"（是{type(data).__name__}），忽略整个文件，"
                      f"使用代码内置默认参数")
            return None
        return data
    except json.JSONDecodeError as e:
        log.error(f"param_loader: {path} JSON解析失败（{e}），"
                  f"忽略整个文件，使用代码内置默认参数——"
                  f"请检查文件是否正在被写入或者语法有误")
        return None
    except Exception as e:
        log.error(f"param_loader: {path} 读取异常（{e}），使用代码内置默认参数")
        return None


def _get_nested(data: dict, dotted_path: str) -> Any:
    """按'A.B.C'路径从嵌套dict取值，任何一级不存在都返回_MISSING
    （区分"取到了None"和"路径本身不存在"这两种不同情况）。"""
    node = data
    for part in dotted_path.split("."):
        if not isinstance(node, dict) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _deep_merge_dict(default: dict, override: dict) -> dict:
    """递归合并：override里显式给出的key覆盖default，未提到的key
    保留default原值。只对两边都是dict的情况递归，其余情况override
    直接覆盖default（标量、list等）。"""
    merged = dict(default)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _deep_merge_dict(merged[k], v)
        else:
            merged[k] = v
    return merged


def _merge_tiers_list(default_tiers: list, override_tiers: list) -> list:
    """
    screener.py的TIERS专用：list of dict，按'level'字段逐项合并。
    override里只出现T1，不会导致T2-T4从生效的TIERS里消失；
    override里某个tier只写了部分字段（比如只想调vol_mult），
    该tier其余字段保留默认值。默认顺序（T1→T4）保持不变，
    override里如果出现默认没有的新level，追加在末尾。
    """
    by_level = {t.get("level"): dict(t) for t in default_tiers}
    order = [t.get("level") for t in default_tiers]
    for ot in override_tiers:
        lv = ot.get("level")
        if lv is None:
            continue
        if lv in by_level:
            by_level[lv] = _deep_merge_dict(by_level[lv], ot)
        else:
            by_level[lv] = dict(ot)
            order.append(lv)
    return [by_level[lv] for lv in order]


def apply_params(
    target_module,
    mapping: dict,
    log: logging.Logger,
    params_path: Optional[str] = None,
    hard_bounds: Optional[dict] = None,
    telegram_on_change: Optional[Callable[[str], None]] = None,
    state_tag: str = "default",
) -> dict:
    """
    核心入口。

    mapping格式：{"本地属性名": "JSON里的路径（用.分隔，支持嵌套）"}
        例如 {"RISK_PER_TRADE": "PRODUCTION_RISK_PARAMS.RISK_PER_TRADE"}
        本地属性名和JSON路径的最后一段可以不同名——这是为了让
        daily_analysis.py和intraday_monitor.py里名字不同但语义
        相同的常量（比如PULLBACK_MIN_DEPTH_PCT vs
        MODE4_PULLBACK_MIN_DEPTH_PCT）能指向params.json里同一段
        配置，从根本上消除"两份独立实现必须手动保持一致"的风险。

    hard_bounds格式（可选）：{"本地属性名": (下限, 上限)}，均为
        不含边界的开区间校验。超出范围的字段：不应用该字段的
        override（保留代码默认值），记error（若提供
        telegram_on_change，变更比对里会带上这条拒绝记录），
        不影响其余字段正常应用。

    返回值：应用报告dict：
        applied: {属性名: (旧值, 新值)}，实际生效的覆盖
        skipped_missing: [属性名, ...]，JSON里没有对应路径
        skipped_invalid: [(属性名, 原因), ...]，类型不对/超范围/
                          mapping指向的属性在模块里不存在
        params_hash: 本次params.json内容的sha256前12位
                     （文件不存在/不可用时为None）
    """
    if params_path is None:
        params_path = default_params_path(target_module.__file__)

    report = {
        "applied": {}, "skipped_missing": [], "skipped_invalid": [],
        "params_hash": None,
    }

    data = load_params_json(params_path, log)
    if data is None:
        _maybe_alert_on_change(target_module.__file__, state_tag, None,
                                report, telegram_on_change, log)
        return report

    report["params_hash"] = hashlib.sha256(
        json.dumps(data, sort_keys=True).encode("utf-8")
    ).hexdigest()[:12]

    hard_bounds = hard_bounds or {}

    for attr_name, json_path in mapping.items():
        if not hasattr(target_module, attr_name):
            reason = "mapping指向的属性在模块里不存在（拼写错误或mapping与代码不同步）"
            report["skipped_invalid"].append((attr_name, reason))
            log.warning(
                f"param_loader: mapping里的属性'{attr_name}'在"
                f"{getattr(target_module, '__name__', '?')}里不存在，跳过"
                f"（可能是拼写错误，或者mapping和代码已经不同步，请检查）"
            )
            continue

        override_val = _get_nested(data, json_path)
        if override_val is _MISSING:
            report["skipped_missing"].append(attr_name)
            continue

        default_val = getattr(target_module, attr_name)

        if attr_name in hard_bounds:
            lo, hi = hard_bounds[attr_name]
            try:
                num_val = float(override_val)
            except (TypeError, ValueError):
                reason = f"期望数值，实际是{type(override_val).__name__}"
                report["skipped_invalid"].append((attr_name, reason))
                log.error(f"param_loader: [{attr_name}] {reason}，"
                          f"回退默认值{default_val}")
                continue
            if not (lo < num_val < hi):
                reason = f"值{num_val}超出安全范围({lo}, {hi})"
                report["skipped_invalid"].append((attr_name, reason))
                log.error(
                    f"param_loader: [{attr_name}] {reason}，"
                    f"回退默认值{default_val}，请检查params.json是否手滑写错"
                )
                continue

        try:
            if isinstance(default_val, dict) and isinstance(override_val, dict):
                new_val = _deep_merge_dict(default_val, override_val)
            elif (attr_name == "TIERS" and isinstance(default_val, list)
                  and isinstance(override_val, list)):
                new_val = _merge_tiers_list(default_val, override_val)
            elif isinstance(default_val, dict) != isinstance(override_val, dict):
                reason = (f"类型不匹配：默认是{type(default_val).__name__}，"
                          f"JSON里是{type(override_val).__name__}")
                report["skipped_invalid"].append((attr_name, reason))
                log.error(f"param_loader: [{attr_name}] {reason}，回退默认值")
                continue
            else:
                new_val = override_val

            setattr(target_module, attr_name, new_val)
            report["applied"][attr_name] = (default_val, new_val)
        except Exception as e:
            reason = f"应用异常: {e}"
            report["skipped_invalid"].append((attr_name, reason))
            log.error(f"param_loader: [{attr_name}] {reason}，回退默认值")

    if report["applied"]:
        log.info(
            f"param_loader: 生效覆盖 {len(report['applied'])} 项 "
            f"(hash={report['params_hash']}): {list(report['applied'].keys())}"
        )
    else:
        log.info(
            f"param_loader: params.json存在但无匹配字段可覆盖"
            f"(hash={report['params_hash']})，全部使用代码内置默认值"
        )

    if report["skipped_invalid"]:
        log.warning(
            f"param_loader: {len(report['skipped_invalid'])} 项被拒绝"
            f"（类型/范围不对，已回退默认值）: {report['skipped_invalid']}"
        )

    _maybe_alert_on_change(target_module.__file__, state_tag,
                            report["params_hash"], report,
                            telegram_on_change, log)

    return report


def _maybe_alert_on_change(caller_file: str, state_tag: str,
                            current_hash: Optional[str], report: dict,
                            telegram_on_change: Optional[Callable[[str], None]],
                            log: logging.Logger) -> None:
    """
    仅当params.json内容的hash较上次运行发生变化时才推送Telegram，
    避免intraday_monitor.py每15分钟一次的轮询刷屏。"文件不存在/
    不可用"(hash=None)也纳入比对——从"有效"变成"不可用"、或反过来，
    都应该提醒；hash不变则完全静默，不发消息。
    """
    if telegram_on_change is None:
        return
    state_path = _default_state_path(caller_file, state_tag)
    prev_hash = None
    try:
        if os.path.exists(state_path):
            with open(state_path, "r", encoding="utf-8") as f:
                prev_hash = json.load(f).get("hash")
    except Exception:
        prev_hash = None

    if prev_hash == current_hash:
        return

    try:
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump({"hash": current_hash}, f)
    except Exception as e:
        log.warning(f"param_loader: 写入参数状态文件失败: {e}")

    applied_n = len(report.get("applied", {}))
    if current_hash is None:
        msg = "⚙️ params.json不可用（缺失/损坏），已回退全部默认参数"
    elif prev_hash is None:
        msg = f"⚙️ params.json已生效，覆盖{applied_n}项参数（hash={current_hash}）"
    else:
        msg = (f"⚙️ params.json内容发生变化（hash {prev_hash}→{current_hash}），"
               f"当前覆盖{applied_n}项参数")

    invalid = report.get("skipped_invalid")
    if invalid:
        msg += f"\n⚠️ {len(invalid)}项被拒绝（超出安全范围/类型不对）：{invalid}"

    telegram_on_change(msg)
