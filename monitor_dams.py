import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

DATA_FILE = "dams.json"
STATE_FILE = "state.json"
LOGS_DIR = Path("logs")
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

KSA_OFFSET_HOURS = 3
ALERT_THRESHOLD = 60


def now_ksa() -> datetime:
    return datetime.utcnow() + timedelta(hours=KSA_OFFSET_HOURS)


def current_mode() -> str:
    hour = now_ksa().hour
    return "morning" if hour < 12 else "evening"


def load_json_file(path: str, default: Any) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        return default
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_dams(path: str) -> List[Dict[str, Any]]:
    dams = load_json_file(path, [])
    if not isinstance(dams, list):
        raise ValueError("dams.json must contain a list")

    required_fields = {"name", "lat", "lon", "fill_percent"}
    for i, dam in enumerate(dams, start=1):
        missing = required_fields - dam.keys()
        if missing:
            raise ValueError(f"Dam #{i} missing fields: {sorted(missing)}")

        dam.setdefault("fault", False)
        dam.setdefault("fault_note", "")
        dam.setdefault("gate_status", "normal")      # normal / partial / failed
        dam.setdefault("seepage", "normal")          # normal / observed / critical
        dam.setdefault("notes", "")
        dam.setdefault("alert_fill_threshold", 85)
        dam.setdefault("critical_fill_threshold", 95)
        dam.setdefault("team_ready", True)
        dam.setdefault("level_change_24h", 0.0)

    return dams


def safe_get(url: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_rain_forecast(lat: float, lon: float) -> Tuple[float, float, List[float]]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "precipitation_sum",
        "forecast_days": 3,
        "timezone": "Asia/Riyadh",
    }
    data = safe_get(OPEN_METEO_URL, params=params)
    values = data.get("daily", {}).get("precipitation_sum", [])

    if not values or len(values) < 3:
        return 0.0, 0.0, [0.0, 0.0, 0.0]

    values = [float(v or 0) for v in values[:3]]
    rain_24h = round(values[0], 1)
    rain_72h = round(sum(values), 1)
    return rain_24h, rain_72h, [round(v, 1) for v in values]


def score_fill(fill_percent: float, alert_fill_threshold: float, critical_fill_threshold: float) -> int:
    if fill_percent >= critical_fill_threshold:
        return 40
    if fill_percent >= alert_fill_threshold:
        return 25
    if fill_percent >= 70:
        return 12
    if fill_percent >= 50:
        return 5
    return 0


def score_rain(rain_24h: float, rain_72h: float) -> int:
    score = 0

    if rain_24h >= 50:
        score += 20
    elif rain_24h >= 25:
        score += 12
    elif rain_24h >= 10:
        score += 5

    if rain_72h >= 80:
        score += 10
    elif rain_72h >= 50:
        score += 7
    elif rain_72h >= 25:
        score += 3

    return min(score, 30)


def score_fault(fault: bool, gate_status: str, seepage: str, team_ready: bool) -> int:
    score = 0

    if fault:
        score += 15

    if gate_status == "failed":
        score += 20
    elif gate_status == "partial":
        score += 10

    if seepage == "critical":
        score += 20
    elif seepage == "observed":
        score += 8

    if not team_ready:
        score += 10

    return min(score, 40)


def score_level_change(level_change_24h: float) -> int:
    change = abs(float(level_change_24h))
    if change >= 10:
        return 15
    if change >= 5:
        return 8
    if change >= 2:
        return 3
    return 0


def classify(score: int) -> str:
    if score >= 80:
        return "حرج"
    if score >= 60:
        return "مرتفع"
    if score >= 30:
        return "متابعة"
    return "طبيعي"


def classify_icon(level: str) -> str:
    mapping = {
        "حرج": "🔴",
        "مرتفع": "🟠",
        "متابعة": "🟡",
        "طبيعي": "🟢",
    }
    return mapping.get(level, "⚪")


def translate_gate_status(value: str) -> str:
    mapping = {
        "normal": "طبيعية",
        "partial": "ضعف جزئي",
        "failed": "متعطلة",
    }
    return mapping.get(value, value)


def translate_seepage(value: str) -> str:
    mapping = {
        "normal": "طبيعي",
        "observed": "ملاحظ",
        "critical": "حرج",
    }
    return mapping.get(value, value)


def quick_reason(dam: Dict[str, Any], rain_24h: float, rain_72h: float) -> str:
    reasons = []

    fill = float(dam["fill_percent"])
    if fill >= float(dam["critical_fill_threshold"]):
        reasons.append("امتلاء حرج")
    elif fill >= float(dam["alert_fill_threshold"]):
        reasons.append("امتلاء مرتفع")

    if rain_24h >= 25:
        reasons.append("أمطار مؤثرة خلال 24 ساعة")
    elif rain_72h >= 25:
        reasons.append("أمطار تراكمية مؤثرة")

    if dam["gate_status"] == "failed":
        reasons.append("تعطل بوابة")
    elif dam["gate_status"] == "partial":
        reasons.append("ضعف بالبوابة")

    if dam["seepage"] == "critical":
        reasons.append("رشح حرج")
    elif dam["seepage"] == "observed":
        reasons.append("رصد رشح")

    if dam["fault"]:
        reasons.append("ملاحظة تشغيلية")

    if not dam["team_ready"]:
        reasons.append("انخفاض الجاهزية")

    if abs(float(dam["level_change_24h"])) >= 5:
        reasons.append("تغير ملحوظ بالمنسوب")

    return " + ".join(reasons) if reasons else "لا توجد مؤشرات حرجة"


def evaluate_dam(dam: Dict[str, Any]) -> Dict[str, Any]:
    rain_24h, rain_72h, rain_days = fetch_rain_forecast(dam["lat"], dam["lon"])

    total_score = min(
        score_fill(
            float(dam["fill_percent"]),
            float(dam["alert_fill_threshold"]),
            float(dam["critical_fill_threshold"]),
        )
        + score_rain(rain_24h, rain_72h)
        + score_fault(
            bool(dam["fault"]),
            str(dam["gate_status"]),
            str(dam["seepage"]),
            bool(dam["team_ready"]),
        )
        + score_level_change(float(dam["level_change_24h"])),
        100,
    )

    level = classify(total_score)

    return {
        "name": dam["name"],
        "fill_percent": float(dam["fill_percent"]),
        "rain_24h": rain_24h,
        "rain_72h": rain_72h,
        "rain_days": rain_days,
        "fault": bool(dam["fault"]),
        "fault_note": str(dam["fault_note"]),
        "gate_status": str(dam["gate_status"]),
        "seepage": str(dam["seepage"]),
        "notes": str(dam["notes"]),
        "team_ready": bool(dam["team_ready"]),
        "level_change_24h": float(dam["level_change_24h"]),
        "score": total_score,
        "level": level,
        "icon": classify_icon(level),
        "reason": quick_reason(dam, rain_24h, rain_72h),
    }


def build_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {"طبيعي": 0, "متابعة": 0, "مرتفع": 0, "حرج": 0}
    max_score = 0

    for r in results:
        counts[r["level"]] += 1
        max_score = max(max_score, r["score"])

    overall = classify(max_score)
    overall_icon = classify_icon(overall)

    if counts["حرج"] > 0:
        summary_text = "وجود حالات حرجة تتطلب متابعة فورية"
    elif counts["مرتفع"] > 0:
        summary_text = "وجود حالات مرتفعة تستوجب رفع الجاهزية"
    elif counts["متابعة"] > 0:
        summary_text = "توجد حالات تحت المتابعة الاحترازية"
    else:
        summary_text = "لا توجد مؤشرات حرجة حالياً"

    return {
        "total": len(results),
        "normal": counts["طبيعي"],
        "watch": counts["متابعة"],
        "high": counts["مرتفع"],
        "critical": counts["حرج"],
        "overall": overall,
        "overall_icon": overall_icon,
        "overall_score": max_score,
        "summary_text": summary_text,
    }


def sort_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    priority = {"حرج": 0, "مرتفع": 1, "متابعة": 2, "طبيعي": 3}
    return sorted(results, key=lambda x: (priority[x["level"]], -x["score"], -x["fill_percent"]))


def build_morning_report(results: List[Dict[str, Any]]) -> str:
    ts = now_ksa().strftime("%Y-%m-%d %H:%M")
    results = sort_results(results)
    summary = build_summary(results)

    lines = []
    lines.append("📄 التقرير الصباحي لرصد السدود – المملكة العربية السعودية")
    lines.append(f"🕒 {ts} بتوقيت السعودية")
    lines.append("════════════════════")
    lines.append("")
    lines.append("📊 التقييم التنفيذي")
    lines.append(f"📌 مستوى الحالة: {summary['overall_icon']} {summary['overall']}")
    lines.append(f"📊 مؤشر المخاطر العام: {summary['overall_score']}/100")
    lines.append(f"🧾 التفسير السريع: {summary['summary_text']}")
    lines.append("")
    lines.append("════════════════════")
    lines.append("")
    lines.append("🏞️ الملخص الوطني")
    lines.append(f"• إجمالي السدود المرصودة: {summary['total']}")
    lines.append(f"• مستقرة: {summary['normal']}")
    lines.append(f"• متابعة: {summary['watch']}")
    lines.append(f"• مرتفعة: {summary['high']}")
    lines.append(f"• حرجة: {summary['critical']}")
    lines.append("")

    top_items = [r for r in results if r["level"] != "طبيعي"][:5]
    if top_items:
        lines.append("🚨 أبرز السدود محل المتابعة")
        for r in top_items:
            lines.append(f"{r['icon']} {r['name']} — {r['level']} ({r['score']}/100)")
            lines.append(f"• السبب: {r['reason']}")
            lines.append(f"• الامتلاء: {r['fill_percent']:.0f}% | أمطار 24 ساعة: {r['rain_24h']:.1f} ملم")
        lines.append("")
    else:
        lines.append("✅ لا توجد سدود تتطلب متابعة خاصة حالياً")
        lines.append("")

    lines.append("🧭 التوصية التشغيلية")
    if summary["critical"] > 0:
        lines.append("• تصعيد فوري ومتابعة لحظية للحالات الحرجة")
        lines.append("• التحقق من الجاهزية التشغيلية والبوابات")
    elif summary["high"] > 0:
        lines.append("• رفع الجاهزية ومتابعة كل 3 ساعات")
        lines.append("• مراجعة الملاحظات التشغيلية فوراً")
    elif summary["watch"] > 0:
        lines.append("• استمرار الرصد كل 6 ساعات")
        lines.append("• متابعة السدود تحت المراقبة")
    else:
        lines.append("• استمرار الرصد الدوري")
        lines.append("• لا حاجة للتصعيد حالياً")

    return "\n".join(lines)


def build_evening_report(results: List[Dict[str, Any]]) -> str:
    ts = now_ksa().strftime("%Y-%m-%d %H:%M")
    results = sort_results(results)
    summary = build_summary(results)

    lines = []
    lines.append("📄 التقرير المسائي لرصد السدود – المملكة العربية السعودية")
    lines.append(f"🕒 {ts} بتوقيت السعودية")
    lines.append("════════════════════")
    lines.append("")
    lines.append("📊 التقييم التنفيذي")
    lines.append(f"📌 مستوى الحالة: {summary['overall_icon']} {summary['overall']}")
    lines.append(f"📊 مؤشر المخاطر العام: {summary['overall_score']}/100")
    lines.append(f"🧾 التفسير السريع: {summary['summary_text']}")
    lines.append("")
    lines.append("════════════════════")
    lines.append("")
    lines.append("🏞️ ملخص الرصد الوطني")
    lines.append(f"• إجمالي السدود المرصودة: {summary['total']}")
    lines.append(f"• السدود المستقرة: {summary['normal']}")
    lines.append(f"• تحت المتابعة: {summary['watch']}")
    lines.append(f"• تنبيه مرتفع: {summary['high']}")
    lines.append(f"• تنبيه حرج: {summary['critical']}")
    lines.append("")
    lines.append("════════════════════")
    lines.append("")

    for r in results:
        lines.append(f"{r['icon']} {r['name']}")
        lines.append(f"• الحالة: {r['level']}")
        lines.append(f"• المؤشر: {r['score']}/100")
        lines.append(f"• الامتلاء: {r['fill_percent']:.0f}%")
        lines.append(f"• أمطار 24 ساعة: {r['rain_24h']:.1f} ملم")
        lines.append(f"• أمطار 72 ساعة: {r['rain_72h']:.1f} ملم")
        lines.append(f"• تغير المنسوب 24 ساعة: {r['level_change_24h']:+.1f}%")
        lines.append(f"• البوابات: {translate_gate_status(r['gate_status'])}")
        lines.append(f"• الرشح: {translate_seepage(r['seepage'])}")
        lines.append(f"• الجاهزية الميدانية: {'جاهزة' if r['team_ready'] else 'تحتاج دعم'}")
        lines.append(f"• السبب: {r['reason']}")
        if r["fault_note"]:
            lines.append(f"• ملاحظة العطل: {r['fault_note']}")
        if r["notes"]:
            lines.append(f"• ملاحظات: {r['notes']}")
        lines.append("")

    lines.append("════════════════════")
    lines.append("")
    lines.append("🧭 التوصية التشغيلية")
    if summary["critical"] > 0:
        lines.append("• تصعيد فوري ومتابعة لحظية للحالات الحرجة")
        lines.append("• إشعار الجهات المعنية عند الحاجة")
        lines.append("• التحقق الميداني العاجل من العناصر التشغيلية")
    elif summary["high"] > 0:
        lines.append("• رفع الجاهزية ومتابعة كل 3 ساعات")
        lines.append("• معالجة الملاحظات التشغيلية ذات الأولوية")
        lines.append("• متابعة توقعات الأمطار على الأحواض المغذية")
    elif summary["watch"] > 0:
        lines.append("• استمرار الرصد كل 6 ساعات")
        lines.append("• متابعة السدود تحت المراقبة الاحترازية")
        lines.append("• لا حاجة للتصعيد حالياً")
    else:
        lines.append("• استمرار الرصد الدوري")
        lines.append("• لا توجد حاجة للتصعيد حالياً")

    return "\n".join(lines)


def build_alert_message(result: Dict[str, Any]) -> str:
    ts = now_ksa().strftime("%Y-%m-%d %H:%M")
    lines = []
    lines.append("🚨 تنبيه فوري لرصد السدود")
    lines.append(f"🕒 {ts} بتوقيت السعودية")
    lines.append("════════════════════")
    lines.append(f"📍 السد: {result['name']}")
    lines.append(f"📌 الحالة: {result['icon']} {result['level']}")
    lines.append(f"📊 المؤشر: {result['score']}/100")
    lines.append(f"🧾 السبب: {result['reason']}")
    lines.append(f"• الامتلاء: {result['fill_percent']:.0f}%")
    lines.append(f"• أمطار 24 ساعة: {result['rain_24h']:.1f} ملم")
    lines.append(f"• أمطار 72 ساعة: {result['rain_72h']:.1f} ملم")
    lines.append(f"• تغير المنسوب 24 ساعة: {result['level_change_24h']:+.1f}%")
    lines.append(f"• البوابات: {translate_gate_status(result['gate_status'])}")
    lines.append(f"• الرشح: {translate_seepage(result['seepage'])}")
    lines.append(f"• الجاهزية: {'جاهزة' if result['team_ready'] else 'تحتاج دعم'}")
    if result["fault_note"]:
        lines.append(f"• ملاحظة العطل: {result['fault_note']}")
    if result["notes"]:
        lines.append(f"• ملاحظات: {result['notes']}")
    return "\n".join(lines)


def split_message(text: str, limit: int = 3900) -> List[str]:
    if len(text) <= limit:
        return [text]

    parts = []
    current = []
    current_len = 0

    for line in text.splitlines(True):
        if current_len + len(line) > limit:
            parts.append("".join(current).strip())
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line)

    if current:
        parts.append("".join(current).strip())

    return parts


def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Missing Telegram secrets")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    for chunk in split_message(text):
        response = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
            },
            timeout=30,
        )
        response.raise_for_status()


def load_state() -> Dict[str, Any]:
    return load_json_file(STATE_FILE, {"alerts": {}, "last_run": ""})


def save_state(state: Dict[str, Any]) -> None:
    save_json_file(STATE_FILE, state)


def should_send_alert(result: Dict[str, Any], state: Dict[str, Any]) -> bool:
    if result["score"] < ALERT_THRESHOLD:
        return False

    alerts_state = state.setdefault("alerts", {})
    last = alerts_state.get(result["name"])

    current_signature = {
        "level": result["level"],
        "score": result["score"],
        "reason": result["reason"],
    }

    if last == current_signature:
        return False

    alerts_state[result["name"]] = current_signature
    return True


def clear_resolved_alerts(results: List[Dict[str, Any]], state: Dict[str, Any]) -> None:
    active_names = {r["name"] for r in results if r["score"] >= ALERT_THRESHOLD}
    alerts_state = state.setdefault("alerts", {})
    for name in list(alerts_state.keys()):
        if name not in active_names:
            del alerts_state[name]


def append_daily_log(results: List[Dict[str, Any]]) -> None:
    LOGS_DIR.mkdir(exist_ok=True)
    log_file = LOGS_DIR / f"dams_log_{now_ksa().strftime('%Y-%m-%d')}.csv"
    is_new = not log_file.exists()

    with open(log_file, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow([
                "timestamp_ksa",
                "name",
                "level",
                "score",
                "fill_percent",
                "rain_24h",
                "rain_72h",
                "level_change_24h",
                "gate_status",
                "seepage",
                "team_ready",
                "fault",
                "reason",
            ])

        timestamp = now_ksa().strftime("%Y-%m-%d %H:%M")
        for r in results:
            writer.writerow([
                timestamp,
                r["name"],
                r["level"],
                r["score"],
                r["fill_percent"],
                r["rain_24h"],
                r["rain_72h"],
                r["level_change_24h"],
                r["gate_status"],
                r["seepage"],
                r["team_ready"],
                r["fault"],
                r["reason"],
            ])


def build_report(results: List[Dict[str, Any]]) -> str:
    mode = current_mode()
    if mode == "morning":
        return build_morning_report(results)
    return build_evening_report(results)


def main() -> None:
    dams = load_dams(DATA_FILE)
    results = [evaluate_dam(dam) for dam in dams]
    state = load_state()

    report = build_report(results)
    print(report)
    send_telegram_message(report)

    for result in sort_results(results):
        if should_send_alert(result, state):
            alert_message = build_alert_message(result)
            print("\n" + "-" * 60)
            print(alert_message)
            send_telegram_message(alert_message)

    clear_resolved_alerts(results, state)
    state["last_run"] = now_ksa().isoformat()

    append_daily_log(results)
    save_state(state)


if __name__ == "__main__":
    main()
