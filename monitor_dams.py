import os
import json
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

DATA_FILE = "dams.json"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TELEGRAM_SEND_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

KSA_OFFSET_HOURS = 3


def now_ksa() -> datetime:
    return datetime.utcnow() + timedelta(hours=KSA_OFFSET_HOURS)


def load_dams(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        dams = json.load(f)

    if not isinstance(dams, list):
        raise ValueError("dams.json must contain a list of dams")

    required_fields = {"name", "lat", "lon", "fill_percent"}
    for i, dam in enumerate(dams, start=1):
        missing = required_fields - dam.keys()
        if missing:
            raise ValueError(f"Dam #{i} is missing fields: {sorted(missing)}")

        dam.setdefault("fault", False)
        dam.setdefault("fault_note", "")
        dam.setdefault("gate_status", "normal")   # normal / partial / failed
        dam.setdefault("seepage", "normal")       # normal / observed / critical
        dam.setdefault("notes", "")
        dam.setdefault("alert_fill_threshold", 85)
        dam.setdefault("critical_fill_threshold", 95)

    return dams


def safe_get(url: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_rain_forecast_72h(lat: float, lon: float) -> Tuple[float, List[float]]:
    """
    Returns:
      total_72h_mm, [day1_mm, day2_mm, day3_mm]
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "precipitation_sum",
        "forecast_days": 3,
        "timezone": "Asia/Riyadh",
    }
    data = safe_get(OPEN_METEO_URL, params=params)

    daily = data.get("daily", {})
    values = daily.get("precipitation_sum", [])

    if not values or len(values) < 3:
        return 0.0, [0.0, 0.0, 0.0]

    values = [float(v or 0) for v in values[:3]]
    return round(sum(values), 1), [round(v, 1) for v in values]


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


def score_rain(rain_72h: float) -> int:
    if rain_72h >= 80:
        return 30
    if rain_72h >= 50:
        return 22
    if rain_72h >= 25:
        return 14
    if rain_72h >= 10:
        return 6
    return 0


def score_fault(fault: bool, gate_status: str, seepage: str) -> int:
    score = 0

    if fault:
        score += 18

    if gate_status == "failed":
        score += 25
    elif gate_status == "partial":
        score += 12

    if seepage == "critical":
        score += 25
    elif seepage == "observed":
        score += 10

    return min(score, 40)


def classify(score: int) -> str:
    if score >= 80:
        return "🔴 حرج"
    if score >= 60:
        return "🟠 مرتفع"
    if score >= 30:
        return "🟡 متابعة"
    return "🟢 طبيعي"


def short_reason(fill: float, rain_72h: float, fault: bool, gate_status: str, seepage: str,
                 alert_fill_threshold: float, critical_fill_threshold: float) -> str:
    reasons = []

    if fill >= critical_fill_threshold:
        reasons.append("امتلاء حرج")
    elif fill >= alert_fill_threshold:
        reasons.append("امتلاء مرتفع")

    if rain_72h >= 50:
        reasons.append("أمطار قوية متوقعة")
    elif rain_72h >= 25:
        reasons.append("أمطار متوسطة مؤثرة")

    if fault:
        reasons.append("عطل تشغيلي")

    if gate_status == "failed":
        reasons.append("تعطل بوابة")
    elif gate_status == "partial":
        reasons.append("ضعف جزئي بالبوابة")

    if seepage == "critical":
        reasons.append("مؤشر رشح حرج")
    elif seepage == "observed":
        reasons.append("رصد رشح")

    return " + ".join(reasons) if reasons else "لا توجد مؤشرات حرجة"


def evaluate_dam(dam: Dict[str, Any]) -> Dict[str, Any]:
    rain_72h, rain_days = fetch_rain_forecast_72h(dam["lat"], dam["lon"])

    fill_score = score_fill(
        fill_percent=float(dam["fill_percent"]),
        alert_fill_threshold=float(dam["alert_fill_threshold"]),
        critical_fill_threshold=float(dam["critical_fill_threshold"]),
    )
    rain_score = score_rain(rain_72h)
    fault_score = score_fault(
        fault=bool(dam["fault"]),
        gate_status=str(dam["gate_status"]),
        seepage=str(dam["seepage"]),
    )

    score = min(fill_score + rain_score + fault_score, 100)
    level = classify(score)
    reason = short_reason(
        fill=float(dam["fill_percent"]),
        rain_72h=rain_72h,
        fault=bool(dam["fault"]),
        gate_status=str(dam["gate_status"]),
        seepage=str(dam["seepage"]),
        alert_fill_threshold=float(dam["alert_fill_threshold"]),
        critical_fill_threshold=float(dam["critical_fill_threshold"]),
    )

    return {
        "name": dam["name"],
        "fill_percent": float(dam["fill_percent"]),
        "rain_72h": rain_72h,
        "rain_days": rain_days,
        "fault": bool(dam["fault"]),
        "fault_note": dam.get("fault_note", ""),
        "gate_status": str(dam["gate_status"]),
        "seepage": str(dam["seepage"]),
        "notes": dam.get("notes", ""),
        "score": score,
        "level": level,
        "reason": reason,
        "alert_fill_threshold": float(dam["alert_fill_threshold"]),
        "critical_fill_threshold": float(dam["critical_fill_threshold"]),
    }


def build_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(results)
    critical = sum(1 for r in results if r["score"] >= 80)
    high = sum(1 for r in results if 60 <= r["score"] < 80)
    watch = sum(1 for r in results if 30 <= r["score"] < 60)
    normal = sum(1 for r in results if r["score"] < 30)

    max_score = max((r["score"] for r in results), default=0)
    overall = classify(max_score)

    if critical > 0:
        overall_text = "وجود سدود بحالة حرجة تتطلب متابعة فورية"
    elif high > 0:
        overall_text = "وجود حالات مرتفعة تستوجب رفع الجاهزية"
    elif watch > 0:
        overall_text = "توجد حالات تحت المتابعة الاحترازية"
    else:
        overall_text = "لا توجد مؤشرات حرجة حالياً"

    return {
        "total": total,
        "critical": critical,
        "high": high,
        "watch": watch,
        "normal": normal,
        "overall": overall,
        "overall_text": overall_text,
        "overall_score": max_score,
    }


def build_report(results: List[Dict[str, Any]]) -> str:
    ts = now_ksa().strftime("%Y-%m-%d %H:%M")
    results_sorted = sorted(results, key=lambda x: x["score"], reverse=True)
    summary = build_summary(results_sorted)

    lines = []
    lines.append("📄 تقرير رصد السدود – المملكة العربية السعودية")
    lines.append(f"🕒 {ts} بتوقيت السعودية")
    lines.append("════════════════════")
    lines.append("")
    lines.append("📊 التقييم التنفيذي")
    lines.append(f"📌 مستوى الحالة: {summary['overall']}")
    lines.append(f"📊 مؤشر المخاطر العام: {summary['overall_score']}/100")
    lines.append(f"🧾 التفسير السريع: {summary['overall_text']}")
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
    lines.append("🚨 السدود الأعلى خطورة")
    if results_sorted:
        for r in results_sorted[:5]:
            lines.append(f"{r['level']} {r['name']}")
            lines.append(f"• الامتلاء: {r['fill_percent']:.0f}%")
            lines.append(f"• أمطار 72 ساعة: {r['rain_72h']:.1f} ملم")
            lines.append(f"• الحالة التشغيلية: {'يوجد ملاحظة' if r['fault'] else 'مستقرة'}")
            lines.append(f"• البوابات: {translate_gate_status(r['gate_status'])}")
            lines.append(f"• الرشح: {translate_seepage(r['seepage'])}")
            lines.append(f"• المؤشر: {r['score']}/100")
            lines.append(f"• السبب: {r['reason']}")
            if r["fault_note"]:
                lines.append(f"• ملاحظة العطل: {r['fault_note']}")
            if r["notes"]:
                lines.append(f"• ملاحظات: {r['notes']}")
            lines.append("")
    else:
        lines.append("• لا توجد بيانات.")
        lines.append("")

    lines.append("════════════════════")
    lines.append("")
    lines.append("🧭 التوصية التشغيلية")

    if summary["critical"] > 0:
        lines.append("• تصعيد فوري ومتابعة لحظية للسدود الحرجة")
        lines.append("• التحقق من جاهزية البوابات وفرق التشغيل")
        lines.append("• إشعار الجهات المعنية بالمناطق السفلية عند الحاجة")
    elif summary["high"] > 0:
        lines.append("• رفع الجاهزية ومتابعة كل 3 ساعات")
        lines.append("• مراجعة الأعطال والملاحظات التشغيلية")
        lines.append("• متابعة توقعات الأمطار على الأحواض المغذية")
    elif summary["watch"] > 0:
        lines.append("• استمرار الرصد كل 6 ساعات")
        lines.append("• متابعة السدود تحت المراقبة الاحترازية")
        lines.append("• لا حاجة للتصعيد حالياً")
    else:
        lines.append("• استمرار الرصد الدوري")
        lines.append("• لا توجد حاجة للتصعيد حالياً")

    return "\n".join(lines)


def build_alerts(results: List[Dict[str, Any]]) -> List[str]:
    alerts = []
    ts = now_ksa().strftime("%Y-%m-%d %H:%M")

    for r in results:
        if r["score"] >= 60:
            alert_lines = [
                "🚨 تنبيه سدود",
                f"🕒 {ts} بتوقيت السعودية",
                "════════════════════",
                f"📍 السد: {r['name']}",
                f"📌 الحالة: {r['level']}",
                f"📊 المؤشر: {r['score']}/100",
                f"🧾 السبب: {r['reason']}",
                f"• الامتلاء: {r['fill_percent']:.0f}%",
                f"• أمطار 72 ساعة: {r['rain_72h']:.1f} ملم",
                f"• البوابات: {translate_gate_status(r['gate_status'])}",
                f"• الرشح: {translate_seepage(r['seepage'])}",
            ]
            if r["fault_note"]:
                alert_lines.append(f"• ملاحظة العطل: {r['fault_note']}")
            if r["notes"]:
                alert_lines.append(f"• ملاحظات: {r['notes']}")
            alerts.append("\n".join(alert_lines))

    return alerts


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


def split_message(text: str, limit: int = 3900) -> List[str]:
    if len(text) <= limit:
        return [text]

    chunks = []
    current = []

    for line in text.splitlines(True):
        if sum(len(x) for x in current) + len(line) > limit:
            chunks.append("".join(current).strip())
            current = [line]
        else:
            current.append(line)

    if current:
        chunks.append("".join(current).strip())

    return chunks


def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Telegram secrets are missing")

    for chunk in split_message(text):
        response = requests.post(
            TELEGRAM_SEND_URL,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
            },
            timeout=30,
        )
        response.raise_for_status()


def main() -> None:
    dams = load_dams(DATA_FILE)
    results = [evaluate_dam(dam) for dam in dams]

    report = build_report(results)
    print(report)
    print("\n" + "=" * 60 + "\n")

    send_telegram_message(report)

    alerts = build_alerts(results)
    for alert in alerts:
        print(alert)
        print("\n" + "-" * 60 + "\n")
        send_telegram_message(alert)


if __name__ == "__main__":
    main()
