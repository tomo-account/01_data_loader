"""
営業日・日付変換ユーティリティ
"""
import datetime


def today() -> datetime.date:
    return datetime.date.today()


def latest_business_day(d: datetime.date | None = None) -> datetime.date:
    """最新の営業日（平日→当日、土→金、日→金）"""
    if d is None:
        d = today()
    while d.weekday() >= 5:  # 5=土, 6=日
        d -= datetime.timedelta(days=1)
    return d


def prev_business_day(d: datetime.date | None = None) -> datetime.date:
    """直前の営業日（土→金、日→金）"""
    if d is None:
        d = today()
    d -= datetime.timedelta(days=1)
    while d.weekday() >= 5:  # 5=土, 6=日
        d -= datetime.timedelta(days=1)
    return d


def date_range(start: str, end: str) -> list[datetime.date]:
    """'YYYY-MM-DD' 文字列 → 営業日リスト（土日除く）"""
    s = datetime.date.fromisoformat(start)
    e = datetime.date.fromisoformat(end)
    result = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:
            result.append(cur)
        cur += datetime.timedelta(days=1)
    return result


def parse_date(s: str) -> datetime.date:
    return datetime.date.fromisoformat(s)
