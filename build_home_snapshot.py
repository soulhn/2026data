"""홈 대시보드 정적 스냅샷 생성 스크립트.

1기~25기 전 과정 종료(2026-07-03)로 홈 화면 수치는 확정 상태.
DB를 매번 조회하는 대신 이 스크립트로 data/home_snapshot.json을 생성해 커밋하고,
home.py는 해당 파일만 읽어 렌더링한다.

재실행 시점: 취업률 확정(HRD-Net 'B' 코드 해제, 2026년 말~2027년 초 예상) 후 1회.

    python build_home_snapshot.py
"""
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import CacheKey
from utils import (
    load_data, load_cache_json,
    calc_attendance_rate, NOT_ATTEND_STATUSES, _attendance_penalty,
    calc_employment_rate_6, calc_recruit_rate,
)

SNAPSHOT_PATH = Path(__file__).parent / "data" / "home_snapshot.json"

# 원장(finalD 핵심수치_원장.md) 확정 벤치마크 (2026-07-08 확정).
# 시장 데이터가 매일 증가해 재계산값이 드리프트하므로(실측: 6일 만에 60.5 → 60.4)
# 이력서·원장과의 일치를 위해 확정값으로 고정한다. 재생성 시 main()이
# 재계산값과의 차이를 출력하므로 드리프트 폭만 확인하면 된다.
LEDGER_BENCHMARK = {'mkt_recruit': 60.5, 'mkt_satis': 85.7, 'our_satis': 90.3}

# 누적 총 매출 원장 확정값 (2026-07-08 실측 104.1억).
# 이후 캐시 미세 재계산으로 합계가 104.0996억이 되어 기존 버림 표시식으로는
# 104.0으로 떨어짐 → 원장 표기와 어긋나므로 헤드라인은 확정값으로 고정.
LEDGER_REVENUE_EOK = 104.1


def build_courses():
    """기수별 과정 마스터 + 파생 지표 (수료율·6개월 합산 취업률·상태)."""
    df = load_data("""
        SELECT TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT,
               TOT_FXNUM, TOT_PAR_MKS, FINI_CNT,
               EI_EMPL_RATE_3, EI_EMPL_RATE_6, HRD_EMPL_RATE_6
        FROM TB_COURSE_MASTER ORDER BY TR_STA_DT DESC
    """)
    for col in ['TOT_FXNUM', 'TOT_PAR_MKS', 'FINI_CNT']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    # 취업률: NaN 유지 (상태코드 'A'=개설예정 'B'=집계중 'C'=미실시 'D'=수료자없음 → 0과 구분)
    for col in ['EI_EMPL_RATE_3', 'EI_EMPL_RATE_6', 'HRD_EMPL_RATE_6']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['TOTAL_RATE_6'] = df.apply(
        lambda r: calc_employment_rate_6(r['EI_EMPL_RATE_6'], r['HRD_EMPL_RATE_6']), axis=1)
    df['수료율'] = (df['FINI_CNT'] / df['TOT_PAR_MKS'].replace(0, pd.NA) * 100).fillna(0)
    today = pd.Timestamp(datetime.now().date())
    df['상태'] = pd.to_datetime(df['TR_END_DT']).apply(
        lambda x: '진행중' if x >= today else '종료')
    df = df[['TRPR_DEGR', 'TRPR_NM', 'TR_STA_DT', 'TR_END_DT', 'TOT_FXNUM',
             'TOT_PAR_MKS', 'FINI_CNT', 'EI_EMPL_RATE_3', 'TOTAL_RATE_6', '수료율', '상태']]
    # NaN → None (JSON null)
    return json.loads(df.to_json(orient='records', force_ascii=False))


def build_attendance():
    """종료 기수별 출결 통계 — ETL 사전 집계 캐시 우선, 미적중 시 원본 집계."""
    cached = load_cache_json(CacheKey.ATTENDANCE_STATS)
    if cached:
        return cached

    today_str = datetime.now().strftime('%Y-%m-%d')
    att_df = load_data(
        "SELECT a.TRPR_DEGR, a.TRNEE_ID, a.ATEND_STATUS, a.ATEND_DT "
        "FROM TB_ATTENDANCE_LOG a "
        "INNER JOIN TB_COURSE_MASTER c ON a.TRPR_ID = c.TRPR_ID AND a.TRPR_DEGR = c.TRPR_DEGR "
        "WHERE c.TR_END_DT < ?",
        params=[today_str],
    )
    if att_df.empty:
        return []

    def _cohort_stats(grp):
        student_rates = [
            calc_attendance_rate(s_grp, raw=True)
            for _, s_grp in grp.groupby('TRNEE_ID')
        ]
        avg_rate = sum(student_rates) / len(student_rates) if student_rates else 0.0
        present = grp[~grp['ATEND_STATUS'].isin(NOT_ATTEND_STATUSES)]
        penalty = int(present['ATEND_STATUS'].apply(_attendance_penalty).sum())
        present_days = max(0, len(present) - penalty // 3)
        return pd.Series({'ATT_RATE': round(avg_rate, 1), 'PRESENT_DAYS': present_days})

    stats = att_df.groupby('TRPR_DEGR').apply(
        _cohort_stats, include_groups=False).reset_index()
    return json.loads(stats.to_json(orient='records', force_ascii=False))


def build_benchmark():
    """전국 KDT 벤치마크(모집률·만족도)와 자사 만족도."""
    bench = {'kdt_cnt': 0, 'mkt_recruit': None, 'mkt_satis': None, 'our_satis': None}

    df_kdt = load_data(
        "SELECT TOT_FXNUM, REG_COURSE_MAN, STDG_SCOR FROM TB_MARKET_TREND "
        "WHERE TRAIN_TARGET = ?",
        params=['K-디지털 트레이닝'],
    )
    if not df_kdt.empty:
        bench['kdt_cnt'] = len(df_kdt)
        fx = pd.to_numeric(df_kdt['TOT_FXNUM'], errors='coerce')
        df_fx = df_kdt[fx > 0]
        if not df_fx.empty:
            reg = pd.to_numeric(df_fx['REG_COURSE_MAN'], errors='coerce').fillna(0)
            bench['mkt_recruit'] = round(calc_recruit_rate(
                reg, pd.to_numeric(df_fx['TOT_FXNUM'], errors='coerce')).mean(), 2)
        scor = pd.to_numeric(df_kdt['STDG_SCOR'], errors='coerce')
        scor = scor[scor > 0]
        if not scor.empty:
            bench['mkt_satis'] = round(scor.mean() / 100, 2)

    df_our = load_data(
        "SELECT STDG_SCOR FROM TB_MARKET_TREND "
        "WHERE TRPR_ID IN (SELECT DISTINCT TRPR_ID FROM TB_COURSE_MASTER)"
    )
    if not df_our.empty:
        our = pd.to_numeric(df_our['STDG_SCOR'], errors='coerce')
        our = our[our > 0]
        if not our.empty:
            bench['our_satis'] = round(our.mean() / 100, 2)
    return bench


def main():
    bench_calc = build_benchmark()
    snapshot = {
        'generated_at': datetime.now().strftime('%Y-%m-%d'),
        'courses': build_courses(),
        'attendance': build_attendance(),
        'benchmark': {**bench_calc, **LEDGER_BENCHMARK},
        'kpi_revenue_eok': LEDGER_REVENUE_EOK,
        'revenue': load_cache_json(CacheKey.REVENUE_ALL_TERMS) or [],
    }
    SNAPSHOT_PATH.parent.mkdir(exist_ok=True)
    with open(SNAPSHOT_PATH, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    # 원장(핵심수치) 대조용 요약 출력
    df = pd.DataFrame(snapshot['courses'])
    bench = snapshot['benchmark']
    trainees, fxnum = df['TOT_PAR_MKS'].sum(), df['TOT_FXNUM'].sum()
    fini = df['FINI_CNT'].sum()
    att = pd.DataFrame(snapshot['attendance'])
    print(f"저장: {SNAPSHOT_PATH} ({SNAPSHOT_PATH.stat().st_size:,} bytes)")
    print(f"기수 {len(df)}개 · 수강 {trainees:,}/{fxnum:,}명 (모집률 {trainees/fxnum*100:.1f}%)")
    print(f"수료율 {fini/trainees*100:.1f}% · 평균 출석률 {att['ATT_RATE'].mean():.1f}%")
    print(f"만족도 {bench['our_satis']:.1f} (전국 {bench['mkt_satis']:.1f}) · "
          f"전국 KDT 모집률 {bench['mkt_recruit']:.1f}% · KDT {bench['kdt_cnt']:,}개 과정")
    print("원장 고정값 vs 재계산값 (드리프트 확인용):")
    for key, ledger in LEDGER_BENCHMARK.items():
        calc = bench_calc.get(key)
        same = calc is not None and round(calc, 1) == ledger
        print(f"  {key}: 고정 {ledger} {'=' if same else '≠'} 재계산 {calc}")
    rev_sum_eok = sum(r['actual_fee'] for r in snapshot['revenue']) / 1e8
    same = round(rev_sum_eok, 1) == LEDGER_REVENUE_EOK
    print(f"  revenue_eok: 고정 {LEDGER_REVENUE_EOK} {'=' if same else '≠'} 재계산 {rev_sum_eok:.4f}")


if __name__ == '__main__':
    main()
