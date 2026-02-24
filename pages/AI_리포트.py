import json
import streamlit as st
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import (
    load_data, check_password, get_gemini_api_key,
    calc_attendance_rate_from_counts, calc_employment_rate_6, parse_empl_rate,
    is_completed, calculate_age_at_training, load_cache_json,
    page_error_boundary,
)
from config import (
    CACHE_TTL_DEFAULT, GEMINI_MODEL, CACHE_TTL_AI_REPORT,
    AI_REPORT_MAX_TOKENS, TRNEE_TYPE_MAP, RISK_ABSENT, RISK_LATE,
    RISK_EARLY_LEAVE, CacheKey,
)

st.set_page_config(page_title="AI 리포트", page_icon="🤖", layout="wide")
check_password()
with page_error_boundary():
    st.title("🤖 AI 리포트")
    st.markdown("AI가 훈련 과정 데이터를 분석하여 **성과 리포트**를 자동 생성합니다.")


    # ── 데이터 수집 ──

    @st.cache_data(ttl=CACHE_TTL_DEFAULT)
    def get_all_courses():
        return load_data(
            "SELECT TRPR_ID, TRPR_DEGR, TRPR_NM, TR_STA_DT, TR_END_DT, "
            "TOT_FXNUM, TOT_PAR_MKS, TOT_TRP_CNT, FINI_CNT, "
            "EI_EMPL_RATE_3, EI_EMPL_RATE_6, HRD_EMPL_RATE_6 "
            "FROM TB_COURSE_MASTER ORDER BY CAST(TRPR_DEGR AS INTEGER)"
        )


    @st.cache_data(ttl=CACHE_TTL_DEFAULT)
    def get_cohort_trainees(degr):
        return load_data(
            "SELECT TRNEE_ID, TRNEE_NM, TRNEE_STATUS, TRNEE_TYPE, BIRTH_DATE "
            "FROM TB_TRAINEE_INFO WHERE TRPR_DEGR = ?",
            params=[degr],
        )


    @st.cache_data(ttl=CACHE_TTL_DEFAULT)
    def get_cohort_attendance_stats(degr):
        return load_data(
            "SELECT TRNEE_ID, "
            "COUNT(*) as TOTAL_CNT, "
            "SUM(CASE WHEN ATEND_STATUS = '출석' THEN 1 ELSE 0 END) as PRESENT_CNT, "
            "SUM(CASE WHEN ATEND_STATUS = '결석' THEN 1 ELSE 0 END) as ABSENT_CNT, "
            "SUM(CASE WHEN ATEND_STATUS = '지각' THEN 1 ELSE 0 END) as LATE_CNT, "
            "SUM(CASE WHEN ATEND_STATUS = '조퇴' THEN 1 ELSE 0 END) as EARLY_LEAVE_CNT, "
            "SUM(CASE WHEN ATEND_STATUS = '외출' THEN 1 ELSE 0 END) as OUT_CNT, "
            "SUM(CASE WHEN ATEND_STATUS = '중도탈락미출석' THEN 1 ELSE 0 END) as DROPOUT_ABSENT_CNT, "
            "SUM(CASE WHEN ATEND_STATUS = '100분의50미만출석' THEN 1 ELSE 0 END) as LT50_CNT "
            "FROM TB_ATTENDANCE_LOG WHERE TRPR_DEGR = ? GROUP BY TRNEE_ID",
            params=[degr],
        )


    def collect_cohort_data(degr):
        """기수별 리포트용 데이터 수집 → dict 반환"""
        courses = get_all_courses()
        course = courses[courses['TRPR_DEGR'].astype(str) == str(degr)]
        if course.empty:
            return None

        c = course.iloc[0]
        degr = int(degr)
        trainees = get_cohort_trainees(degr)
        att_stats = get_cohort_attendance_stats(degr)

        # 과정 기본 정보
        data = {
            "과정명": str(c.get('TRPR_NM', '')),
            "기수": int(degr),
            "훈련기간": f"{c['TR_STA_DT']} ~ {c['TR_END_DT']}",
            "정원": int(c.get('TOT_FXNUM', 0)),
            "수강인원": int(c.get('TOT_PAR_MKS', 0)),
        }

        # 수료 현황
        if not trainees.empty:
            completed = is_completed(trainees['TRNEE_STATUS']).sum()
            total = len(trainees)
            dropout = trainees['TRNEE_STATUS'].str.contains('중도탈락', na=False).sum()
            data["수료현황"] = {
                "총원": total,
                "수료": int(completed),
                "중도탈락": int(dropout),
                "수료율": round(completed / total * 100, 1) if total > 0 else 0,
            }

            # 상태 분포
            status_dist = trainees['TRNEE_STATUS'].value_counts().to_dict()
            data["상태분포"] = {str(k): int(v) for k, v in status_dist.items()}

            # 훈련생 유형 분포
            type_mapped = trainees['TRNEE_TYPE'].map(TRNEE_TYPE_MAP).fillna(trainees['TRNEE_TYPE'])
            type_dist = type_mapped.value_counts().to_dict()
            data["유형분포"] = {str(k): int(v) for k, v in type_dist.items()}

            # 연령대 분포
            start_date = c.get('TR_STA_DT', '')
            ages = trainees['BIRTH_DATE'].apply(
                lambda x: calculate_age_at_training(x, start_date)
            )
            age_groups = ages.dropna().apply(lambda x: f"{int(x // 10 * 10)}대")
            if not age_groups.empty:
                data["연령대분포"] = age_groups.value_counts().to_dict()

        # 출석 현황
        if not att_stats.empty:
            rates = []
            risk_absent = 0
            risk_late = 0
            risk_early = 0
            for _, row in att_stats.iterrows():
                rate = calc_attendance_rate_from_counts(
                    total=int(row['TOTAL_CNT']),
                    absent=int(row['ABSENT_CNT']),
                    dropout_absent=int(row['DROPOUT_ABSENT_CNT']),
                    lt50=int(row['LT50_CNT']),
                    late=int(row['LATE_CNT']),
                    early_leave=int(row['EARLY_LEAVE_CNT']),
                    out=int(row['OUT_CNT']),
                )
                rates.append(rate)
                if int(row['ABSENT_CNT']) >= RISK_ABSENT:
                    risk_absent += 1
                if int(row['LATE_CNT']) >= RISK_LATE:
                    risk_late += 1
                if int(row['EARLY_LEAVE_CNT']) >= RISK_EARLY_LEAVE:
                    risk_early += 1

            avg_rate = round(sum(rates) / len(rates), 1) if rates else 0.0
            total_absent = int(att_stats['ABSENT_CNT'].sum())
            total_late = int(att_stats['LATE_CNT'].sum())
            total_early = int(att_stats['EARLY_LEAVE_CNT'].sum())

            data["출석현황"] = {
                "평균출석률": avg_rate,
                "총결석건수": total_absent,
                "총지각건수": total_late,
                "총조퇴건수": total_early,
            }
            data["위험군"] = {
                "결석위험군": risk_absent,
                "지각위험군": risk_late,
                "조퇴위험군": risk_early,
                "기준": f"결석 {RISK_ABSENT}+, 지각 {RISK_LATE}+, 조퇴 {RISK_EARLY_LEAVE}+",
            }

        # 취업률
        ei3_val, ei3_label = parse_empl_rate(c.get('EI_EMPL_RATE_3'))
        ei6_val, ei6_label = parse_empl_rate(c.get('EI_EMPL_RATE_6'))
        hrd6_val, hrd6_label = parse_empl_rate(c.get('HRD_EMPL_RATE_6'))
        total_6 = calc_employment_rate_6(c.get('EI_EMPL_RATE_6'), c.get('HRD_EMPL_RATE_6'))

        empl_data = {}
        if ei3_label:
            empl_data["3개월취업률"] = ei3_label
        elif ei3_val is not None:
            empl_data["3개월취업률"] = f"{ei3_val}%"

        if ei6_label:
            empl_data["6개월취업률_EI"] = ei6_label
        elif ei6_val is not None:
            empl_data["6개월취업률_EI"] = f"{ei6_val}%"

        if hrd6_label:
            empl_data["6개월취업률_HRD"] = hrd6_label
        elif hrd6_val is not None:
            empl_data["6개월취업률_HRD"] = f"{hrd6_val}%"

        if pd.notna(total_6):
            empl_data["6개월총취업률"] = f"{total_6}%"
        elif not empl_data:
            empl_data["상태"] = "미집계"

        data["취업률"] = empl_data

        return data


    def collect_overall_data():
        """전체 종합 리포트용 데이터 수집 → dict 반환"""
        courses = get_all_courses()
        if courses.empty:
            return None

        today = pd.Timestamp(datetime.now().date())
        courses['TR_STA_DT'] = pd.to_datetime(courses['TR_STA_DT'])
        courses['TR_END_DT'] = pd.to_datetime(courses['TR_END_DT'])

        for col in ['TOT_FXNUM', 'TOT_PAR_MKS', 'TOT_TRP_CNT', 'FINI_CNT']:
            courses[col] = pd.to_numeric(courses[col], errors='coerce').fillna(0)

        courses['상태'] = courses['TR_END_DT'].apply(
            lambda x: '진행중' if x >= today else '종료'
        )

        ended = courses[courses['상태'] == '종료'].copy()
        active = courses[courses['상태'] == '진행중']

        # 기본 현황
        data = {
            "총과정수": len(courses),
            "진행중": len(active),
            "종료": len(ended),
            "누적수강생": int(courses['TOT_PAR_MKS'].sum()),
            "누적수료생": int(courses['FINI_CNT'].sum()),
        }

        # 기수별 수료율
        if not ended.empty:
            ended_rates = []
            for _, r in ended.iterrows():
                par = r['TOT_PAR_MKS']
                fini = r['FINI_CNT']
                rate = round(fini / par * 100, 1) if par > 0 else 0
                ended_rates.append({
                    "기수": int(r['TRPR_DEGR']),
                    "수료율": rate,
                    "수강인원": int(par),
                    "수료인원": int(fini),
                })
            data["기수별수료율"] = ended_rates
            avg_comp = round(sum(r["수료율"] for r in ended_rates) / len(ended_rates), 1)
            data["평균수료율"] = avg_comp

            # 최고/최저 수료율
            best = max(ended_rates, key=lambda x: x["수료율"])
            worst = min(ended_rates, key=lambda x: x["수료율"])
            data["최고수료율"] = {"기수": best["기수"], "수료율": best["수료율"]}
            data["최저수료율"] = {"기수": worst["기수"], "수료율": worst["수료율"]}

        # 취업률 트렌드
        if not ended.empty:
            empl_rates = []
            for _, r in ended.iterrows():
                total_6 = calc_employment_rate_6(r.get('EI_EMPL_RATE_6'), r.get('HRD_EMPL_RATE_6'))
                ei3_val, ei3_label = parse_empl_rate(r.get('EI_EMPL_RATE_3'))
                if pd.notna(total_6) or ei3_val is not None:
                    entry = {"기수": int(r['TRPR_DEGR'])}
                    if ei3_val is not None:
                        entry["3개월취업률"] = ei3_val
                    if pd.notna(total_6):
                        entry["6개월총취업률"] = float(total_6)
                    empl_rates.append(entry)
            if empl_rates:
                data["취업률트렌드"] = empl_rates

        # 출석률 (캐시 활용)
        att_cache = load_cache_json(CacheKey.ATTENDANCE_STATS)
        if att_cache:
            att_df = pd.DataFrame(att_cache)
            if not att_df.empty and 'ATT_RATE' in att_df.columns:
                att_summary = []
                for _, r in att_df.iterrows():
                    att_summary.append({
                        "기수": int(r['TRPR_DEGR']),
                        "출석률": float(r['ATT_RATE']),
                    })
                data["기수별출석률"] = att_summary
                data["평균출석률"] = round(att_df['ATT_RATE'].mean(), 1)

        # 연도별 운영 규모
        courses['YEAR'] = courses['TR_STA_DT'].dt.year
        year_summary = []
        for year, grp in courses.groupby('YEAR'):
            year_summary.append({
                "연도": int(year),
                "과정수": len(grp),
                "수강생수": int(grp['TOT_PAR_MKS'].sum()),
            })
        data["연도별운영규모"] = year_summary

        return data


    # ── Gemini API 호출 ──

    SYSTEM_PROMPT = """당신은 HRD-Net 직업훈련 과정 성과 분석 전문가입니다.
제공된 데이터를 바탕으로 한국어 분석 리포트를 작성하세요.

리포트 형식:
## 요약
- 핵심 인사이트 3-5개 불릿포인트

## 상세 분석
### 1. 수료/출석 현황
### 2. 취업 성과
### 3. 위험 요소 및 개선 제안

## 결론 및 제언

주의사항:
- 수치는 반드시 제공된 데이터 기반으로만 언급
- 추측이나 외부 데이터 참조 금지
- 구체적이고 실행 가능한 제언 포함"""

    SYSTEM_PROMPT_OVERALL = """당신은 HRD-Net 직업훈련 과정 성과 분석 전문가입니다.
제공된 전체 기수 데이터를 바탕으로 종합 분석 리포트를 작성하세요.

리포트 형식:
## 요약
- 핵심 인사이트 3-5개 불릿포인트

## 상세 분석
### 1. 운영 규모 및 추이
### 2. 수료율/출석률 트렌드
### 3. 취업 성과 분석
### 4. 기수 간 비교 (최고/최저 성과)

## 결론 및 제언

주의사항:
- 수치는 반드시 제공된 데이터 기반으로만 언급
- 추측이나 외부 데이터 참조 금지
- 기수 간 비교를 통한 실행 가능한 제언 포함"""


    @st.cache_data(ttl=CACHE_TTL_AI_REPORT, show_spinner=False)
    def generate_report(data_json: str, report_type: str) -> str:
        from google import genai

        api_key = get_gemini_api_key()
        client = genai.Client(api_key=api_key)

        prompt = SYSTEM_PROMPT_OVERALL if report_type == "전체 종합" else SYSTEM_PROMPT

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=f"{prompt}\n\n분석 데이터:\n{data_json}",
            config={"max_output_tokens": AI_REPORT_MAX_TOKENS},
        )
        return response.text


    # ── UI ──

    api_key = get_gemini_api_key()
    if not api_key:
        st.warning(
            "GEMINI_API_KEY가 설정되지 않았습니다. "
            "환경변수 또는 Streamlit secrets에 키를 추가해주세요."
        )
        st.stop()

    courses_df = get_all_courses()
    if courses_df.empty:
        st.info("과정 데이터가 없습니다.")
        st.stop()

    with st.sidebar:
        report_type = st.radio("리포트 유형", ["기수별 분석", "전체 종합"], index=0)

        selected_degr = None
        if report_type == "기수별 분석":
            degr_list = sorted(courses_df['TRPR_DEGR'].astype(int).unique())
            degr_labels = []
            for d in degr_list:
                row = courses_df[courses_df['TRPR_DEGR'].astype(int) == d].iloc[0]
                degr_labels.append(f"{d}기 - {row['TRPR_NM']}")
            selected_idx = st.selectbox(
                "기수 선택",
                range(len(degr_list)),
                format_func=lambda i: degr_labels[i],
            )
            selected_degr = degr_list[selected_idx]

        generate_btn = st.button("리포트 생성", type="primary", use_container_width=True)

    if generate_btn:
        if report_type == "기수별 분석" and selected_degr is not None:
            with st.spinner("데이터를 수집하고 있습니다..."):
                data = collect_cohort_data(selected_degr)
            if data is None:
                st.error("선택한 기수의 데이터를 찾을 수 없습니다.")
            else:
                data_json = json.dumps(data, ensure_ascii=False, indent=2)
                with st.spinner("AI가 리포트를 생성하고 있습니다..."):
                    try:
                        report_text = generate_report(data_json, report_type)
                    except Exception as e:
                        st.error(f"리포트 생성 중 오류가 발생했습니다: {e}")
                        report_text = None

                if report_text:
                    st.markdown(report_text)
                    st.divider()
                    st.download_button(
                        label="리포트 다운로드 (.md)",
                        data=report_text,
                        file_name=f"AI_리포트_{selected_degr}기_{datetime.now().strftime('%Y%m%d_%H%M')}.md",
                        mime="text/markdown",
                    )
        else:
            with st.spinner("전체 데이터를 수집하고 있습니다..."):
                data = collect_overall_data()
            if data is None:
                st.error("과정 데이터를 찾을 수 없습니다.")
            else:
                data_json = json.dumps(data, ensure_ascii=False, indent=2)
                with st.spinner("AI가 리포트를 생성하고 있습니다..."):
                    try:
                        report_text = generate_report(data_json, report_type)
                    except Exception as e:
                        st.error(f"리포트 생성 중 오류가 발생했습니다: {e}")
                        report_text = None

                if report_text:
                    st.markdown(report_text)
                    st.divider()
                    st.download_button(
                        label="리포트 다운로드 (.md)",
                        data=report_text,
                        file_name=f"AI_리포트_전체종합_{datetime.now().strftime('%Y%m%d_%H%M')}.md",
                        mime="text/markdown",
                    )
