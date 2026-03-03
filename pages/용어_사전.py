import streamlit as st
import sys
import os
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import check_password, page_error_boundary

st.set_page_config(page_title="용어 사전", page_icon="📖", layout="wide")
check_password()
with page_error_boundary():
    st.title("📖 용어 사전")
    st.caption("대시보드에 표시되는 지표명·단위·표기 기준을 확인합니다.")

    # ── GLOSSARY.md 읽기 & 섹션 분할 ──
    glossary_path = Path(__file__).resolve().parent.parent / "GLOSSARY.md"
    raw = glossary_path.read_text(encoding="utf-8")

    def _extract_section(text: str, header: str, next_headers: list[str]) -> str:
        """## header 부터 다음 ## header 직전까지 추출."""
        start = text.find(f"## {header}")
        if start == -1:
            return ""
        # 헤더 라인 자체는 제외하고 본문만 추출
        start = text.index("\n", start) + 1
        end = len(text)
        for nh in next_headers:
            pos = text.find(f"## {nh}", start)
            if pos != -1:
                end = min(end, pos)
        return text[start:end].strip()

    section_a = _extract_section(raw, "A. UI 표기 규칙", ["B.", "C.", "D."])
    section_b = _extract_section(raw, "B. 전체 지표 정의 & 계산식", ["C.", "D."])
    section_c = _extract_section(raw, "C. 페이지별 용어 카탈로그", ["D."])

    # ── 탭 렌더링 ──
    tab1, tab2, tab3 = st.tabs(["표기 규칙", "지표 정의", "페이지별 용어"])

    with tab1:
        st.markdown(section_a)

    with tab2:
        # ### 단위로 분할하여 expander로 표시
        blocks = section_b.split("\n### ")
        for block in blocks:
            block = block.strip()
            if not block:
                continue
            lines = block.split("\n", 1)
            title = lines[0].strip().lstrip("# ")
            body = lines[1].strip() if len(lines) > 1 else ""
            with st.expander(title, expanded=False):
                st.markdown(body)

    with tab3:
        # ### 단위로 분할하여 expander로 표시
        blocks = section_c.split("\n### ")
        for block in blocks:
            block = block.strip()
            if not block:
                continue
            lines = block.split("\n", 1)
            title = lines[0].strip().lstrip("# ")
            # "pages/시장_분석.py — 시장 분석" → "시장 분석" 형태로 간결하게
            if " — " in title:
                title = title.split(" — ", 1)[1]
            elif "— " in title:
                title = title.split("— ", 1)[1]
            body = lines[1].strip() if len(lines) > 1 else ""
            with st.expander(title, expanded=False):
                st.markdown(body)
