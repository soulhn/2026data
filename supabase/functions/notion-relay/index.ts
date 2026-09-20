// 노션 API 웹훅 수신 → 최종결과가 HRD등록(등 지정 상태)으로 바뀐 페이지면 GitHub Actions `kpi_poll.yml`을 즉시 실행.
//
// 흐름: 담당자가 최종결과 변경 → 노션이 몇 초 안에 이 함수로 POST (page.properties_updated, 값은 없고 바뀐 속성 ID만)
//       → 이 함수가 그 페이지의 최종결과를 API로 읽어 트리거 상태인지 확인 → workflow_dispatch → 파이프라인(약 2분)이 전이 로그·명부 대조·디스코드
// 개인정보: 페이지에서 최종결과 속성 하나만 읽는다(filter_properties). 이름·연락처는 요청하지도 저장하지도 않는다.
//
// 배포: Supabase 대시보드 › Edge Functions › `notion-relay` › 이 코드 › Deploy · "Verify JWT" 끔 (노션은 JWT를 못 보냄)
// 시크릿: NOTION_TOKEN (통합 sul) · GITHUB_TOKEN (fine-grained, soulhn/2026data, Actions: Read and write)
//         NOTION_VERIFICATION_TOKEN (노션 웹훅 구독 만들 때 처음 오는 값 — 함수 로그에 찍힌다. 그 뒤 서명 검증 키로 쓴다)
//         TRIGGER_STATUSES (선택, 기본 "HRD등록". 쉼표로 여러 개: "HRD등록,합격취소(신청자 요청)")
const OWNER_REPO = "soulhn/2026data";
const WORKFLOW = "kpi_poll.yml";
const NOTION_VERSION = "2022-06-28";
const APPLICANTS_DB = ["375d943bcac280e7ba18cd107d5e40d2", "375d943bcac2804499a6000bf5cd7ad5"]; // DB ID · 데이터 소스 ID (2025-09 API)
const STATUS_PROP_ID = "HnnX";        // 신청자 리스트 '최종결과' 속성 ID
const STATUS_PROP_NAME = "최종결과";

const norm = (id: string) => (id ?? "").replace(/-/g, "");
const encoder = new TextEncoder();

async function validSignature(raw: string, header: string | null): Promise<boolean> {
  const secret = Deno.env.get("NOTION_VERIFICATION_TOKEN");
  if (!secret || !header?.startsWith("sha256=")) return false;
  const key = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = await crypto.subtle.sign("HMAC", key, encoder.encode(raw));
  const hex = Array.from(new Uint8Array(mac)).map((b) => b.toString(16).padStart(2, "0")).join("");
  return `sha256=${hex}` === header;
}

async function currentStatus(pageId: string): Promise<string | null> {
  const r = await fetch(`https://api.notion.com/v1/pages/${pageId}?filter_properties=${STATUS_PROP_ID}`, {
    headers: { Authorization: `Bearer ${Deno.env.get("NOTION_TOKEN")}`, "Notion-Version": NOTION_VERSION },
  });
  if (!r.ok) return null;
  const page = await r.json();
  return page?.properties?.[STATUS_PROP_NAME]?.select?.name ?? null;
}

async function dispatch(): Promise<number> {
  const r = await fetch(`https://api.github.com/repos/${OWNER_REPO}/actions/workflows/${WORKFLOW}/dispatches`, {
    method: "POST",
    headers: { Authorization: `Bearer ${Deno.env.get("GITHUB_TOKEN")}`, Accept: "application/vnd.github+json",
               "Content-Type": "application/json", "User-Agent": "notion-relay" },
    body: JSON.stringify({ ref: "main" }),
  });
  return r.status;   // 204 = 큐에 들어감. concurrency 그룹이라 연달아 와도 대기 1개만 남는다
}

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return new Response("POST only", { status: 405 });
  const raw = await req.text();
  let body: any;
  try { body = JSON.parse(raw); } catch { return new Response("bad json", { status: 400 }); }

  // 1) 구독 생성 직후 노션이 한 번 보내는 검증 토큰 — 로그에 찍어 두고 노션 화면에 붙여넣는다
  if (body.verification_token) {
    console.log(`[notion-relay] verification_token = ${body.verification_token}`);
    return new Response("ok", { status: 200 });
  }
  // 2) 그 뒤 모든 이벤트는 서명 검증
  if (!(await validSignature(raw, req.headers.get("X-Notion-Signature")))) return new Response("bad signature", { status: 401 });

  // 3) 신청자 리스트의 최종결과 변경만 본다
  const parent = norm(body?.data?.parent?.id ?? body?.data?.parent?.data_source_id ?? "");
  const changed: string[] = body?.data?.updated_properties ?? [];
  const isTarget = body?.type === "page.properties_updated" && APPLICANTS_DB.includes(parent) && changed.includes(STATUS_PROP_ID);
  if (!isTarget) return new Response("ignored", { status: 200 });

  const status = await currentStatus(body.entity.id);
  const triggers = (Deno.env.get("TRIGGER_STATUSES") ?? "HRD등록").split(",").map((s) => s.trim()).filter(Boolean);
  if (!status || !triggers.some((t) => status === t || status.startsWith(t))) {
    console.log(`[notion-relay] 최종결과=${status} → 트리거 아님`);
    return new Response("not a trigger status", { status: 200 });
  }
  const gh = await dispatch();
  console.log(`[notion-relay] 최종결과=${status} → workflow_dispatch ${gh}`);
  return new Response(gh === 204 ? "queued" : `github ${gh}`, { status: 200 });   // 노션엔 항상 200 (재전송 폭주 방지)
});
