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
// 신청자 리스트 원본 — DB ID·데이터 소스 ID 둘 다 (이벤트 부모가 어느 쪽으로 오든). config.NOTION_APPLICANT_SOURCES와 같아야 한다
const SOURCES: Record<string, string[]> = {
  "375d943bcac280e7ba18cd107d5e40d2": ["HnnX"], "375d943bcac2804499a6000bf5cd7ad5": ["HnnX"],   // AI캠퍼스 · 최종결과
  "dfe9c46469bb40e1aee143f91eaf0abd": ["sXGM"], "837e6b2f06db48f3862d8ccd769cd310": ["sXGM"],   // SKN · 최종결과 (2026-09-21)
};
const ALL_STATUS_PROP_IDS = Array.from(new Set(Object.values(SOURCES).flat()));
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

// 페이지의 최종결과만 읽는다. 그 DB에 없는 속성 ID를 섞어 보내면 노션이 400을 돌려주므로(2026-09-21 실측) 부모 DB에 맞는 ID 하나만 쓴다.
// 부모를 모르면 원본별 ID를 차례로 시도한다.
async function currentStatus(pageId: string, propIds: string[]): Promise<string | null> {
  for (const id of propIds) {
    const r = await fetch(`https://api.notion.com/v1/pages/${pageId}?filter_properties=${encodeURIComponent(id)}`, {
      headers: { Authorization: `Bearer ${Deno.env.get("NOTION_TOKEN")}`, "Notion-Version": NOTION_VERSION },
    });
    if (!r.ok) { console.log(`[notion-relay] 페이지 읽기 ${r.status} (prop ${id})`); continue; }
    const page = await r.json();
    const name = page?.properties?.[STATUS_PROP_NAME]?.select?.name;
    if (name !== undefined) return name ?? null;
  }
  return null;
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

  // 3) 페이지 이벤트만. 이벤트 형식은 API 버전마다 조금씩 달라 느슨하게 본다 — 최종 판단은 어차피 아래에서 페이지를 직접 읽어서 한다
  //    (통합이 연결된 페이지의 이벤트만 오고, 최종결과 속성이 없는 페이지는 null → 트리거 아님)
  const type: string = body?.type ?? "";
  const entityId: string | undefined = body?.entity?.id;
  const entityType: string = body?.entity?.type ?? "";
  const parentRaw = body?.data?.parent ?? {};
  const parent = norm(parentRaw?.id ?? parentRaw?.data_source_id ?? parentRaw?.database_id ?? "");
  const changedRaw: any[] = body?.data?.updated_properties ?? body?.data?.updated_property_ids ?? [];
  const changed = changedRaw.map((c) => (typeof c === "string" ? c : c?.id ?? c?.property_id ?? "")).filter(Boolean);
  const changedKnown = changedRaw.length > 0;
  console.log(`[notion-relay] event type=${type} entity=${entityType} parent=${parent || "-"} updated=[${changed.join(",")}] keys=${Object.keys(body?.data ?? {}).join(",")}`);

  const isPage = type.startsWith("page.") || entityType === "page";
  const parentOk = !parent || parent in SOURCES;                        // 부모를 못 읽으면 통과시키고 아래에서 걸러진다
  const wanted = parent && parent in SOURCES ? SOURCES[parent] : ALL_STATUS_PROP_IDS;
  const propOk = !changedKnown || changed.some((c) => wanted.includes(c) || wanted.includes(decodeURIComponent(c)));
  if (!isPage || !entityId || !parentOk || !propOk || type.includes("deleted")) return new Response("ignored", { status: 200 });

  const status = await currentStatus(entityId, wanted);
  const triggers = (Deno.env.get("TRIGGER_STATUSES") ?? "HRD등록").split(",").map((s) => s.trim()).filter(Boolean);
  if (!status || !triggers.some((t) => status === t || status.startsWith(t))) {
    console.log(`[notion-relay] 최종결과=${status} → 트리거 아님`);
    return new Response("not a trigger status", { status: 200 });
  }
  const gh = await dispatch();
  console.log(`[notion-relay] 최종결과=${status} → workflow_dispatch ${gh}`);
  return new Response(gh === 204 ? "queued" : `github ${gh}`, { status: 200 });   // 노션엔 항상 200 (재전송 폭주 방지)
});
