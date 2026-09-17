// 노션 자동화 → 이 함수 → GitHub Actions `kpi_poll.yml` 즉시 실행.
//
// 노션 "웹훅 보내기" 자동화는 본문·헤더를 우리가 정할 수 없어 GitHub API를 직접 못 부른다. 그래서 이 함수가 중계한다.
// 본문(노션이 보내는 페이지 속성)은 읽지도 저장하지도 않는다 — 실명·연락처가 섞여 올 수 있기 때문. 트리거 신호로만 쓴다.
//
// 배포: Supabase 대시보드 › Edge Functions › 새 함수 `notion-relay` › 이 코드 붙여넣기 › "Verify JWT" 끔 (노션은 JWT를 못 보냄)
// 시크릿: GITHUB_TOKEN (fine-grained, 저장소 soulhn/2026data, Actions: Read and write) · RELAY_KEY (아무 긴 문자열)
// 호출: POST https://<project-ref>.functions.supabase.co/notion-relay?key=<RELAY_KEY>
const OWNER_REPO = "soulhn/2026data";
const WORKFLOW = "kpi_poll.yml";

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") return new Response("POST only", { status: 405 });
  const key = new URL(req.url).searchParams.get("key") ?? req.headers.get("x-relay-key");
  if (!key || key !== Deno.env.get("RELAY_KEY")) return new Response("forbidden", { status: 403 });

  const gh = await fetch(`https://api.github.com/repos/${OWNER_REPO}/actions/workflows/${WORKFLOW}/dispatches`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${Deno.env.get("GITHUB_TOKEN")}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "User-Agent": "notion-relay",
    },
    body: JSON.stringify({ ref: "main" }),
  });
  // 204 = 큐에 들어감. 같은 concurrency 그룹이라 연달아 눌러도 대기 1개만 남는다 (자연스러운 디바운스)
  return new Response(gh.status === 204 ? "queued" : `github ${gh.status}`, { status: gh.status === 204 ? 200 : 502 });
});
