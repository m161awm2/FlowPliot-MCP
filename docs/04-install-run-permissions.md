# 04. 설치·실행·권한 설정

## A. 사용자로 쓰기 (클라우드)
1. `https://mcp.m161awm.kr`에서 가입하고 로그인한 뒤 **Get an MCP token**을 누른다. 토큰은 한 번만 보여주고, 7일간
   유효하며, 계획을 승인할 수는 없다.
2. 에이전트에 서버를 등록한다.
   - Claude Code:
     ```
     claude mcp add --transport http pmcp https://mcp.m161awm.kr/mcp --header "Authorization: Bearer <TOKEN>"
     ```
   - Codex CLI: `~/.codex/config.toml`
     ```toml
     [mcp_servers.pmcp]
     url = "https://mcp.m161awm.kr/mcp"
     bearer_token_env_var = "PMCP_TOKEN"
     ```
3. `/connect`에서 계정 ID, 허용 리전, 월 비용 상한을 입력한다. 배포를 허용할지 고른 뒤 **Launch onboarding stack**을
   누른다. 이 버튼은 자기 계정 콘솔에서 CloudFormation 빠른 생성 화면을 연다.
4. 스택이 `CREATE_COMPLETE`가 되면 `/connect`에서 **Verify connection**을 누른다.
5. 에이전트가 계획을 만들면 `/approve/<plan_id>`에서 내용을 검토하고 승인한다. 로그인한 지 10분이 지났으면 다시
   로그인해야 한다.

### 사용자 계정에 생기는 권한 (`infra/tenant-onboarding.yaml`)
| 역할/정책 | 신뢰 대상 | 권한 요약 |
|---|---|---|
| `pmcp-reader` (필수) | 플랫폼 태스크 역할 + ExternalId | 목록·조회와 S3 객체 읽기(프로파일링), `pmcp-*` 작업 그룹에서 Athena 조회와 그 결과 쓰기, 비용·한도·IAM 시뮬레이션 조회, `pmcp/*` 시크릿 읽기 |
| `pmcp-deployer` (선택) | 플랫폼 태스크 역할 + ExternalId | `pmcp-*` 스택 ChangeSet 생성·실행. 단 `pmcp-cfn-exec`을 지정해야만 가능(Deny 조건). `pmcp-*` 리소스에 데이터 단계, artifacts 버킷 |
| `pmcp-cfn-exec` (선택) | cloudformation.amazonaws.com (같은 계정) | `pmcp-*` 이름의 리소스만 생성·변경·삭제. IAM 역할은 `pmcp-boundary`가 붙은 경우에만 생성 |
| `pmcp-boundary` | — | 흐름이 만드는 역할의 상한. 데이터 서비스만 허용, `iam:*`와 `sts:AssumeRole` 거부 |
| `pmcp-<flow>-consumer` | 플랫폼 태스크 역할 + ExternalId | 흐름 출력만 읽을 수 있음(검증용 실제 읽기) |

pmcp는 액세스 키를 받지 않는다. 연결을 끊으려면 onboarding 스택을 지우면 된다.

## B. 로컬 실행 (개발용, stdio)
```
uv sync
uv run pytest                                   # 모의 테스트 68개
uv run pmcp serve --transport stdio --regions ap-southeast-2
claude mcp add pmcp -- uv --directory /path/to/FlowPliot-MCP run pmcp serve --transport stdio
```
- 로컬 모드는 현재 셸의 AWS 자격 증명으로 계정을 `direct` 방식으로 자동 연결한다. **권한 범위가 그 자격 증명
  그대로라서** 개발용으로만 쓴다.
- 승인은 사람이 터미널에서 직접 한다: `uv run pmcp approve <plan_id> --digest <digest>`. 이 명령은 `/dev/tty`에서
  digest 끝 6자리를 입력받는다. TTY가 없는 에이전트 셸에서는 승인할 수 없다.
- 상태를 유지하려면 `PMCP_STATE_TABLE=pmcp-state`를 지정한다. 지정하지 않으면 메모리에만 저장돼 프로세스가 끝나면
  사라진다.

## C. 플랫폼 배포 (운영자, 계정 608420805531)
```
export PMCP_ALERT_EMAIL=<budget-alert-email>
scripts/platform.sh certs        # ACM 인증서 2개 요청, 검증용 CNAME 출력 → 가비아에 등록
scripts/platform.sh wait-certs
scripts/platform.sh image        # ECR 스택 생성, arm64 이미지 빌드·푸시
scripts/platform.sh platform     # pmcp-platform 스택
scripts/platform.sh onboarding   # 온보딩 템플릿을 공개 버킷에 업로드
scripts/platform.sh dns          # mcp / origin-mcp CNAME 출력 → 가비아에 등록
scripts/platform.sh teardown     # 정리 (DynamoDB는 의도적으로 남김)
```

### 플랫폼 태스크 역할 (`pmcp-platform-task`) 권한
- DynamoDB `pmcp-state` 읽기·쓰기
- `pmcp/*` 시크릿 읽기·생성(JWT 서명 키, 오리진 비밀값)
- `arn:aws:iam::*:role/pmcp-*` AssumeRole. 사용자 계정의 신뢰 정책과 ExternalId가 실제 차단 장치다.
- `pmcp-worker` 태스크 정의 RunTask와 PassRole
- `pricing:GetProducts`

### 환경 변수 (`PMCP_*`)
| 변수 | 설명 |
|---|---|
| `MODE` | local / cloud |
| `STATE_TABLE` | 상태 테이블 이름 |
| `PUBLIC_BASE_URL` | 공개 주소 |
| `JWT_SECRET_ID` | JWT 서명 키 시크릿 |
| `ORIGIN_VERIFY_SECRET_ID` | 오리진 비밀값 시크릿 |
| `COGNITO_USER_POOL_ID`, `COGNITO_CLIENT_ID`, `COGNITO_DOMAIN` | Cognito 설정 |
| `RUNNER` | ecs / thread |
| `ECS_CLUSTER`, `WORKER_TASK_DEFINITION`, `WORKER_SUBNETS`, `WORKER_SECURITY_GROUPS` | 워커 실행 설정 |
| `PLATFORM_PRINCIPAL_ARNS` | 사용자 역할이 신뢰하는 플랫폼 역할 |
| `RATE_LIMIT_PER_MINUTE` | 기본 60 |
| `MAX_CONCURRENT_OPERATIONS` | 기본 2 |
| `OPERATION_MAX_SECONDS` | 기본 1800 |

### 운영 명령
- 관리자 토큰 발급: `pmcp token issue --sub <sub> --ttl-days 1` (Secrets Manager 키에 접근할 수 있어야 함)
- 토큰 폐기: `pmcp token revoke --jti <jti> --exp <exp>`
